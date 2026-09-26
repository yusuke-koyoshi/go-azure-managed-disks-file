// Package azurediskfile exposes Azure managed disk snapshots as random-access
// readers backed by HTTP Range requests.
package azurediskfile

import (
	"bytes"
	"container/list"
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/pageblob"
	"golang.org/x/sync/singleflight"
)

const (
	// DefaultBlockSize is the default size of a cached HTTP range.
	DefaultBlockSize       int64 = 1 << 20
	defaultPageRangeWindow       = int64(4 << 30)
	maxRangeGetContentMD5        = int64(4 << 20)
)

// Range describes a byte range on a blob. Start and End are both inclusive.
type Range struct {
	Start int64
	End   int64
}

// Cache stores blocks. Implementations must be safe for concurrent use when a
// reader is shared by multiple goroutines.
type Cache[K comparable, V any] interface {
	Add(key K, value V) bool
	Get(key K) (value V, ok bool)
}

// BlobAPI is the small part of Azure Blob Storage used by Open.
type BlobAPI interface {
	Size(ctx context.Context) (int64, error)
	ReadRange(ctx context.Context, off, count int64) ([]byte, error)
	PageRanges(ctx context.Context) ([]Range, error)
}

type blobIdentifier interface {
	BlobIdentifier() string
}

// OpenOptions controls block caching and optional read-ahead.
type OpenOptions struct {
	// BlockSize is the size of each cached range. A non-positive value uses
	// DefaultBlockSize.
	BlockSize int64
	// PrefetchBlocks asynchronously fetches this many blocks after each read.
	// It is disabled by default. Prefetching only follows reads that are
	// sequential with the previous completed read.
	PrefetchBlocks int
	// MaxConcurrentBlocks limits the number of blocks being fetched at once.
	// A non-positive value leaves block fetching unlimited.
	MaxConcurrentBlocks int
}

// Open opens api as a concurrent-safe *io.SectionReader. The returned reader's
// ReadAt method may be called concurrently. Blocks are fetched in parallel,
// cached by blob identity and block number, and coalesced with singleflight.
// PageRanges is an optional optimization: errors and empty results disable
// sparse-range skipping without preventing the reader from opening. If api
// does not provide BlobIdentifier, the reader gets a unique identity and its
// blocks are not shared with other readers through a shared cache.
func Open(ctx context.Context, api BlobAPI, cache Cache[string, []byte]) (*io.SectionReader, error) {
	return OpenWithOptions(ctx, api, cache, OpenOptions{})
}

// OpenWithOptions is Open with configurable block size and optional read-ahead.
func OpenWithOptions(ctx context.Context, api BlobAPI, cache Cache[string, []byte], options OpenOptions) (*io.SectionReader, error) {
	if api == nil {
		return nil, errors.New("azurediskfile: nil BlobAPI")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	size, err := api.Size(ctx)
	if err != nil {
		return nil, err
	}
	if size < 0 {
		return nil, fmt.Errorf("azurediskfile: invalid blob size %d", size)
	}
	if options.BlockSize <= 0 {
		options.BlockSize = DefaultBlockSize
	}
	if options.PrefetchBlocks < 0 {
		options.PrefetchBlocks = 0
	}
	ranges, rangesErr := api.PageRanges(ctx)
	if rangesErr != nil {
		ranges = nil
	}
	ranges = normalizeRanges(ranges)
	id := fmt.Sprintf("%T:reader-%d", api, nextReaderID.Add(1))
	if identified, ok := api.(blobIdentifier); ok {
		if identifiedID := identified.BlobIdentifier(); identifiedID != "" {
			id = identifiedID
		}
	}
	r := &blobReader{
		ctx:            ctx,
		api:            api,
		cache:          cache,
		id:             id,
		sizeValue:      size,
		blockSize:      options.BlockSize,
		prefetchBlocks: options.PrefetchBlocks,
		maxConcurrent:  options.MaxConcurrentBlocks,
		ranges:         ranges,
		rangesKnown:    len(ranges) > 0,
	}
	if r.cache == nil {
		r.cache = newMemoryCache(256)
	}
	if r.maxConcurrent > 0 {
		r.blockSem = make(chan struct{}, r.maxConcurrent)
	}
	return io.NewSectionReader(r, 0, size), nil
}

var nextReaderID atomic.Uint64

type blobReader struct {
	ctx            context.Context
	api            BlobAPI
	cache          Cache[string, []byte]
	id             string
	sizeValue      int64
	blockSize      int64
	prefetchBlocks int
	maxConcurrent  int
	blockSem       chan struct{}
	ranges         []Range
	rangesKnown    bool
	group          singleflight.Group
	readMu         sync.Mutex
	lastReadEnd    int64
	hasRead        bool
}

func (r *blobReader) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, errors.New("azurediskfile: negative offset")
	}
	if len(p) == 0 {
		return 0, nil
	}
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}

	first := off / r.blockSize
	last := (off + int64(len(p)) - 1) / r.blockSize
	blocks := make([][]byte, last-first+1)
	errs := make([]error, len(blocks))
	var wg sync.WaitGroup
	for i := range blocks {
		blockNumber := first + int64(i)
		wg.Add(1)
		go func(i int, number int64) {
			defer wg.Done()
			blocks[i], errs[i] = r.loadBlock(number)
		}(i, blockNumber)
	}
	wg.Wait()

	n := 0
	var firstErr error
	for i, block := range blocks {
		blockStart := (first + int64(i)) * r.blockSize
		start := maxInt64(off, blockStart)
		end := minInt64(off+int64(len(p)), blockStart+int64(len(block)))
		if errs[i] != nil {
			firstErr = errs[i]
			break
		}
		if end <= start {
			continue
		}
		copied := copy(p[start-off:end-off], block[start-blockStart:end-blockStart])
		n += copied
		if copied != int(end-start) {
			firstErr = io.ErrUnexpectedEOF
			break
		}
	}
	if firstErr != nil {
		return n, firstErr
	}
	if n != len(p) {
		return n, io.EOF
	}

	r.readMu.Lock()
	sequential := !r.hasRead || off == r.lastReadEnd
	r.lastReadEnd = off + int64(len(p))
	r.hasRead = true
	r.readMu.Unlock()
	if sequential && r.prefetchBlocks > 0 {
		next := last + 1
		for i := 0; i < r.prefetchBlocks; i++ {
			number := next + int64(i)
			if number*r.blockSize >= r.sizeValue {
				break
			}
			r.prefetch(number)
		}
	}
	return n, nil
}

func (r *blobReader) loadBlock(number int64) ([]byte, error) {
	ctx := r.ctx
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	key := fmt.Sprintf("%s:%d:%d", r.id, r.blockSize, number)
	if r.cache != nil {
		if value, ok := r.cache.Get(key); ok {
			return value, nil
		}
	}
	value, err, _ := r.group.Do(key, func() (any, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if r.cache != nil {
			if value, ok := r.cache.Get(key); ok {
				return value, nil
			}
		}
		if err := r.acquireBlock(ctx); err != nil {
			return nil, err
		}
		defer r.releaseBlock()
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		start := number * r.blockSize
		if r.isUnallocated(start, start+r.blockSize-1) {
			value := make([]byte, r.blockSize)
			if r.cache != nil {
				r.cache.Add(key, value)
			}
			return value, nil
		}
		count := r.blockSize
		if end := start + count; end > r.size() {
			count = r.size() - start
		}
		if count <= 0 {
			return []byte{}, nil
		}
		value, err := r.api.ReadRange(ctx, start, count)
		if err != nil {
			return nil, fmt.Errorf("azurediskfile: range %d-%d: %w", start, start+count-1, err)
		}
		if int64(len(value)) != count {
			return nil, fmt.Errorf("azurediskfile: range %d-%d returned %d bytes, want %d", start, start+count-1, len(value), count)
		}
		if r.cache != nil {
			r.cache.Add(key, value)
		}
		return value, nil
	})
	if err != nil {
		return nil, err
	}
	return value.([]byte), nil
}

func (r *blobReader) acquireBlock(ctx context.Context) error {
	if r.blockSem == nil {
		return nil
	}
	select {
	case r.blockSem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *blobReader) releaseBlock() {
	if r.blockSem != nil {
		<-r.blockSem
	}
}

func (r *blobReader) prefetch(number int64) {
	go func() {
		_, _ = r.loadBlock(number)
	}()
}

func (r *blobReader) size() int64 { return r.sizeValue }

func (r *blobReader) isUnallocated(start, end int64) bool {
	if !r.rangesKnown {
		return false
	}
	for _, allocated := range r.ranges {
		if allocated.End >= start && allocated.Start <= end {
			return false
		}
		if allocated.Start > end {
			break
		}
	}
	return true
}

func normalizeRanges(ranges []Range) []Range {
	if ranges == nil {
		return nil
	}
	result := make([]Range, 0, len(ranges))
	for _, current := range ranges {
		if current.Start < 0 || current.End < current.Start {
			continue
		}
		result = append(result, current)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Start < result[j].Start })
	merged := result[:0]
	for _, current := range result {
		if len(merged) == 0 || current.Start > merged[len(merged)-1].End+1 {
			merged = append(merged, current)
			continue
		}
		if current.End > merged[len(merged)-1].End {
			merged[len(merged)-1].End = current.End
		}
	}
	return merged
}

type memoryCache struct {
	mu         sync.Mutex
	values     map[string][]byte
	maxEntries int
	order      *list.List
	positions  map[string]*list.Element
}

type memoryCacheEntry struct {
	key string
}

func newMemoryCache(maxEntries int) *memoryCache {
	return &memoryCache{
		values:     make(map[string][]byte),
		maxEntries: maxEntries,
		order:      list.New(),
		positions:  make(map[string]*list.Element),
	}
}

func (c *memoryCache) Add(key string, value []byte) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.values[key]; exists {
		c.order.MoveToFront(c.positions[key])
		return false
	}
	if c.maxEntries > 0 && len(c.values) >= c.maxEntries {
		oldest := c.order.Back()
		if oldest != nil {
			entry := oldest.Value.(memoryCacheEntry)
			delete(c.values, entry.key)
			delete(c.positions, entry.key)
			c.order.Remove(oldest)
		}
	}
	c.values[key] = value
	c.positions[key] = c.order.PushFront(memoryCacheEntry{key: key})
	return true
}

func (c *memoryCache) Get(key string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	value, ok := c.values[key]
	if ok {
		c.order.MoveToFront(c.positions[key])
	}
	return value, ok
}

type sasBlobAPI struct {
	blob              *blob.Client
	pageBlob          *pageblob.Client
	identifier        string
	initErr           error
	windowSize        int64
	skipRangeChecksum bool
	sizeMu            sync.Mutex
	sizeValue         int64
	sizeReady         bool
}

// SASOption configures the Azure SDK client used by NewSASBlobAPI.
type SASOption func(*sasBlobOptions)

type sasBlobOptions struct {
	retry             policy.RetryOptions
	windowSize        int64
	skipRangeChecksum bool
}

// WithRetryOptions replaces the Azure SDK retry policy. Azure's default policy
// retries 408, 429, 500, 502, 503 and 504 and honors Retry-After.
func WithRetryOptions(options policy.RetryOptions) SASOption {
	return func(config *sasBlobOptions) { config.retry = options }
}

// WithPageRangeWindow sets the maximum range queried by each Get Page Ranges
// request. The default is 4 GiB.
func WithPageRangeWindow(size int64) SASOption {
	return func(config *sasBlobOptions) {
		if size > 0 {
			config.windowSize = size
		}
	}
}

// WithoutRangeChecksum disables per-range MD5 validation. Validation is on by
// default and fails closed, so a range response without Content-MD5 is an
// error. Use this only when the endpoint is known not to support
// x-ms-range-get-content-md5, because it removes detection of silently
// corrupted range responses.
//
// Validation only covers ranges of 4 MiB or less, the limit Azure accepts for
// x-ms-range-get-content-md5. An OpenOptions.BlockSize above that reads
// without validation even when this option is not used.
func WithoutRangeChecksum() SASOption {
	return func(config *sasBlobOptions) { config.skipRangeChecksum = true }
}

// NewSASBlobAPI creates a BlobAPI backed by an export SAS URL.
func NewSASBlobAPI(sasURL string, options ...SASOption) BlobAPI {
	config := sasBlobOptions{windowSize: defaultPageRangeWindow}
	for _, option := range options {
		if option != nil {
			option(&config)
		}
	}
	parsed, err := url.Parse(sasURL)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		// url.Parse errors quote the input, which would expose the signature.
		return &sasBlobAPI{
			initErr:           errors.New("azurediskfile: invalid SAS URL"),
			identifier:        "invalid-sas-url",
			windowSize:        config.windowSize,
			skipRangeChecksum: config.skipRangeChecksum,
		}
	}
	blobOptions := &blob.ClientOptions{}
	blobOptions.Retry = config.retry
	pageOptions := &pageblob.ClientOptions{}
	pageOptions.Retry = config.retry
	blobClient, blobErr := blob.NewClientWithNoCredential(sasURL, blobOptions)
	pageClient, pageErr := pageblob.NewClientWithNoCredential(sasURL, pageOptions)
	if blobErr != nil {
		err = blobErr
	} else if pageErr != nil {
		err = pageErr
	}
	return &sasBlobAPI{
		blob:              blobClient,
		pageBlob:          pageClient,
		identifier:        parsed.Scheme + "://" + parsed.Host + parsed.Path,
		initErr:           err,
		windowSize:        config.windowSize,
		skipRangeChecksum: config.skipRangeChecksum,
	}
}

func (a *sasBlobAPI) BlobIdentifier() string { return a.identifier }

func (a *sasBlobAPI) Size(ctx context.Context) (int64, error) {
	if a.initErr != nil {
		return 0, a.initErr
	}
	a.sizeMu.Lock()
	defer a.sizeMu.Unlock()
	if a.sizeReady {
		return a.sizeValue, nil
	}
	response, err := a.blob.GetProperties(ctx, nil)
	if err != nil {
		return 0, err
	}
	if response.ContentLength == nil {
		return 0, errors.New("azurediskfile: Azure response did not include Content-Length")
	}
	a.sizeValue = *response.ContentLength
	a.sizeReady = true
	return a.sizeValue, nil
}

func (a *sasBlobAPI) ReadRange(ctx context.Context, off, count int64) ([]byte, error) {
	if a.initErr != nil {
		return nil, a.initErr
	}
	var rangeGetContentMD5 *bool
	if !a.skipRangeChecksum {
		rangeGetContentMD5 = rangeGetContentMD5Option(count)
	}
	response, err := a.blob.DownloadStream(ctx, &blob.DownloadStreamOptions{
		Range:              azblob.HTTPRange{Offset: off, Count: count},
		RangeGetContentMD5: rangeGetContentMD5,
	})
	if err != nil {
		return nil, err
	}
	reader := response.NewRetryReader(ctx, nil)
	defer reader.Close()
	value, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	if rangeGetContentMD5 != nil {
		if err := validateRangeContentMD5(value, response.ContentMD5); err != nil {
			return nil, err
		}
	}
	return value, nil
}

func rangeGetContentMD5Option(count int64) *bool {
	if count <= 0 || count > maxRangeGetContentMD5 {
		return nil
	}
	value := true
	return &value
}

func validateRangeContentMD5(value, expected []byte) error {
	if len(expected) == 0 {
		return errors.New("azurediskfile: range response did not include Content-MD5")
	}
	actual := md5.Sum(value)
	if !bytes.Equal(actual[:], expected) {
		return errors.New("azurediskfile: range Content-MD5 mismatch")
	}
	return nil
}

func (a *sasBlobAPI) PageRanges(ctx context.Context) ([]Range, error) {
	if a.initErr != nil {
		return nil, a.initErr
	}
	size, err := a.Size(ctx)
	if err != nil {
		return nil, err
	}
	var ranges []Range
	for start := int64(0); start < size; start += a.windowSize {
		count := minInt64(a.windowSize, size-start)
		pager := a.pageBlob.NewGetPageRangesPager(&pageblob.GetPageRangesOptions{
			Range: azblob.HTTPRange{Offset: start, Count: count},
		})
		for pager.More() {
			response, err := pager.NextPage(ctx)
			if err != nil {
				return nil, err
			}
			for _, pageRange := range response.PageRange {
				if pageRange.Start != nil && pageRange.End != nil {
					ranges = append(ranges, Range{Start: *pageRange.Start, End: *pageRange.End})
				}
			}
		}
	}
	if ranges == nil {
		ranges = []Range{}
	}
	return normalizeRanges(ranges), nil
}

type mockBlobAPI struct {
	file       *os.File
	size       int64
	identifier string
}

// NewMockBlobAPI opens a local file as a BlobAPI for tests and offline tools.
func NewMockBlobAPI(filePath string) BlobAPI {
	file, err := os.Open(filePath)
	if err != nil {
		return &mockBlobAPI{identifier: filepath.Clean(filePath), size: -1}
	}
	info, statErr := file.Stat()
	if statErr != nil {
		_ = file.Close()
		return &mockBlobAPI{identifier: filepath.Clean(filePath), size: -1}
	}
	return &mockBlobAPI{file: file, size: info.Size(), identifier: filepath.Clean(filePath)}
}

func (m *mockBlobAPI) BlobIdentifier() string { return m.identifier }

func (m *mockBlobAPI) Size(context.Context) (int64, error) {
	if m.file == nil {
		return 0, os.ErrNotExist
	}
	return m.size, nil
}

func (m *mockBlobAPI) ReadRange(ctx context.Context, off, count int64) ([]byte, error) {
	if m.file == nil {
		return nil, os.ErrNotExist
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if off < 0 || count < 0 {
		return nil, errors.New("azurediskfile: invalid range")
	}
	buf := make([]byte, count)
	n, err := m.file.ReadAt(buf, off)
	if err != nil && !(errors.Is(err, io.EOF) && int64(n) == count) {
		return nil, err
	}
	return buf[:n], nil
}

func (m *mockBlobAPI) PageRanges(context.Context) ([]Range, error) { return nil, nil }

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

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

// Cache stores blocks. Implementations must be safe for concurrent use even if
// the reader is not shared, because a read of several blocks and read-ahead
// call it from goroutines started by the reader.
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
	// PrefetchBlocks fetches this many blocks ahead in the background while
	// each read starts where the previous successful read ended; the first
	// read counts. It is disabled by default. Keep it small relative to the
	// cache: a prefetched block holds a goroutine until it is fetched and may
	// be evicted before it is read, and a run that stops early wastes up to
	// this many requests.
	PrefetchBlocks int
	// MaxConcurrentBlocks limits the number of blocks being fetched at once,
	// prefetches included. Each fetched block is copied into the caller's
	// buffer as soon as it arrives, so this also roughly limits the block
	// memory a read uses outside the cache. A non-positive value leaves block
	// fetching unlimited.
	MaxConcurrentBlocks int
}

// Open opens api as a concurrent-safe *io.SectionReader. The returned reader's
// ReadAt method may be called concurrently. Blocks are fetched in parallel,
// cached by blob identity and block number, and coalesced with singleflight.
// PageRanges is an optional optimization: errors and empty results disable
// sparse-range skipping without preventing the reader from opening. If api
// does not provide BlobIdentifier, the reader gets a unique identity and its
// blocks are not shared with other readers through a shared cache.
//
// A panic in api or cache while a block is fetched ends the process when the
// fetch runs in a goroutine started by the reader: in a read that fetches
// several blocks, and in read-ahead. A read that fetches one block raises it
// in the caller's goroutine.
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
		prefetchedUpTo: -1,
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
	prefetchedUpTo int64
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

	if off >= r.sizeValue {
		return 0, io.EOF
	}
	end := min(off+int64(len(p)), r.sizeValue)
	first := off / r.blockSize
	last := (end - 1) / r.blockSize
	errs := r.readBlocks(p[:end-off], off, first, last)
	for i, err := range errs {
		if err != nil {
			// p is scratch space on error, so later blocks may already be
			// copied past n.
			n := 0
			if blockStart := (first + int64(i)) * r.blockSize; blockStart > off {
				n = int(blockStart - off)
			}
			return n, err
		}
	}
	n := int(end - off)
	if n != len(p) {
		return n, io.EOF
	}

	if r.prefetchBlocks > 0 {
		r.prefetchAfter(off, end, last)
	}
	return n, nil
}

func (r *blobReader) prefetchAfter(off, end, last int64) {
	from := last + 1
	// Clamp before adding so that a huge PrefetchBlocks does not overflow.
	limit := (r.sizeValue - 1) / r.blockSize
	if n := int64(r.prefetchBlocks); n < limit-last {
		limit = last + n
	}

	r.readMu.Lock()
	sequential := !r.hasRead || off == r.lastReadEnd
	r.lastReadEnd = end
	r.hasRead = true
	// Small reads within a block would otherwise prefetch the same blocks
	// again. A read that does not follow resets the mark, so a later run
	// earlier in the blob still prefetches.
	if !sequential {
		r.prefetchedUpTo = -1
	} else if r.prefetchedUpTo >= from {
		from = r.prefetchedUpTo + 1
	}
	if sequential && from <= limit {
		r.prefetchedUpTo = limit
	}
	r.readMu.Unlock()

	if !sequential {
		return
	}
	for number := from; number <= limit; number++ {
		r.prefetch(number)
	}
}

func (r *blobReader) readBlocks(p []byte, off, first, last int64) []error {
	type block struct {
		number int64
		key    string
	}
	errs := make([]error, last-first+1)
	var missing []block
	for i := range errs {
		number := first + int64(i)
		if r.blockUnallocated(number) {
			// Zero-fill instead of caching zeros, which would evict data.
			dst, _ := r.blockSpan(p, off, number)
			clear(dst)
			continue
		}
		key := r.blockKey(number)
		if value, ok := r.cache.Get(key); ok {
			errs[i] = r.copyBlock(p, off, number, value)
			continue
		}
		missing = append(missing, block{number, key})
	}
	if len(missing) == 1 {
		// Most reads fall within one block; skip the goroutine for them.
		b := missing[0]
		errs[b.number-first] = r.fetchInto(p, off, b.number, b.key)
		return errs
	}
	var wg sync.WaitGroup
	for _, b := range missing {
		wg.Go(func() { errs[b.number-first] = r.fetchInto(p, off, b.number, b.key) })
	}
	wg.Wait()
	return errs
}

func (r *blobReader) fetchInto(p []byte, off, number int64, key string) error {
	value, err := r.fetchBlock(number, key)
	if err != nil {
		return err
	}
	return r.copyBlock(p, off, number, value)
}

func (r *blobReader) copyBlock(p []byte, off, number int64, block []byte) error {
	dst, from := r.blockSpan(p, off, number)
	if int64(len(block)) < from+int64(len(dst)) {
		return io.ErrUnexpectedEOF
	}
	copy(dst, block[from:])
	return nil
}

// The second result is where the span starts within the block.
func (r *blobReader) blockSpan(p []byte, off, number int64) ([]byte, int64) {
	blockStart := number * r.blockSize
	start := max(off, blockStart)
	end := min(off+int64(len(p)), blockStart+r.blockSize)
	return p[start-off : end-off], start - blockStart
}

func (r *blobReader) blockUnallocated(number int64) bool {
	start := number * r.blockSize
	return r.isUnallocated(start, start+r.blockSize-1)
}

func (r *blobReader) blockKey(number int64) string {
	return fmt.Sprintf("%s:%d:%d", r.id, r.blockSize, number)
}

func (r *blobReader) fetchBlock(number int64, key string) ([]byte, error) {
	ctx := r.ctx
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	value, err, _ := r.group.Do(key, func() (any, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if value, ok := r.cache.Get(key); ok {
			return value, nil
		}
		if err := r.acquireBlock(ctx); err != nil {
			return nil, err
		}
		defer r.releaseBlock()
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		start := number * r.blockSize
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
		r.cache.Add(key, value)
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
	if r.blockUnallocated(number) {
		return
	}
	go func() {
		key := r.blockKey(number)
		if _, ok := r.cache.Get(key); ok {
			return
		}
		_, _ = r.fetchBlock(number, key)
	}()
}

func (r *blobReader) size() int64 { return r.sizeValue }

func (r *blobReader) isUnallocated(start, end int64) bool {
	if !r.rangesKnown {
		return false
	}
	// ranges is sorted and merged, so the first range ending at or after start
	// is the only one that can overlap.
	i := sort.Search(len(r.ranges), func(i int) bool { return r.ranges[i].End >= start })
	return i == len(r.ranges) || r.ranges[i].Start > end
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
	perRetry          []policy.Policy
	windowSize        int64
	skipRangeChecksum bool
}

// WithRetryOptions replaces the Azure SDK retry policy. Azure's default policy
// retries 408, 429, 500, 502, 503 and 504 and honors Retry-After.
func WithRetryOptions(options policy.RetryOptions) SASOption {
	return func(config *sasBlobOptions) { config.retry = options }
}

// WithPerRetryPolicies adds policies that run on every attempt of the blob
// requests, retries included, for example to count throttled responses that
// the SDK retries and the caller never sees. Each use adds to the policies of
// earlier uses. The policies serve concurrent requests, so they must be safe
// for concurrent use.
func WithPerRetryPolicies(policies ...policy.Policy) SASOption {
	return func(config *sasBlobOptions) { config.perRetry = append(config.perRetry, policies...) }
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
	blobOptions.PerRetryPolicies = config.perRetry
	pageOptions := &pageblob.ClientOptions{}
	pageOptions.Retry = config.retry
	pageOptions.PerRetryPolicies = config.perRetry
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
		count := min(a.windowSize, size-start)
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

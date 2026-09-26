package azurediskfile

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
)

type testBlob struct {
	id        string
	data      []byte
	ranges    []Range
	delay     chan struct{}
	started   chan struct{}
	requests  atomic.Int64
	active    atomic.Int64
	maxActive atomic.Int64
	rangesErr error
}

func (b *testBlob) BlobIdentifier() string { return b.id }
func (b *testBlob) Size(context.Context) (int64, error) {
	return int64(len(b.data)), nil
}
func (b *testBlob) PageRanges(context.Context) ([]Range, error) { return b.ranges, b.rangesErr }
func (b *testBlob) ReadRange(ctx context.Context, off, count int64) ([]byte, error) {
	b.requests.Add(1)
	active := b.active.Add(1)
	defer b.active.Add(-1)
	for {
		maxActive := b.maxActive.Load()
		if active <= maxActive || b.maxActive.CompareAndSwap(maxActive, active) {
			break
		}
	}
	if b.started != nil {
		select {
		case b.started <- struct{}{}:
		default:
		}
	}
	if b.delay != nil {
		select {
		case <-b.delay:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if off < 0 || off+count > int64(len(b.data)) {
		return nil, io.ErrUnexpectedEOF
	}
	return append([]byte(nil), b.data[off:off+count]...), nil
}

type noIdentityBlob struct {
	blob *testBlob
}

func (b *noIdentityBlob) Size(ctx context.Context) (int64, error) {
	return b.blob.Size(ctx)
}
func (b *noIdentityBlob) PageRanges(ctx context.Context) ([]Range, error) {
	return b.blob.PageRanges(ctx)
}
func (b *noIdentityBlob) ReadRange(ctx context.Context, off, count int64) ([]byte, error) {
	return b.blob.ReadRange(ctx, off, count)
}

type cancelAfterFirstRangeBlob struct {
	cancel context.CancelFunc
	data   []byte
	once   sync.Once
}

func (b *cancelAfterFirstRangeBlob) BlobIdentifier() string { return "partial-cancel" }
func (b *cancelAfterFirstRangeBlob) Size(context.Context) (int64, error) {
	return int64(len(b.data)), nil
}
func (b *cancelAfterFirstRangeBlob) PageRanges(context.Context) ([]Range, error) {
	return nil, nil
}
func (b *cancelAfterFirstRangeBlob) ReadRange(ctx context.Context, off, count int64) ([]byte, error) {
	if off == 0 {
		b.once.Do(b.cancel)
		return append([]byte(nil), b.data[off:off+count]...), nil
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

type testCache struct {
	mu     sync.Mutex
	values map[string][]byte
}

func (c *testCache) Add(key string, value []byte) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.values[key]; exists {
		return false
	}
	c.values[key] = value
	return true
}
func (c *testCache) Get(key string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	value, ok := c.values[key]
	return value, ok
}

func TestOpenReadsAcrossBlockBoundaryAndFinalBlock(t *testing.T) {
	api := &testBlob{id: "boundary", data: []byte("0123456789")}
	reader, err := OpenWithOptions(context.Background(), api, nil, OpenOptions{BlockSize: 4})
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 6)
	n, err := reader.ReadAt(buf, 3)
	if err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	if n != len(buf) || string(buf) != "345678" {
		t.Fatalf("got n=%d data=%q", n, buf)
	}
	buf = make([]byte, 2)
	if _, err := reader.ReadAt(buf, 8); err != nil {
		t.Fatalf("final block ReadAt: %v", err)
	}
	if string(buf) != "89" {
		t.Fatalf("final block = %q", buf)
	}
}

func TestNewMockBlobAPI(t *testing.T) {
	path := "testdata/mock.raw"
	reader, err := OpenWithOptions(context.Background(), NewMockBlobAPI(path), nil, OpenOptions{BlockSize: 4})
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 4)
	if _, err := reader.ReadAt(buf, 4); err != nil {
		t.Fatal(err)
	}
	if string(buf) != "data" {
		t.Fatalf("got %q", buf)
	}
}

func TestOpenConcurrentReads(t *testing.T) {
	data := bytes.Repeat([]byte("abcdefgh"), 128)
	api := &testBlob{id: "concurrent", data: data}
	reader, err := OpenWithOptions(context.Background(), api, nil, OpenOptions{BlockSize: 16})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := range 64 {
		wg.Go(func() {
			off := int64((i * 7) % (len(data) - 24))
			buf := make([]byte, 24)
			n, err := reader.ReadAt(buf, off)
			if err != nil || n != len(buf) || !bytes.Equal(buf, data[off:off+int64(len(buf))]) {
				t.Errorf("ReadAt(%d): n=%d err=%v", off, n, err)
			}
		})
	}
	wg.Wait()
}

func TestOpenSingleflightForSameBlock(t *testing.T) {
	api := &testBlob{
		id:      "singleflight",
		data:    []byte("0123456789abcdef"),
		delay:   make(chan struct{}),
		started: make(chan struct{}, 1),
	}
	reader, err := OpenWithOptions(context.Background(), api, nil, OpenOptions{BlockSize: 16})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 32)
	for range 32 {
		wg.Go(func() {
			buf := make([]byte, 4)
			_, err := reader.ReadAt(buf, 2)
			errs <- err
		})
	}
	select {
	case <-api.started:
	case <-time.After(time.Second):
		t.Fatal("ReadRange was not called")
	}
	close(api.delay)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if got := api.requests.Load(); got != 1 {
		t.Fatalf("ReadRange calls = %d, want 1", got)
	}
}

func TestOpenSharedCacheIncludesBlobIdentity(t *testing.T) {
	cache := &testCache{values: make(map[string][]byte)}
	first := &testBlob{id: "first", data: []byte("AAAA")}
	second := &testBlob{id: "second", data: []byte("BBBB")}
	reader1, err := OpenWithOptions(context.Background(), first, cache, OpenOptions{BlockSize: 4})
	if err != nil {
		t.Fatal(err)
	}
	reader2, err := OpenWithOptions(context.Background(), second, cache, OpenOptions{BlockSize: 4})
	if err != nil {
		t.Fatal(err)
	}
	got1 := make([]byte, 4)
	got2 := make([]byte, 4)
	if _, err := reader1.ReadAt(got1, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := reader2.ReadAt(got2, 0); err != nil {
		t.Fatal(err)
	}
	if string(got1) != "AAAA" || string(got2) != "BBBB" {
		t.Fatalf("got %q and %q", got1, got2)
	}
	if first.requests.Load() != 1 || second.requests.Load() != 1 {
		t.Fatalf("requests = %d, %d", first.requests.Load(), second.requests.Load())
	}
}

func TestOpenSkipsUnallocatedBlocks(t *testing.T) {
	api := &testBlob{id: "sparse", data: []byte("abcdWXYZ"), ranges: []Range{{Start: 4, End: 7}}}
	reader, err := OpenWithOptions(context.Background(), api, nil, OpenOptions{BlockSize: 4})
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 4)
	if _, err := reader.ReadAt(buf, 0); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf, make([]byte, 4)) {
		t.Fatalf("unallocated block = %q", buf)
	}
	if api.requests.Load() != 0 {
		t.Fatalf("unallocated block made %d requests", api.requests.Load())
	}
	if _, err := reader.ReadAt(buf, 4); err != nil {
		t.Fatal(err)
	}
	if string(buf) != "WXYZ" {
		t.Fatalf("allocated block = %q", buf)
	}
}

func TestOpenEmptyPageRangesDoesNotZeroReads(t *testing.T) {
	api := &testBlob{id: "empty-ranges", data: []byte("data"), ranges: []Range{}}
	reader, err := OpenWithOptions(context.Background(), api, nil, OpenOptions{BlockSize: 4})
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 4)
	if _, err := reader.ReadAt(buf, 0); err != nil {
		t.Fatal(err)
	}
	if string(buf) != "data" {
		t.Fatalf("got %q, want data", buf)
	}
	if api.requests.Load() != 1 {
		t.Fatalf("requests = %d, want 1", api.requests.Load())
	}
}

func TestOpenContinuesWhenPageRangesFails(t *testing.T) {
	api := &testBlob{
		id:        "ranges-error",
		data:      []byte("data"),
		rangesErr: errors.New("page ranges unavailable"),
	}
	reader, err := OpenWithOptions(context.Background(), api, nil, OpenOptions{BlockSize: 4})
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 4)
	if _, err := reader.ReadAt(buf, 0); err != nil {
		t.Fatal(err)
	}
	if string(buf) != "data" {
		t.Fatalf("got %q, want data", buf)
	}
}

func TestOpenFallbackBlobIdentityIsUniquePerReader(t *testing.T) {
	cache := &testCache{values: make(map[string][]byte)}
	underlying := &testBlob{data: []byte("data")}
	api := &noIdentityBlob{blob: underlying}
	reader1, err := OpenWithOptions(context.Background(), api, cache, OpenOptions{BlockSize: 4})
	if err != nil {
		t.Fatal(err)
	}
	reader2, err := OpenWithOptions(context.Background(), api, cache, OpenOptions{BlockSize: 4})
	if err != nil {
		t.Fatal(err)
	}
	for _, reader := range []*io.SectionReader{reader1, reader2} {
		buf := make([]byte, 4)
		if _, err := reader.ReadAt(buf, 0); err != nil {
			t.Fatal(err)
		}
		if string(buf) != "data" {
			t.Fatalf("got %q", buf)
		}
	}
	if got := underlying.requests.Load(); got != 2 {
		t.Fatalf("ReadRange calls = %d, want 2", got)
	}
}

func TestOpenReadHonorsCancellationAfterOpen(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	api := &testBlob{id: "cancelled", data: []byte("data")}
	reader, err := OpenWithOptions(ctx, api, nil, OpenOptions{BlockSize: 4})
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	buf := make([]byte, 4)
	if _, err := reader.ReadAt(buf, 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("ReadAt error = %v, want context.Canceled", err)
	}
	if api.requests.Load() != 0 {
		t.Fatalf("ReadRange calls = %d, want 0", api.requests.Load())
	}
}

func TestReadAtReturnsPartialBytesOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	api := &cancelAfterFirstRangeBlob{
		cancel: cancel,
		data:   []byte("abcdefgh"),
	}
	reader, err := OpenWithOptions(ctx, api, nil, OpenOptions{BlockSize: 4})
	if err != nil {
		t.Fatal(err)
	}
	n, err := reader.ReadAt(make([]byte, 8), 0)
	if n != 4 {
		t.Fatalf("ReadAt n = %d, want 4", n)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("ReadAt error = %v, want context.Canceled", err)
	}
}

func TestOpenMaxConcurrentBlocks(t *testing.T) {
	api := &testBlob{
		id:      "bounded",
		data:    bytes.Repeat([]byte("x"), 32),
		delay:   make(chan struct{}),
		started: make(chan struct{}, 8),
	}
	reader, err := OpenWithOptions(context.Background(), api, nil, OpenOptions{
		BlockSize:           4,
		MaxConcurrentBlocks: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		_, _ = reader.ReadAt(make([]byte, len(api.data)), 0)
		close(done)
	}()
	for range 2 {
		select {
		case <-api.started:
		case <-time.After(time.Second):
			t.Fatal("did not start initial block requests")
		}
	}
	select {
	case <-api.started:
		t.Fatal("started more than MaxConcurrentBlocks requests")
	case <-time.After(50 * time.Millisecond):
	}
	close(api.delay)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("bounded read did not finish")
	}
	if got := api.maxActive.Load(); got > 2 {
		t.Fatalf("max active requests = %d, want at most 2", got)
	}
}

func TestPrefetchDoesNotFollowRandomAccess(t *testing.T) {
	const blockSize = 4
	data := bytes.Repeat([]byte("x"), 64)
	api := &testBlob{id: "random", data: data}
	reader, err := OpenWithOptions(context.Background(), api, nil, OpenOptions{
		BlockSize:      blockSize,
		PrefetchBlocks: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	// The first read is always treated as sequential, so start on the final
	// block: read-ahead stops at the end of the blob and spawns no request.
	// Every later offset is non-adjacent, so the request count is exactly the
	// number of distinct blocks touched.
	offsets := []int64{60, 0, 32, 8, 48, 16}
	buf := make([]byte, blockSize)
	for _, off := range offsets {
		if _, err := reader.ReadAt(buf, off); err != nil {
			t.Fatalf("ReadAt(%d): %v", off, err)
		}
	}
	if got := api.requests.Load(); got != int64(len(offsets)) {
		t.Fatalf("ReadRange calls = %d, want %d", got, len(offsets))
	}
}

func BenchmarkReadAtPrefetch(b *testing.B) {
	const blockSize = 1 << 10
	data := bytes.Repeat([]byte("x"), 256*blockSize)
	blocks := int64(len(data) / blockSize)
	for _, bench := range []struct {
		name   string
		offset func(i int64) int64
	}{
		{name: "sequential", offset: func(i int64) int64 { return (i % blocks) * blockSize }},
		// A stride coprime with the block count visits every block without
		// ever landing next to the previous read.
		{name: "random", offset: func(i int64) int64 { return ((i * 97) % blocks) * blockSize }},
	} {
		b.Run(bench.name, func(b *testing.B) {
			api := &testBlob{id: bench.name, data: data}
			reader, err := OpenWithOptions(context.Background(), api, newMemoryCache(16), OpenOptions{
				BlockSize:      blockSize,
				PrefetchBlocks: 4,
			})
			if err != nil {
				b.Fatal(err)
			}
			buf := make([]byte, blockSize)
			for i := 0; b.Loop(); i++ {
				if _, err := reader.ReadAt(buf, bench.offset(int64(i))); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(api.requests.Load())/float64(b.N), "requests/op")
		})
	}
}

func TestMemoryCacheEvictsLeastRecentlyUsed(t *testing.T) {
	cache := newMemoryCache(2)
	if !cache.Add("a", []byte("a")) || !cache.Add("b", []byte("b")) {
		t.Fatal("initial cache adds failed")
	}
	if _, ok := cache.Get("a"); !ok {
		t.Fatal("cache did not return a")
	}
	if !cache.Add("c", []byte("c")) {
		t.Fatal("cache add c failed")
	}
	if _, ok := cache.Get("b"); ok {
		t.Fatal("least recently used entry b was not evicted")
	}
	if _, ok := cache.Get("a"); !ok {
		t.Fatal("recently used entry a was evicted")
	}
}

func TestMemoryCacheDuplicateAddRefreshesLRU(t *testing.T) {
	cache := newMemoryCache(2)
	cache.Add("a", []byte("a"))
	cache.Add("b", []byte("b"))
	if cache.Add("a", []byte("new a")) {
		t.Fatal("duplicate add reported an insertion")
	}
	cache.Add("c", []byte("c"))
	if _, ok := cache.Get("b"); ok {
		t.Fatal("duplicate add did not refresh a")
	}
	if value, ok := cache.Get("a"); !ok || string(value) != "a" {
		t.Fatalf("a = %q, %v", value, ok)
	}
}

func TestRangeGetContentMD5OptionBoundary(t *testing.T) {
	// The Azure SDK owns the generated HTTP transport and response type, so
	// the boundary is tested through the request-option helper instead of a
	// live Azure response.
	for _, test := range []struct {
		count int64
		want  bool
	}{
		{count: 0, want: false},
		{count: maxRangeGetContentMD5 - 1, want: true},
		{count: maxRangeGetContentMD5, want: true},
		{count: maxRangeGetContentMD5 + 1, want: false},
	} {
		option := rangeGetContentMD5Option(test.count)
		if (option != nil) != test.want {
			t.Fatalf("count %d: option present = %v, want %v", test.count, option != nil, test.want)
		}
		if option != nil && !*option {
			t.Fatalf("count %d: option = false, want true", test.count)
		}
	}
}

// rangeRequestRecorder captures the request headers the Azure SDK sends, so
// the assertions do not race with the still-running handler.
type rangeRequestRecorder struct {
	mu          sync.Mutex
	rangeHeader string
	md5Header   string
}

func (r *rangeRequestRecorder) record(request *http.Request) {
	r.mu.Lock()
	defer r.mu.Unlock()
	// The generated Azure SDK sends the range as x-ms-range.
	r.rangeHeader = request.Header.Get("x-ms-range")
	if r.rangeHeader == "" {
		r.rangeHeader = request.Header.Get("Range")
	}
	r.md5Header = request.Header.Get("x-ms-range-get-content-md5")
}

func (r *rangeRequestRecorder) headers() (rangeHeader, md5Header string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.rangeHeader, r.md5Header
}

func TestSASBlobAPIReadRangeContentMD5(t *testing.T) {
	data := []byte("azure export range payload")
	sum := md5.Sum(data)
	for _, test := range []struct {
		name       string
		contentMD5 string
		options    []SASOption
		wantMD5    string
		wantErr    string
	}{
		{
			name:       "matching checksum",
			contentMD5: base64.StdEncoding.EncodeToString(sum[:]),
			wantMD5:    "true",
		},
		{
			name:       "mismatched checksum",
			contentMD5: base64.StdEncoding.EncodeToString(md5Sum([]byte("other payload"))),
			wantMD5:    "true",
			wantErr:    "Content-MD5 mismatch",
		},
		{
			name:    "missing checksum fails closed",
			wantMD5: "true",
			wantErr: "did not include Content-MD5",
		},
		{
			name:    "missing checksum accepted without validation",
			options: []SASOption{WithoutRangeChecksum()},
			wantMD5: "",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			recorder := &rangeRequestRecorder{}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
				recorder.record(request)
				if test.contentMD5 != "" {
					w.Header().Set("Content-MD5", test.contentMD5)
				}
				w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-%d/%d", len(data)-1, len(data)))
				w.WriteHeader(http.StatusPartialContent)
				_, _ = w.Write(data)
			}))
			defer server.Close()

			options := append([]SASOption{
				// Disable retries so a server-side failure cannot turn into a
				// multi-second test.
				WithRetryOptions(policy.RetryOptions{MaxRetries: -1}),
			}, test.options...)
			api := NewSASBlobAPI(server.URL+"/md-test/blob?sv=test&sig=test", options...)

			value, err := api.ReadRange(context.Background(), 0, int64(len(data)))
			if test.wantErr == "" {
				if err != nil {
					t.Fatalf("ReadRange: %v", err)
				}
				if !bytes.Equal(value, data) {
					t.Fatalf("ReadRange = %q, want %q", value, data)
				}
			} else {
				if err == nil {
					t.Fatalf("ReadRange succeeded, want error containing %q", test.wantErr)
				}
				if !strings.Contains(err.Error(), test.wantErr) {
					t.Fatalf("ReadRange error = %v, want it to contain %q", err, test.wantErr)
				}
			}

			rangeHeader, md5Header := recorder.headers()
			if want := fmt.Sprintf("bytes=0-%d", len(data)-1); rangeHeader != want {
				t.Fatalf("Range header = %q, want %q", rangeHeader, want)
			}
			if md5Header != test.wantMD5 {
				t.Fatalf("x-ms-range-get-content-md5 = %q, want %q", md5Header, test.wantMD5)
			}
		})
	}
}

func md5Sum(value []byte) []byte {
	sum := md5.Sum(value)
	return sum[:]
}

func TestNewSASBlobAPIInvalidURLHidesSignature(t *testing.T) {
	// The invalid escape makes url.Parse fail.
	api := NewSASBlobAPI("https://account.blob.core.windows.net/%zz?sv=test&sig=secret")
	_, err := api.Size(context.Background())
	if err == nil {
		t.Fatal("Size succeeded with an invalid SAS URL")
	}
	if strings.Contains(err.Error(), "secret") {
		t.Fatalf("error exposes the SAS signature: %v", err)
	}
	if id := api.(blobIdentifier).BlobIdentifier(); strings.Contains(id, "secret") {
		t.Fatalf("identifier exposes the SAS signature: %q", id)
	}
}

func TestValidateRangeContentMD5(t *testing.T) {
	data := []byte("range data")
	sum := md5.Sum(data)
	if err := validateRangeContentMD5(data, sum[:]); err != nil {
		t.Fatalf("validateRangeContentMD5: %v", err)
	}
	if err := validateRangeContentMD5(data, []byte("bad")); err == nil {
		t.Fatal("validateRangeContentMD5 accepted a mismatched checksum")
	}
	if err := validateRangeContentMD5(data, nil); err == nil {
		t.Fatal("validateRangeContentMD5 accepted a missing checksum")
	}
}

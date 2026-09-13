package azurediskfile

import (
	"bytes"
	"context"
	"crypto/md5"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"
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

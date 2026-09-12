package azurediskfile

import (
	"bytes"
	"context"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type testBlob struct {
	id       string
	data     []byte
	ranges   []Range
	delay    chan struct{}
	started  chan struct{}
	requests atomic.Int64
}

func (b *testBlob) BlobIdentifier() string { return b.id }
func (b *testBlob) Size(context.Context) (int64, error) {
	return int64(len(b.data)), nil
}
func (b *testBlob) PageRanges(context.Context) ([]Range, error) { return b.ranges, nil }
func (b *testBlob) ReadRange(ctx context.Context, off, count int64) ([]byte, error) {
	b.requests.Add(1)
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

package snapshot

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	azurediskfile "github.com/yusuke-koyoshi/go-azure-managed-disks-file"
)

// memoryBlob is an in-memory BlobAPI whose methods can answer 403 a set number
// of times, the way a SAS behaves while it propagates.
type memoryBlob struct {
	mu     sync.Mutex
	data   []byte
	ranges []azurediskfile.Range
	forbid map[string]int
	err    error
	calls  map[string]int
}

func newMemoryBlob(data []byte, ranges []azurediskfile.Range, forbid map[string]int) *memoryBlob {
	if forbid == nil {
		forbid = map[string]int{}
	}
	return &memoryBlob{data: data, ranges: ranges, forbid: forbid, calls: map[string]int{}}
}

func (b *memoryBlob) answer(method string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.calls[method]++
	if b.forbid[method] > 0 {
		b.forbid[method]--
		return &azcore.ResponseError{StatusCode: http.StatusForbidden, ErrorCode: "AuthenticationFailed"}
	}
	return b.err
}

func (b *memoryBlob) callCount(method string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.calls[method]
}

func (b *memoryBlob) BlobIdentifier() string { return "memory" }

func (b *memoryBlob) Size(context.Context) (int64, error) {
	if err := b.answer("Size"); err != nil {
		return 0, err
	}
	return int64(len(b.data)), nil
}

func (b *memoryBlob) PageRanges(context.Context) ([]azurediskfile.Range, error) {
	if err := b.answer("PageRanges"); err != nil {
		return nil, err
	}
	return b.ranges, nil
}

func (b *memoryBlob) ReadRange(_ context.Context, off, count int64) ([]byte, error) {
	if err := b.answer("ReadRange"); err != nil {
		return nil, err
	}
	return append([]byte(nil), b.data[off:off+count]...), nil
}

func isForbidden(err error) bool {
	var responseErr *azcore.ResponseError
	return errors.As(err, &responseErr) && responseErr.StatusCode == http.StatusForbidden
}

func TestActivatingBlobAPI(t *testing.T) {
	data := []byte("snapshot payload")

	for _, method := range []string{"Size", "PageRanges", "ReadRange"} {
		t.Run("retries 403 from "+method, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				blob := newMemoryBlob(data, nil, map[string]int{method: 2})
				api := &activatingBlobAPI{api: blob, activeBy: time.Now().Add(sasActivationTimeout)}
				start := time.Now()
				var err error
				switch method {
				case "Size":
					_, err = api.Size(t.Context())
				case "PageRanges":
					_, err = api.PageRanges(t.Context())
				case "ReadRange":
					_, err = api.ReadRange(t.Context(), 0, 4)
				}
				if err != nil {
					t.Fatal(err)
				}
				if got := blob.callCount(method); got != 3 {
					t.Errorf("calls = %d, want 3", got)
				}
				if got := time.Since(start); got != 2*sasActivationInterval {
					t.Errorf("waited %s, want %s", got, 2*sasActivationInterval)
				}
			})
		})
	}

	t.Run("gives up at the activation deadline", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			blob := newMemoryBlob(data, nil, map[string]int{"Size": 1 << 30})
			start := time.Now()
			api := &activatingBlobAPI{api: blob, activeBy: start.Add(sasActivationTimeout)}
			if _, err := api.Size(t.Context()); !isForbidden(err) {
				t.Fatalf("error = %v, want HTTP 403", err)
			}
			if got := time.Since(start); got > sasActivationTimeout {
				t.Errorf("waited %s, beyond %s", got, sasActivationTimeout)
			}
		})
	})

	t.Run("does not retry 403 after the deadline", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			blob := newMemoryBlob(data, nil, map[string]int{"Size": 1})
			api := &activatingBlobAPI{api: blob, activeBy: time.Now()}
			if _, err := api.Size(t.Context()); !isForbidden(err) {
				t.Fatalf("error = %v, want HTTP 403", err)
			}
			if got := blob.callCount("Size"); got != 1 {
				t.Errorf("calls = %d, want 1", got)
			}
		})
	})

	t.Run("does not retry other errors", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			blob := newMemoryBlob(data, nil, nil)
			blob.err = &azcore.ResponseError{StatusCode: http.StatusNotFound}
			api := &activatingBlobAPI{api: blob, activeBy: time.Now().Add(sasActivationTimeout)}
			if _, err := api.Size(t.Context()); err == nil {
				t.Fatal("Size hid a 404")
			}
			if got := blob.callCount("Size"); got != 1 {
				t.Errorf("calls = %d, want 1", got)
			}
		})
	})

	t.Run("stops waiting when the context is cancelled", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			blob := newMemoryBlob(data, nil, map[string]int{"Size": 1 << 30})
			api := &activatingBlobAPI{api: blob, activeBy: time.Now().Add(sasActivationTimeout)}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			start := time.Now()
			if _, err := api.Size(ctx); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("error = %v, want the context error", err)
			}
			if got := time.Since(start); got != 5*time.Second {
				t.Errorf("waited %s, want 5s", got)
			}
		})
	})

	t.Run("keeps the blob identifier", func(t *testing.T) {
		api := &activatingBlobAPI{api: newMemoryBlob(data, nil, nil)}
		if got := api.BlobIdentifier(); got != "memory" {
			t.Errorf("BlobIdentifier = %q, want memory", got)
		}
	})
}

func useBlobAPI(t *testing.T, api azurediskfile.BlobAPI) {
	t.Helper()
	original := newSASBlobAPI
	newSASBlobAPI = func(string, ...azurediskfile.SASOption) azurediskfile.BlobAPI { return api }
	t.Cleanup(func() { newSASBlobAPI = original })
}

func TestGrantAccessAndOpenWaitsForActivation(t *testing.T) {
	const durationSeconds = 1200

	t.Run("opens and reads through a propagating SAS", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			data := []byte("allocated.......unallocated.....")
			blob := newMemoryBlob(data, []azurediskfile.Range{{Start: 0, End: 15}},
				map[string]int{"PageRanges": 1, "ReadRange": 1})
			useBlobAPI(t, blob)
			client := &fakeSnapshotClient{sas: "https://example.invalid/blob?sig=test"}
			reader, _, err := grantAccessAndOpen(t.Context(), client, durationSeconds, nil, Options{})
			if err != nil {
				t.Fatal(err)
			}
			buf := make([]byte, 16)
			if _, err := reader.ReadAt(buf, 0); err != nil {
				t.Fatalf("read of allocated data: %v", err)
			}
			if string(buf) != "allocated......." {
				t.Errorf("read %q", buf)
			}
			// PageRanges was retried rather than dropped, so the unallocated
			// block is served without a range request.
			reads := blob.callCount("ReadRange")
			if _, err := reader.ReadAt(buf, 16); err != nil {
				t.Fatal(err)
			}
			if got := blob.callCount("ReadRange"); got != reads {
				t.Errorf("unallocated read made %d range requests", got-reads)
			}
			if got := client.revokes(); got != 0 {
				t.Errorf("revocations = %d, want 0", got)
			}
		})
	})

	t.Run("revokes when the SAS never becomes active", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			useBlobAPI(t, newMemoryBlob(nil, nil, map[string]int{"Size": 1 << 30}))
			client := &fakeSnapshotClient{sas: "https://example.invalid/blob?sig=test"}
			_, _, err := grantAccessAndOpen(t.Context(), client, durationSeconds, nil, Options{})
			if !isForbidden(err) {
				t.Fatalf("error = %v, want HTTP 403", err)
			}
			if got := client.revokes(); got != 1 {
				t.Errorf("revocations = %d, want 1", got)
			}
		})
	})
}

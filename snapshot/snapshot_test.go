package snapshot

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
)

// fakeSnapshotClient records the SAS lifecycle calls in the order they happen.
type fakeSnapshotClient struct {
	mu             sync.Mutex
	sas            string
	grantErr       error
	revokeFailures int
	calls          []string
	durations      []int32
}

func (c *fakeSnapshotClient) GrantReadAccess(_ context.Context, durationSeconds int32) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, "grant")
	c.durations = append(c.durations, durationSeconds)
	if c.grantErr != nil {
		return "", c.grantErr
	}
	return c.sas, nil
}

func (c *fakeSnapshotClient) RevokeAccess(context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, "revoke")
	if c.revokeFailures > 0 {
		c.revokeFailures--
		return errors.New("revoke failed")
	}
	return nil
}

func (c *fakeSnapshotClient) recorded() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.calls...)
}

func (c *fakeSnapshotClient) revokes() int {
	count := 0
	for _, call := range c.recorded() {
		if call == "revoke" {
			count++
		}
	}
	return count
}

func (c *fakeSnapshotClient) grantedDurations() []int32 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]int32(nil), c.durations...)
}

// newBlobServer serves the part of the Blob API that Open uses: blob
// properties, an empty page range list, and range reads. Range responses
// deliberately omit Content-MD5, which is what SkipRangeChecksum controls.
func newBlobServer(t *testing.T, data []byte) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch {
		case request.Method == http.MethodHead:
			w.Header().Set("Content-Length", strconv.Itoa(len(data)))
			w.Header().Set("x-ms-blob-type", "PageBlob")
		case request.URL.Query().Get("comp") == "pagelist":
			w.Header().Set("Content-Type", "application/xml")
			_, _ = io.WriteString(w, `<?xml version="1.0" encoding="utf-8"?><PageList />`)
		default:
			var start, end int64
			if _, err := fmt.Sscanf(request.Header.Get("x-ms-range"), "bytes=%d-%d", &start, &end); err != nil {
				http.Error(w, "unexpected range", http.StatusBadRequest)
				return
			}
			if end >= int64(len(data)) {
				end = int64(len(data)) - 1
			}
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(data[start : end+1])
		}
	}))
	t.Cleanup(server.Close)
	return server
}

func newFakeClient(t *testing.T, data []byte) *fakeSnapshotClient {
	t.Helper()
	return &fakeSnapshotClient{sas: newBlobServer(t, data).URL + "/md-test/blob?sv=test&sig=test"}
}

func TestGrantAccessAndOpen(t *testing.T) {
	const durationSeconds = 1200
	data := []byte("snapshot payload")

	// A SAS left behind stays readable until it expires, so revocation must
	// not depend on any Options field being set.
	t.Run("revokes on cleanup", func(t *testing.T) {
		for _, options := range []Options{{}, {Duration: 45 * time.Minute}} {
			client := newFakeClient(t, data)
			_, cleanup, err := grantAccessAndOpen(t.Context(), client, durationSeconds, nil, options)
			if err != nil {
				t.Fatalf("%+v: %v", options, err)
			}
			if err := cleanup(); err != nil {
				t.Fatalf("%+v: cleanup: %v", options, err)
			}
			if got := client.revokes(); got != 1 {
				t.Errorf("%+v: revocations = %d, want 1", options, got)
			}
		}
	})

	// Granting replaces an active SAS, so revoking one first only adds an
	// ARM round trip.
	t.Run("grants without revoking first", func(t *testing.T) {
		client := newFakeClient(t, data)
		if _, _, err := grantAccessAndOpen(t.Context(), client, durationSeconds, nil, Options{}); err != nil {
			t.Fatal(err)
		}
		want := []string{"grant"}
		if got := client.recorded(); !slices.Equal(got, want) {
			t.Errorf("calls = %v, want %v", got, want)
		}
	})

	t.Run("cleanup revokes once when called repeatedly", func(t *testing.T) {
		client := newFakeClient(t, data)
		_, cleanup, err := grantAccessAndOpen(t.Context(), client, durationSeconds, nil, Options{})
		if err != nil {
			t.Fatal(err)
		}
		for range 2 {
			if err := cleanup(); err != nil {
				t.Fatalf("cleanup: %v", err)
			}
		}
		if got := client.revokes(); got != 1 {
			t.Errorf("revocations = %d, want 1", got)
		}
	})

	t.Run("cleanup does nothing when revocation is skipped", func(t *testing.T) {
		client := newFakeClient(t, data)
		options := Options{SkipRevokeOnCleanup: true}
		_, cleanup, err := grantAccessAndOpen(t.Context(), client, durationSeconds, nil, options)
		if err != nil {
			t.Fatal(err)
		}
		if err := cleanup(); err != nil {
			t.Fatalf("cleanup: %v", err)
		}
		if got := client.revokes(); got != 0 {
			t.Errorf("revocations = %d, want 0", got)
		}
	})

	t.Run("revokes when the grant returns no SAS", func(t *testing.T) {
		client := &fakeSnapshotClient{}
		_, _, err := grantAccessAndOpen(t.Context(), client, durationSeconds, nil, Options{})
		if err == nil {
			t.Fatal("grantAccessAndOpen succeeded without a SAS URI")
		}
		if got := client.revokes(); got != 1 {
			t.Errorf("revocations = %d, want 1", got)
		}
	})

	// The grant is a long-running operation, so an error can arrive after ARM
	// has already issued the SAS. Revocation must not depend on the grant call
	// reporting success.
	t.Run("revokes when the grant fails", func(t *testing.T) {
		client := &fakeSnapshotClient{grantErr: errors.New("grant failed")}
		_, _, err := grantAccessAndOpen(t.Context(), client, durationSeconds, nil, Options{})
		if err == nil {
			t.Fatal("grantAccessAndOpen succeeded with a failing grant")
		}
		if got := client.revokes(); got != 1 {
			t.Errorf("revocations = %d, want 1", got)
		}
	})

	t.Run("revokes when the reader cannot be opened", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "no such blob", http.StatusNotFound)
		}))
		t.Cleanup(server.Close)
		client := &fakeSnapshotClient{sas: server.URL + "/md-test/blob?sv=test&sig=test"}
		_, _, err := grantAccessAndOpen(t.Context(), client, durationSeconds, nil, Options{})
		if err == nil {
			t.Fatal("grantAccessAndOpen succeeded against a missing blob")
		}
		if got := client.revokes(); got != 1 {
			t.Errorf("revocations = %d, want 1", got)
		}
	})

	t.Run("cleanup reports a failed revocation and retries it", func(t *testing.T) {
		client := newFakeClient(t, data)
		client.revokeFailures = 1
		_, cleanup, err := grantAccessAndOpen(t.Context(), client, durationSeconds, nil, Options{})
		if err != nil {
			t.Fatal(err)
		}
		if err := cleanup(); err == nil {
			t.Fatal("cleanup hid a failed revocation")
		}
		if err := cleanup(); err != nil {
			t.Fatalf("retried cleanup: %v", err)
		}
		if err := cleanup(); err != nil {
			t.Fatalf("cleanup after success: %v", err)
		}
		if got := client.revokes(); got != 2 {
			t.Errorf("revocations = %d, want 2", got)
		}
	})

	t.Run("reports a failed revocation after a failed grant", func(t *testing.T) {
		grantErr := errors.New("grant failed")
		client := &fakeSnapshotClient{grantErr: grantErr, revokeFailures: 1}
		_, _, err := grantAccessAndOpen(t.Context(), client, durationSeconds, nil, Options{})
		if !errors.Is(err, grantErr) {
			t.Fatalf("error = %v, want the grant error", err)
		}
		if !strings.Contains(err.Error(), "revoke SAS") {
			t.Fatalf("error = %v, want the revocation failure too", err)
		}
	})

	t.Run("grants the requested duration", func(t *testing.T) {
		client := newFakeClient(t, data)
		if _, _, err := grantAccessAndOpen(t.Context(), client, 2700, nil, Options{}); err != nil {
			t.Fatal(err)
		}
		if got := client.grantedDurations(); len(got) != 1 || got[0] != 2700 {
			t.Errorf("granted durations = %v, want [2700]", got)
		}
	})

	// The blob server omits Content-MD5, so validation decides whether the
	// read succeeds. This is what threads Options through to the reader.
	t.Run("range checksums", func(t *testing.T) {
		for _, test := range []struct {
			name    string
			options Options
			wantErr bool
		}{
			{name: "validated by default", wantErr: true},
			{name: "skipped on request", options: Options{SkipRangeChecksum: true}},
		} {
			t.Run(test.name, func(t *testing.T) {
				client := newFakeClient(t, data)
				reader, _, err := grantAccessAndOpen(t.Context(), client, durationSeconds, nil, test.options)
				if err != nil {
					t.Fatal(err)
				}
				buf := make([]byte, len(data))
				_, err = reader.ReadAt(buf, 0)
				if test.wantErr {
					if err == nil || !strings.Contains(err.Error(), "Content-MD5") {
						t.Fatalf("ReadAt error = %v, want a Content-MD5 error", err)
					}
					return
				}
				if err != nil {
					t.Fatalf("ReadAt: %v", err)
				}
				if string(buf) != string(data) {
					t.Fatalf("ReadAt = %q, want %q", buf, data)
				}
			})
		}
	})
}

func TestGrantAccessAndOpenWithOptions(t *testing.T) {
	for _, test := range []struct {
		name       string
		cred       azcore.TokenCredential
		resourceID string
		options    Options
	}{
		{
			name:       "nil credential",
			resourceID: "/subscriptions/s/resourceGroups/g/providers/Microsoft.Compute/snapshots/n",
		},
		{
			name:       "resource ID is not a snapshot",
			cred:       fakeCredential{},
			resourceID: "not-a-resource-id",
		},
		{
			name:       "negative duration",
			cred:       fakeCredential{},
			resourceID: "/subscriptions/s/resourceGroups/g/providers/Microsoft.Compute/snapshots/n",
			options:    Options{Duration: -time.Second},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			// Every case must fail before any ARM or blob request is made.
			_, _, err := GrantAccessAndOpenWithOptions(t.Context(), test.cred, test.resourceID, nil, test.options)
			if err == nil {
				t.Fatal("GrantAccessAndOpenWithOptions accepted invalid input")
			}
		})
	}
}

func TestParseSnapshotID(t *testing.T) {
	for _, test := range []struct {
		resourceID string
		wantErr    bool
	}{
		{resourceID: "/subscriptions/s/resourceGroups/g/providers/Microsoft.Compute/snapshots/n"},
		{resourceID: "/subscriptions/s/resourceGroups/g/providers/microsoft.compute/Snapshots/n"},
		{resourceID: "/subscriptions/s/resourceGroups/g/providers/Microsoft.Compute/disks/n", wantErr: true},
		{resourceID: "/subscriptions/s/resourceGroups/g", wantErr: true},
		{resourceID: "not-a-resource-id", wantErr: true},
	} {
		id, err := parseSnapshotID(test.resourceID)
		if test.wantErr {
			if err == nil {
				t.Errorf("parseSnapshotID(%q) accepted a non-snapshot ID", test.resourceID)
			}
			continue
		}
		if err != nil {
			t.Errorf("parseSnapshotID(%q): %v", test.resourceID, err)
			continue
		}
		if id.Name != "n" || id.ResourceGroupName != "g" {
			t.Errorf("parseSnapshotID(%q) = %s/%s, want g/n", test.resourceID, id.ResourceGroupName, id.Name)
		}
	}
}

func TestAccessDurationSeconds(t *testing.T) {
	for _, test := range []struct {
		duration time.Duration
		want     int32
		wantErr  bool
	}{
		{duration: 0, want: int32(defaultAccessDuration / time.Second)},
		{duration: 45 * time.Minute, want: 2700},
		{duration: -time.Second, wantErr: true},
		{duration: 500 * time.Millisecond, wantErr: true},
		{duration: time.Duration(1<<31) * time.Second, wantErr: true},
	} {
		got, err := accessDurationSeconds(test.duration)
		if test.wantErr {
			if err == nil {
				t.Errorf("accessDurationSeconds(%v) = %d, want an error", test.duration, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("accessDurationSeconds(%v): %v", test.duration, err)
			continue
		}
		if got != test.want {
			t.Errorf("accessDurationSeconds(%v) = %d, want %d", test.duration, got, test.want)
		}
	}
}

type fakeCredential struct{}

func (fakeCredential) GetToken(context.Context, policy.TokenRequestOptions) (azcore.AccessToken, error) {
	return azcore.AccessToken{}, errors.New("fake credential")
}

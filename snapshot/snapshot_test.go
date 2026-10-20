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
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	azfake "github.com/Azure/azure-sdk-for-go/sdk/azcore/fake"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v9"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v9/fake"
	azurediskfile "github.com/yusuke-koyoshi/go-azure-managed-disks-file"
)

// fakeSnapshotClient records the SAS lifecycle calls in the order they happen.
type fakeSnapshotClient struct {
	mu             sync.Mutex
	state          string
	stateErr       error
	sas            string
	grantErr       error
	revokeFailures int
	revokeErr      error
	calls          []string
	durations      []int32
}

func (c *fakeSnapshotClient) State(context.Context) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, "state")
	return c.state, c.stateErr
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
	return c.revokeErr
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
	server := httptest.NewServer(blobHandler(data))
	t.Cleanup(server.Close)
	return server
}

func blobHandler(data []byte) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
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
	})
}

func newClientFor(t *testing.T, handler http.Handler) *fakeSnapshotClient {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return &fakeSnapshotClient{sas: server.URL + "/md-test/blob?sv=test&sig=test"}
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

	t.Run("checks the state and grants without revoking first", func(t *testing.T) {
		// "" is a snapshot without a diskState, which is granted as well.
		for _, state := range []string{string(armcompute.DiskStateUnattached), ""} {
			client := newFakeClient(t, data)
			client.state = state
			if _, _, err := grantAccessAndOpen(t.Context(), client, durationSeconds, nil, Options{}); err != nil {
				t.Fatalf("diskState %q: %v", state, err)
			}
			want := []string{"state", "grant"}
			if got := client.recorded(); !slices.Equal(got, want) {
				t.Errorf("diskState %q: calls = %v, want %v", state, got, want)
			}
		}
	})

	t.Run("refuses a snapshot in use without granting or revoking", func(t *testing.T) {
		for _, state := range []armcompute.DiskState{
			armcompute.DiskStateActiveSAS, armcompute.DiskStateActiveSASFrozen,
			armcompute.DiskStateActiveUpload, armcompute.DiskStateReadyToUpload,
		} {
			client := newFakeClient(t, data)
			client.state = string(state)
			_, _, err := grantAccessAndOpen(t.Context(), client, durationSeconds, nil, Options{})
			if !errors.Is(err, ErrSnapshotInUse) || !strings.Contains(err.Error(), string(state)) {
				t.Errorf("diskState %s: error = %v, want ErrSnapshotInUse naming the state", state, err)
			}
			if got := client.recorded(); !slices.Equal(got, []string{"state"}) {
				t.Errorf("diskState %s: calls = %v, want only the state check", state, got)
			}
		}
	})

	t.Run("neither grants nor revokes when the state cannot be read", func(t *testing.T) {
		stateErr := &azcore.ResponseError{StatusCode: http.StatusNotFound}
		client := &fakeSnapshotClient{stateErr: stateErr}
		_, _, err := grantAccessAndOpen(t.Context(), client, durationSeconds, nil, Options{})
		if !errors.Is(err, stateErr) {
			t.Fatalf("error = %v, want the state error", err)
		}
		if got := client.recorded(); !slices.Equal(got, []string{"state"}) {
			t.Errorf("calls = %v, want only the state check", got)
		}
	})

	t.Run("replaces an active SAS when the check is skipped", func(t *testing.T) {
		client := newFakeClient(t, data)
		client.state = string(armcompute.DiskStateActiveSAS)
		if _, _, err := grantAccessAndOpen(t.Context(), client, durationSeconds, nil, Options{SkipActiveSASCheck: true}); err != nil {
			t.Fatal(err)
		}
		if got := client.recorded(); !slices.Equal(got, []string{"grant"}) {
			t.Errorf("calls = %v, want only the grant", got)
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
		var requests atomic.Int64
		client := newClientFor(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			requests.Add(1)
			http.Error(w, "no such blob", http.StatusNotFound)
		}))
		_, _, err := grantAccessAndOpen(t.Context(), client, durationSeconds, nil, Options{})
		if err == nil {
			t.Fatal("grantAccessAndOpen succeeded against a missing blob")
		}
		if got := requests.Load(); got != 1 {
			t.Errorf("requests = %d, want 1 (only 403 is retried)", got)
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

	t.Run("treats a missing snapshot as revoked", func(t *testing.T) {
		client := newFakeClient(t, data)
		client.revokeErr = &azcore.ResponseError{StatusCode: http.StatusNotFound}
		_, cleanup, err := grantAccessAndOpen(t.Context(), client, durationSeconds, nil, Options{})
		if err != nil {
			t.Fatal(err)
		}
		if err := cleanup(); err != nil {
			t.Fatalf("cleanup: %v", err)
		}
	})

	t.Run("reports other revocation status codes", func(t *testing.T) {
		client := newFakeClient(t, data)
		client.revokeErr = &azcore.ResponseError{StatusCode: http.StatusForbidden}
		_, cleanup, err := grantAccessAndOpen(t.Context(), client, durationSeconds, nil, Options{})
		if err != nil {
			t.Fatal(err)
		}
		if err := cleanup(); err == nil {
			t.Fatal("cleanup hid a forbidden revocation")
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

// rangeRecorder serves blobHandler, fails the first range reads with the given
// statuses, and records how many range reads arrive and overlap.
type rangeRecorder struct {
	next      http.Handler
	failures  []int
	delay     time.Duration
	requests  atomic.Int64
	active    atomic.Int64
	maxActive atomic.Int64
}

func (r *rangeRecorder) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	if request.Method == http.MethodHead || request.URL.Query().Get("comp") == "pagelist" {
		r.next.ServeHTTP(w, request)
		return
	}
	n := r.requests.Add(1)
	active := r.active.Add(1)
	defer r.active.Add(-1)
	for {
		maxActive := r.maxActive.Load()
		if active <= maxActive || r.maxActive.CompareAndSwap(maxActive, active) {
			break
		}
	}
	time.Sleep(r.delay)
	if n <= int64(len(r.failures)) {
		w.WriteHeader(r.failures[n-1])
		return
	}
	r.next.ServeHTTP(w, request)
}

func TestGrantAccessAndOpenPassesReadOptions(t *testing.T) {
	const durationSeconds = 1200
	data := []byte("snapshot payload")

	t.Run("reader options", func(t *testing.T) {
		recorder := &rangeRecorder{next: blobHandler(data), delay: 10 * time.Millisecond}
		client := newClientFor(t, recorder)
		reader, _, err := grantAccessAndOpen(t.Context(), client, durationSeconds, nil, Options{
			SkipRangeChecksum: true,
			Open:              azurediskfile.OpenOptions{BlockSize: 4, MaxConcurrentBlocks: 1},
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := reader.ReadAt(make([]byte, len(data)), 0); err != nil {
			t.Fatalf("ReadAt: %v", err)
		}
		if got := recorder.requests.Load(); got != 4 {
			t.Errorf("range reads = %d, want 4 blocks of 4 bytes", got)
		}
		if got := recorder.maxActive.Load(); got != 1 {
			t.Errorf("concurrent range reads = %d, want 1", got)
		}
	})

	for _, test := range []struct {
		name         string
		failures     []int
		retry        policy.RetryOptions
		wantRequests int64
		wantStatus   int
	}{
		{
			// The default policy would retry the 503, so a failure shows that
			// these options replaced it.
			name:         "retry options replace the default",
			failures:     []int{http.StatusServiceUnavailable},
			retry:        policy.RetryOptions{MaxRetries: -1},
			wantRequests: 1,
			wantStatus:   http.StatusServiceUnavailable,
		},
		{
			name:         "retried 503",
			failures:     []int{http.StatusServiceUnavailable, http.StatusServiceUnavailable},
			retry:        policy.RetryOptions{RetryDelay: time.Millisecond},
			wantRequests: 3,
		},
		{
			// Documented against, but the SDK retrying 403 as well must not
			// break the read. Finishing before sasActivationInterval shows that
			// the SDK, not this package, retried it.
			name:         "403 in StatusCodes",
			failures:     []int{http.StatusForbidden},
			retry:        policy.RetryOptions{RetryDelay: time.Millisecond, StatusCodes: []int{http.StatusForbidden}},
			wantRequests: 2,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			recorder := &rangeRecorder{next: blobHandler(data), failures: test.failures}
			client := newClientFor(t, recorder)
			reader, _, err := grantAccessAndOpen(t.Context(), client, durationSeconds, nil, Options{
				SkipRangeChecksum: true,
				Retry:             test.retry,
			})
			if err != nil {
				t.Fatal(err)
			}
			buf := make([]byte, len(data))
			start := time.Now()
			_, err = reader.ReadAt(buf, 0)
			if elapsed := time.Since(start); elapsed >= sasActivationInterval {
				t.Errorf("ReadAt took %v, want less than %v", elapsed, sasActivationInterval)
			}
			if test.wantStatus != 0 {
				var responseErr *azcore.ResponseError
				if !errors.As(err, &responseErr) || responseErr.StatusCode != test.wantStatus {
					t.Fatalf("ReadAt error = %v, want status %d", err, test.wantStatus)
				}
			} else if err != nil || string(buf) != string(data) {
				t.Fatalf("ReadAt = %q, %v; want %q", buf, err, data)
			}
			if got := recorder.requests.Load(); got != test.wantRequests {
				t.Errorf("range reads = %d, want %d", got, test.wantRequests)
			}
		})
	}
}

// attemptRecorder is a per-retry policy that records each attempt it sees as
// "method last-path-segment status api-name".
type attemptRecorder struct {
	mu       sync.Mutex
	attempts []string
}

func (r *attemptRecorder) Do(req *policy.Request) (*http.Response, error) {
	resp, err := req.Next()
	path := req.Raw().URL.Path
	segment := path[strings.LastIndex(path, "/")+1:]
	if req.Raw().URL.Query().Get("comp") == "pagelist" {
		segment = "pagelist"
	}
	status := "error"
	if resp != nil {
		status = strconv.Itoa(resp.StatusCode)
	}
	apiName, _ := req.Raw().Context().Value(runtime.CtxAPINameKey{}).(string)
	r.mu.Lock()
	r.attempts = append(r.attempts, strings.TrimSpace(strings.Join([]string{req.Raw().Method, segment, status, apiName}, " ")))
	r.mu.Unlock()
	return resp, err
}

func (r *attemptRecorder) recorded() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.attempts...)
}

func TestUniqueID(t *testing.T) {
	const resourceID = "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Compute/snapshots/snap"
	for _, test := range []struct {
		name       string
		resourceID string
		nilCred    bool
		properties *armcompute.SnapshotProperties
		getStatus  int
		want       string
		wantErr    string
		wantGets   int64
	}{
		{
			name:       "returns the uniqueId",
			properties: &armcompute.SnapshotProperties{UniqueID: new("2f0c4a0e-0000-4000-8000-000000000001")},
			want:       "2f0c4a0e-0000-4000-8000-000000000001",
			wantGets:   1,
		},
		{name: "empty uniqueId", properties: &armcompute.SnapshotProperties{UniqueID: new("")}, wantErr: "no uniqueId", wantGets: 1},
		{name: "no properties", wantErr: "no uniqueId", wantGets: 1},
		{name: "Get fails", getStatus: http.StatusNotFound, wantErr: "ResourceNotFound", wantGets: 1},
		{
			name:       "disk resource ID",
			resourceID: "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Compute/disks/snap",
			wantErr:    "must identify a snapshot",
		},
		{name: "nil credential", nilCred: true, wantErr: "nil TokenCredential"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var gets atomic.Int64
			transport := fake.NewSnapshotsServerTransport(&fake.SnapshotsServer{
				Get: func(context.Context, string, string, *armcompute.SnapshotsClientGetOptions) (resp azfake.Responder[armcompute.SnapshotsClientGetResponse], errResp azfake.ErrorResponder) {
					gets.Add(1)
					if test.getStatus != 0 {
						errResp.SetResponseError(test.getStatus, "ResourceNotFound")
						return
					}
					resp.SetResponse(http.StatusOK, armcompute.SnapshotsClientGetResponse{
						Snapshot: armcompute.Snapshot{Properties: test.properties},
					}, nil)
					return
				},
			})
			id := test.resourceID
			if id == "" {
				id = resourceID
			}
			var cred azcore.TokenCredential = &azfake.TokenCredential{}
			if test.nilCred {
				cred = nil
			}
			recorder := &attemptRecorder{}
			got, err := UniqueID(t.Context(), cred, id, Options{
				PerRetryPolicies: []policy.Policy{recorder},
				Transport:        transport,
			})
			if test.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantErr) {
					t.Fatalf("UniqueID error = %v, want it to contain %q", err, test.wantErr)
				}
			} else if err != nil || got != test.want {
				t.Fatalf("UniqueID = %q, %v; want %q", got, err, test.want)
			}
			if n := gets.Load(); n != test.wantGets {
				t.Errorf("Get calls = %d, want %d", n, test.wantGets)
			}
			if test.wantGets > 0 {
				if attempts := recorder.recorded(); len(attempts) != 1 || !strings.HasSuffix(attempts[0], "SnapshotsClient.Get") {
					t.Errorf("policy saw %v, want one SnapshotsClient.Get attempt", attempts)
				}
			}
		})
	}
}

// throttleFirst answers the first request of each HTTP method with 429 and
// passes the rest to next. Errors from the fake servers are never retried, so
// the 429 the SDK retries has to come from here. Retry-After-Ms keeps the SDK
// from waiting its default backoff before the retry.
type throttleFirst struct {
	next policy.Transporter
	sent sync.Map
}

func (t *throttleFirst) Do(req *http.Request) (*http.Response, error) {
	if _, loaded := t.sent.LoadOrStore(req.Method, true); !loaded {
		header := http.Header{"Retry-After-Ms": []string{"1"}}
		return &http.Response{StatusCode: http.StatusTooManyRequests, Header: header, Body: http.NoBody, Request: req}, nil
	}
	return t.next.Do(req)
}

// hostRouter sends the requests for blobHost to the blob test server and the
// rest to arm, the way a caller's fake has to serve both clients.
type hostRouter struct {
	blobHost     string
	arm          policy.Transporter
	blobRequests atomic.Int64
}

func (r *hostRouter) Do(req *http.Request) (*http.Response, error) {
	if req.URL.Host != r.blobHost {
		return r.arm.Do(req)
	}
	r.blobRequests.Add(1)
	return http.DefaultTransport.RoundTrip(req)
}

func TestGrantAccessAndOpenWithOptionsPerRetryPolicies(t *testing.T) {
	data := []byte("snapshot payload")
	blobServer := newBlobServer(t, data)
	sas := blobServer.URL + "/md-test/blob?sv=test&sig=test"
	server := &fake.SnapshotsServer{
		Get: func(context.Context, string, string, *armcompute.SnapshotsClientGetOptions) (resp azfake.Responder[armcompute.SnapshotsClientGetResponse], errResp azfake.ErrorResponder) {
			state := armcompute.DiskStateUnattached
			resp.SetResponse(http.StatusOK, armcompute.SnapshotsClientGetResponse{
				Snapshot: armcompute.Snapshot{Properties: &armcompute.SnapshotProperties{DiskState: &state}},
			}, nil)
			return
		},
		BeginGrantAccess: func(context.Context, string, string, armcompute.GrantAccessData, *armcompute.SnapshotsClientBeginGrantAccessOptions) (resp azfake.PollerResponder[armcompute.SnapshotsClientGrantAccessResponse], errResp azfake.ErrorResponder) {
			resp.AddNonTerminalResponse(http.StatusAccepted, nil)
			resp.SetTerminalResponse(http.StatusOK, armcompute.SnapshotsClientGrantAccessResponse{
				AccessURI: armcompute.AccessURI{AccessSAS: &sas},
			}, nil)
			return
		},
		BeginRevokeAccess: func(context.Context, string, string, *armcompute.SnapshotsClientBeginRevokeAccessOptions) (resp azfake.PollerResponder[armcompute.SnapshotsClientRevokeAccessResponse], errResp azfake.ErrorResponder) {
			resp.SetTerminalResponse(http.StatusOK, armcompute.SnapshotsClientRevokeAccessResponse{}, nil)
			return
		},
	}
	router := &hostRouter{
		blobHost: blobServer.Listener.Addr().String(),
		arm:      &throttleFirst{next: fake.NewSnapshotsServerTransport(server)},
	}

	recorder := &attemptRecorder{}
	reader, cleanup, err := GrantAccessAndOpenWithOptions(t.Context(), &azfake.TokenCredential{},
		"/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Compute/snapshots/snap", nil, Options{
			SkipRangeChecksum: true,
			PerRetryPolicies:  []policy.Policy{recorder},
			Transport:         router,
		})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reader.ReadAt(make([]byte, len(data)), 0); err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	if err := cleanup(); err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	// The retried 429s of the state check and of the grant, and the polling of
	// the grant reach the policy. The polling GET carries the API name only
	// because the fake poller sets it; the real pollers poll on the caller's
	// context, which has none.
	want := []string{
		"GET snap 429 SnapshotsClient.Get",
		"GET snap 200 SnapshotsClient.Get",
		"POST beginGetAccess 429 SnapshotsClient.BeginGrantAccess",
		"POST beginGetAccess 202 SnapshotsClient.BeginGrantAccess",
		"GET status 200 SnapshotsClient.BeginGrantAccess",
		"HEAD blob 200",
		"GET pagelist 200",
		"GET blob 206",
		"POST endGetAccess 200 SnapshotsClient.BeginRevokeAccess",
	}
	if got := recorder.recorded(); !slices.Equal(got, want) {
		t.Errorf("policy saw\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
	if n := router.blobRequests.Load(); n != 3 {
		t.Errorf("the transport saw %d blob requests, want 3", n)
	}
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

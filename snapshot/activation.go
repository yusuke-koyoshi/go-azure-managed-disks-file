package snapshot

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	azurediskfile "github.com/yusuke-koyoshi/go-azure-managed-disks-file"
)

const (
	sasActivationTimeout  = 60 * time.Second
	sasActivationInterval = 2 * time.Second
)

// Tests replace it, because network I/O keeps a synctest bubble's clock from
// advancing.
var newSASBlobAPI = azurediskfile.NewSASBlobAPI

// activatingBlobAPI retries 403 until activeBy. A new SAS propagates to the
// storage front ends one by one over up to 30 seconds, so any request in that
// window can be rejected even after an earlier one succeeded.
type activatingBlobAPI struct {
	api      azurediskfile.BlobAPI
	activeBy time.Time
}

func (a *activatingBlobAPI) BlobIdentifier() string {
	if identified, ok := a.api.(interface{ BlobIdentifier() string }); ok {
		return identified.BlobIdentifier()
	}
	return ""
}

func (a *activatingBlobAPI) Size(ctx context.Context) (int64, error) {
	return retryForbidden(ctx, a.activeBy, func() (int64, error) { return a.api.Size(ctx) })
}

func (a *activatingBlobAPI) ReadRange(ctx context.Context, off, count int64) ([]byte, error) {
	return retryForbidden(ctx, a.activeBy, func() ([]byte, error) { return a.api.ReadRange(ctx, off, count) })
}

func (a *activatingBlobAPI) PageRanges(ctx context.Context) ([]azurediskfile.Range, error) {
	return retryForbidden(ctx, a.activeBy, func() ([]azurediskfile.Range, error) { return a.api.PageRanges(ctx) })
}

func retryForbidden[T any](ctx context.Context, activeBy time.Time, call func() (T, error)) (T, error) {
	for {
		value, err := call()
		var responseErr *azcore.ResponseError
		if err == nil || !errors.As(err, &responseErr) || responseErr.StatusCode != http.StatusForbidden ||
			time.Now().Add(sasActivationInterval).After(activeBy) {
			return value, err
		}
		select {
		case <-ctx.Done():
			var zero T
			return zero, errors.Join(err, ctx.Err())
		case <-time.After(sasActivationInterval):
		}
	}
}

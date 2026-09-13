// Package snapshot grants short-lived access to an Azure managed disk snapshot
// and opens it as a random-access reader.
package snapshot

import (
	"context"
	"errors"
	"io"
	"math"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute"
	azurediskfile "github.com/yusuke-koyoshi/go-azure-managed-disks-file"
)

const defaultAccessDuration = 20 * time.Minute

// Options configures snapshot SAS issuance and cleanup.
//
// A zero Duration uses the default 20-minute grant duration. By default,
// GrantAccessAndOpenWithOptions revokes an existing active SAS before granting
// access and revokes the new SAS during cleanup. Set the Skip fields only when
// deliberately opting out of those safety behaviors.
type Options struct {
	// Duration is the SAS lifetime. Zero uses the 20-minute default.
	Duration time.Duration
	// SkipPreRevokeActiveSAS skips revoking an existing active SAS when true.
	SkipPreRevokeActiveSAS bool
	// SkipRevokeOnCleanup skips revoking the granted SAS during cleanup when true.
	SkipRevokeOnCleanup bool
}

// GrantAccessAndOpen revokes a previous active SAS, grants a new read SAS for
// 20 minutes, and opens the snapshot through the root package. The cleanup
// function revokes the newly granted SAS and is safe to call more than once.
//
// The credential is intentionally explicit. This function does not construct
// DefaultAzureCredential and therefore cannot silently use an unintended
// managed identity.
func GrantAccessAndOpen(
	ctx context.Context,
	cred azcore.TokenCredential,
	resourceID string,
	cache azurediskfile.Cache[string, []byte],
) (*io.SectionReader, func(), error) {
	return GrantAccessAndOpenWithOptions(ctx, cred, resourceID, cache, Options{})
}

// GrantAccessAndOpenWithOptions is GrantAccessAndOpen with configurable SAS
// duration and revocation behavior.
func GrantAccessAndOpenWithOptions(
	ctx context.Context,
	cred azcore.TokenCredential,
	resourceID string,
	cache azurediskfile.Cache[string, []byte],
	options Options,
) (*io.SectionReader, func(), error) {
	if cred == nil {
		return nil, nil, errors.New("snapshot: nil TokenCredential")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	id, err := arm.ParseResourceID(resourceID)
	if err != nil {
		return nil, nil, err
	}
	if id.SubscriptionID == "" || id.ResourceGroupName == "" || id.Name == "" {
		return nil, nil, errors.New("snapshot: resource ID must identify a snapshot")
	}
	duration := options.Duration
	if duration == 0 {
		duration = defaultAccessDuration
	}
	if duration < 0 {
		return nil, nil, errors.New("snapshot: Duration must not be negative")
	}
	durationSeconds := duration / time.Second
	if durationSeconds <= 0 || durationSeconds > math.MaxInt32 {
		return nil, nil, errors.New("snapshot: Duration must be between one second and int32 maximum")
	}
	client, err := armcompute.NewSnapshotsClient(id.SubscriptionID, cred, nil)
	if err != nil {
		return nil, nil, err
	}

	state, err := client.Get(ctx, id.ResourceGroupName, id.Name, nil)
	if err != nil {
		return nil, nil, err
	}
	if !options.SkipPreRevokeActiveSAS && state.Properties != nil && state.Properties.DiskState != nil &&
		*state.Properties.DiskState == armcompute.DiskStateActiveSAS {
		if err := revoke(ctx, client, id.ResourceGroupName, id.Name); err != nil {
			return nil, nil, err
		}
	}

	grantPoller, err := client.BeginGrantAccess(ctx, id.ResourceGroupName, id.Name, armcompute.GrantAccessData{
		Access:                   new(armcompute.AccessLevelRead),
		DurationInSeconds:        new(int32(durationSeconds)),
		GetSecureVMGuestStateSAS: new(false),
	}, nil)
	if err != nil {
		return nil, nil, err
	}
	grant, err := grantPoller.PollUntilDone(ctx, nil)
	if err != nil {
		return nil, nil, err
	}
	if grant.AccessSAS == nil || *grant.AccessSAS == "" {
		revokeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = revoke(revokeCtx, client, id.ResourceGroupName, id.Name)
		return nil, nil, errors.New("snapshot: GrantAccess returned no SAS URI")
	}

	api := azurediskfile.NewSASBlobAPI(*grant.AccessSAS)
	reader, err := azurediskfile.Open(ctx, api, cache)
	if err != nil {
		revokeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = revoke(revokeCtx, client, id.ResourceGroupName, id.Name)
		return nil, nil, err
	}
	var once sync.Once
	cleanup := func() {
		if options.SkipRevokeOnCleanup {
			return
		}
		once.Do(func() {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			_ = revoke(cleanupCtx, client, id.ResourceGroupName, id.Name)
		})
	}
	return reader, cleanup, nil
}

func revoke(ctx context.Context, client *armcompute.SnapshotsClient, resourceGroup, name string) error {
	poller, err := client.BeginRevokeAccess(ctx, resourceGroup, name, nil)
	if err != nil {
		return err
	}
	_, err = poller.PollUntilDone(ctx, nil)
	return err
}

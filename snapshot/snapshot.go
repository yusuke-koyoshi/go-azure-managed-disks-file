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
// access, revokes the new SAS during cleanup, and validates range checksums.
// Set the Skip fields only when deliberately opting out of those safety
// behaviors.
type Options struct {
	// Duration is the SAS lifetime. Zero uses the 20-minute default.
	Duration time.Duration
	// SkipPreRevokeActiveSAS skips revoking an existing active SAS when true.
	SkipPreRevokeActiveSAS bool
	// SkipRevokeOnCleanup skips revoking the granted SAS during cleanup when true.
	SkipRevokeOnCleanup bool
	// SkipRangeChecksum disables per-range MD5 validation when true. Validation
	// fails closed, so this is the fallback for export endpoints that do not
	// support x-ms-range-get-content-md5. Validation only covers ranges of
	// 4 MiB or less, which the default block size satisfies.
	SkipRangeChecksum bool
}

// snapshotClient is the part of armcompute.SnapshotsClient this package uses.
// It covers the SAS lifecycle, which is the security-relevant behavior here,
// and lets that lifecycle be exercised without ARM.
type snapshotClient interface {
	DiskState(ctx context.Context) (armcompute.DiskState, error)
	GrantReadAccess(ctx context.Context, durationSeconds int32) (string, error)
	RevokeAccess(ctx context.Context) error
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
	durationSeconds, err := accessDurationSeconds(options.Duration)
	if err != nil {
		return nil, nil, err
	}
	client, err := armcompute.NewSnapshotsClient(id.SubscriptionID, cred, nil)
	if err != nil {
		return nil, nil, err
	}
	snapshots := &armSnapshotClient{
		client:        client,
		resourceGroup: id.ResourceGroupName,
		name:          id.Name,
	}
	return grantAccessAndOpen(ctx, snapshots, durationSeconds, cache, options)
}

// accessDurationSeconds converts a SAS lifetime into the int32 seconds the ARM
// API accepts. Zero selects the default lifetime.
func accessDurationSeconds(duration time.Duration) (int32, error) {
	if duration == 0 {
		duration = defaultAccessDuration
	}
	if duration < 0 {
		return 0, errors.New("snapshot: Duration must not be negative")
	}
	seconds := duration / time.Second
	if seconds <= 0 || seconds > math.MaxInt32 {
		return 0, errors.New("snapshot: Duration must be between one second and int32 maximum")
	}
	return int32(seconds), nil
}

func grantAccessAndOpen(
	ctx context.Context,
	client snapshotClient,
	durationSeconds int32,
	cache azurediskfile.Cache[string, []byte],
	options Options,
) (*io.SectionReader, func(), error) {
	// The state is read even when pre-revocation is skipped, so a missing
	// snapshot or a missing permission is reported before access is granted.
	state, err := client.DiskState(ctx)
	if err != nil {
		return nil, nil, err
	}
	if !options.SkipPreRevokeActiveSAS && state == armcompute.DiskStateActiveSAS {
		if err := client.RevokeAccess(ctx); err != nil {
			return nil, nil, err
		}
	}

	sas, err := client.GrantReadAccess(ctx, durationSeconds)
	if err != nil {
		// The grant is a long-running operation: it can fail while polling
		// after ARM has already issued the SAS. Revoking is harmless when
		// nothing was granted, so fail closed rather than leaving a SAS
		// active for the whole duration with no cleanup handle returned.
		revokeDetached(client)
		return nil, nil, err
	}
	if sas == "" {
		revokeDetached(client)
		return nil, nil, errors.New("snapshot: GrantAccess returned no SAS URI")
	}

	var sasOptions []azurediskfile.SASOption
	if options.SkipRangeChecksum {
		sasOptions = append(sasOptions, azurediskfile.WithoutRangeChecksum())
	}
	reader, err := azurediskfile.Open(ctx, azurediskfile.NewSASBlobAPI(sas, sasOptions...), cache)
	if err != nil {
		revokeDetached(client)
		return nil, nil, err
	}

	var once sync.Once
	cleanup := func() {
		if options.SkipRevokeOnCleanup {
			return
		}
		once.Do(func() { revokeDetached(client) })
	}
	return reader, cleanup, nil
}

// revokeDetached revokes on a context of its own, so the SAS is still
// withdrawn when the caller's context has already been cancelled.
func revokeDetached(client snapshotClient) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_ = client.RevokeAccess(ctx)
}

type armSnapshotClient struct {
	client        *armcompute.SnapshotsClient
	resourceGroup string
	name          string
}

func (c *armSnapshotClient) DiskState(ctx context.Context) (armcompute.DiskState, error) {
	response, err := c.client.Get(ctx, c.resourceGroup, c.name, nil)
	if err != nil {
		return "", err
	}
	if response.Properties == nil || response.Properties.DiskState == nil {
		return "", nil
	}
	return *response.Properties.DiskState, nil
}

func (c *armSnapshotClient) GrantReadAccess(ctx context.Context, durationSeconds int32) (string, error) {
	poller, err := c.client.BeginGrantAccess(ctx, c.resourceGroup, c.name, armcompute.GrantAccessData{
		Access:                   new(armcompute.AccessLevelRead),
		DurationInSeconds:        new(durationSeconds),
		GetSecureVMGuestStateSAS: new(false),
	}, nil)
	if err != nil {
		return "", err
	}
	grant, err := poller.PollUntilDone(ctx, nil)
	if err != nil {
		return "", err
	}
	if grant.AccessSAS == nil {
		return "", nil
	}
	return *grant.AccessSAS, nil
}

func (c *armSnapshotClient) RevokeAccess(ctx context.Context) error {
	poller, err := c.client.BeginRevokeAccess(ctx, c.resourceGroup, c.name, nil)
	if err != nil {
		return err
	}
	_, err = poller.PollUntilDone(ctx, nil)
	return err
}

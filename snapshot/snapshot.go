// Package snapshot grants short-lived access to an Azure managed disk snapshot
// and opens it as a random-access reader.
package snapshot

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v9"
	azurediskfile "github.com/yusuke-koyoshi/go-azure-managed-disks-file"
)

const (
	defaultAccessDuration = 20 * time.Minute
	snapshotResourceType  = "Microsoft.Compute/snapshots"
)

// Options configures snapshot SAS issuance and cleanup, and the reader.
//
// A zero Duration uses the default 20-minute grant duration. By default,
// GrantAccessAndOpenWithOptions refuses a snapshot that already has an active
// SAS, revokes the SAS during cleanup and validates range checksums. Set the
// Skip fields only when deliberately opting out of those safety behaviors.
type Options struct {
	// Duration is the SAS lifetime. Zero uses the 20-minute default.
	Duration time.Duration
	// SkipRevokeOnCleanup skips revoking the granted SAS during cleanup when
	// true. While the SAS left behind is active, later opens of the snapshot
	// return ErrSnapshotInUse unless they set SkipActiveSASCheck.
	SkipRevokeOnCleanup bool
	// SkipActiveSASCheck grants access even when the snapshot already has an
	// active SAS, replacing it. Set it only when no other reader can exist.
	SkipActiveSASCheck bool
	// SkipRangeChecksum disables per-range MD5 validation when true. Validation
	// fails closed, so this is the fallback for export endpoints that do not
	// support x-ms-range-get-content-md5. Validation only covers ranges of
	// 4 MiB or less, which the default block size satisfies.
	SkipRangeChecksum bool
	// Open configures the reader, such as MaxConcurrentBlocks and
	// PrefetchBlocks. The zero value is what Open uses. A BlockSize above
	// 4 MiB reads without range checksum validation. Format is replaced with
	// the format the grant asks for.
	Open azurediskfile.OpenOptions
	// Retry replaces the Azure SDK retry policy for the blob requests (size,
	// page ranges and range reads), not for the grant and revocation. The
	// zero value keeps the SDK defaults. This package retries 403 itself
	// while a granted SAS propagates, so leave 403 out of StatusCodes.
	Retry policy.RetryOptions
	// PerRetryPolicies run on every attempt of the blob requests and of the
	// ARM requests that read the snapshot state and grant and revoke the SAS,
	// retries included, unlike Retry, which covers only the blob requests.
	// Revocation runs on a context of its own, so a policy can see its
	// attempts after the reader and even the caller's context are done. The
	// policies serve concurrent requests, so they must be safe for concurrent
	// use.
	PerRetryPolicies []policy.Policy
	// Transport replaces the HTTP transport of both the ARM and the blob
	// clients, for example for tests or a proxy, so a fake has to serve or
	// route both hosts. The retry and per-retry policies still run in front of
	// it. Nil keeps the SDK default. The SAS is redacted only from a
	// *url.Error, so the transport must not put the request URL into other
	// errors.
	Transport policy.Transporter
}

// ErrSnapshotInUse is returned when the snapshot already has an active SAS or
// upload, which a grant would replace.
var ErrSnapshotInUse = errors.New("snapshot: the snapshot already has an active SAS or upload")

// snapshotClient is the part of armcompute.SnapshotsClient this package uses.
// It covers the SAS lifecycle, which is the security-relevant behavior here,
// and lets that lifecycle be exercised without ARM.
type snapshotClient interface {
	Properties(ctx context.Context) (snapshotProperties, error)
	GrantReadAccess(ctx context.Context, durationSeconds int32, fileFormat armcompute.FileFormat) (string, error)
	RevokeAccess(ctx context.Context) error
}

// snapshotProperties holds "" and 0 for what ARM does not return.
type snapshotProperties struct {
	UniqueID          string
	DiskState         string
	LogicalSectorSize int32
}

// logicalSectorSize decides both the grant's file format and the size that
// Describe reports, so the two cannot disagree. ARM returns no size for
// snapshots that predate it, which have 512-byte sectors.
func logicalSectorSize(properties snapshotProperties) (int, error) {
	switch properties.LogicalSectorSize {
	case 0, 512:
		return 512, nil
	case 4096:
		return 4096, nil
	default:
		return 0, fmt.Errorf("snapshot: unsupported logical sector size %d", properties.LogicalSectorSize)
	}
}

// Info is what Describe returns about a snapshot.
type Info struct {
	// UniqueID changes when the snapshot is deleted and recreated under the
	// same name.
	UniqueID string
	// LogicalSectorSize is the logical sector size of the disk in bytes: 512
	// or 4096.
	LogicalSectorSize int
}

// GrantAccessAndOpen grants a read SAS for 20 minutes and opens the snapshot
// through the root package. For 60 seconds after the grant, every request
// retries 403 while the new SAS propagates. Granting would replace any SAS
// already active on the snapshot, so a snapshot supports one reader at a
// time: when the snapshot already has an active SAS or upload, it returns
// ErrSnapshotInUse without granting. Two callers that check before either
// grants can still both grant. A replaced or revoked SAS keeps working for
// about 30 seconds. The cleanup function revokes the SAS and reports a failed
// revocation; calling it again retries a failed revocation and does nothing
// after a successful one.
//
// The credential is intentionally explicit. This function does not construct
// DefaultAzureCredential and therefore cannot silently use an unintended
// managed identity.
func GrantAccessAndOpen(
	ctx context.Context,
	cred azcore.TokenCredential,
	resourceID string,
	cache azurediskfile.Cache[string, []byte],
) (*io.SectionReader, func() error, error) {
	return GrantAccessAndOpenWithOptions(ctx, cred, resourceID, cache, Options{})
}

// GrantAccessAndOpenWithOptions is GrantAccessAndOpen with configurable SAS
// duration, revocation, reader and retry behavior.
func GrantAccessAndOpenWithOptions(
	ctx context.Context,
	cred azcore.TokenCredential,
	resourceID string,
	cache azurediskfile.Cache[string, []byte],
	options Options,
) (*io.SectionReader, func() error, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	snapshots, err := newARMSnapshotClient(cred, resourceID, options)
	if err != nil {
		return nil, nil, err
	}
	durationSeconds, err := accessDurationSeconds(options.Duration)
	if err != nil {
		return nil, nil, err
	}
	return grantAccessAndOpen(ctx, snapshots, durationSeconds, cache, options)
}

// Describe returns the uniqueId and the logical sector size of the snapshot
// with one ARM call. Unlike the resource ID, the uniqueId changes when a
// snapshot is deleted and recreated under the same name, so it can key results
// cached for the snapshot. The snapshot can be recreated between this call and
// a later grant, so a caller that keys a cache by the result can call it again
// afterwards and compare. A disk with 4096-byte sectors keeps them in the
// reader, so a partition table parser needs LogicalSectorSize. Only
// options.PerRetryPolicies and options.Transport are used; it takes Options so
// that callers can pass the value they pass to GrantAccessAndOpenWithOptions.
func Describe(ctx context.Context, cred azcore.TokenCredential, resourceID string, options Options) (Info, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	snapshots, err := newARMSnapshotClient(cred, resourceID, options)
	if err != nil {
		return Info{}, err
	}
	return describe(ctx, snapshots)
}

func describe(ctx context.Context, client snapshotClient) (Info, error) {
	properties, err := client.Properties(ctx)
	if err != nil {
		return Info{}, fmt.Errorf("snapshot: read the snapshot: %w", err)
	}
	// An empty ID would key the cache by the reusable resource ID alone.
	if properties.UniqueID == "" {
		return Info{}, errors.New("snapshot: the snapshot has no uniqueId")
	}
	sectorSize, err := logicalSectorSize(properties)
	if err != nil {
		return Info{}, err
	}
	return Info{UniqueID: properties.UniqueID, LogicalSectorSize: sectorSize}, nil
}

// UniqueID returns the uniqueId that Describe returns.
func UniqueID(ctx context.Context, cred azcore.TokenCredential, resourceID string, options Options) (string, error) {
	info, err := Describe(ctx, cred, resourceID, options)
	if err != nil {
		return "", err
	}
	return info.UniqueID, nil
}

func newARMSnapshotClient(cred azcore.TokenCredential, resourceID string, options Options) (*armSnapshotClient, error) {
	if cred == nil {
		return nil, errors.New("snapshot: nil TokenCredential")
	}
	id, err := parseSnapshotID(resourceID)
	if err != nil {
		return nil, err
	}
	clientOptions := &arm.ClientOptions{}
	clientOptions.PerRetryPolicies = options.PerRetryPolicies
	clientOptions.Transport = options.Transport
	client, err := armcompute.NewSnapshotsClient(id.SubscriptionID, cred, clientOptions)
	if err != nil {
		return nil, err
	}
	return &armSnapshotClient{
		client:        client,
		resourceGroup: id.ResourceGroupName,
		name:          id.Name,
	}, nil
}

// parseSnapshotID requires resourceID to name a snapshot. Only the name reaches
// the snapshots client, so a disk ID would otherwise hit a same-named snapshot.
func parseSnapshotID(resourceID string) (*arm.ResourceID, error) {
	id, err := arm.ParseResourceID(resourceID)
	if err != nil {
		return nil, err
	}
	if id.SubscriptionID == "" || id.ResourceGroupName == "" || id.Name == "" ||
		!strings.EqualFold(id.ResourceType.String(), snapshotResourceType) {
		return nil, errors.New("snapshot: resource ID must identify a snapshot")
	}
	return id, nil
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
) (*io.SectionReader, func() error, error) {
	properties, err := client.Properties(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("snapshot: read the snapshot: %w", err)
	}
	// Return without revoking: a revocation would withdraw the other SAS too.
	if !options.SkipActiveSASCheck {
		switch state := armcompute.DiskState(properties.DiskState); state {
		case armcompute.DiskStateActiveSAS, armcompute.DiskStateActiveSASFrozen,
			armcompute.DiskStateActiveUpload, armcompute.DiskStateReadyToUpload:
			return nil, nil, fmt.Errorf("%w (diskState %s)", ErrSnapshotInUse, state)
		}
	}
	sectorSize, err := logicalSectorSize(properties)
	if err != nil {
		return nil, nil, err
	}
	// Azure exports a snapshot with 4096-byte sectors only as VHDX, and
	// rejects the grant without the format.
	var fileFormat armcompute.FileFormat
	if sectorSize == 4096 {
		fileFormat = armcompute.FileFormatVHDX
	}

	sas, err := client.GrantReadAccess(ctx, durationSeconds, fileFormat)
	if err != nil {
		// The grant can fail while polling after ARM has already issued the SAS.
		return nil, nil, errors.Join(err, revokeDetached(client))
	}
	if sas == "" {
		return nil, nil, errors.Join(errors.New("snapshot: GrantAccess returned no SAS URI"), revokeDetached(client))
	}

	sasOptions := []azurediskfile.SASOption{
		azurediskfile.WithRetryOptions(options.Retry),
		azurediskfile.WithPerRetryPolicies(options.PerRetryPolicies...),
		azurediskfile.WithTransport(options.Transport),
	}
	if options.SkipRangeChecksum {
		sasOptions = append(sasOptions, azurediskfile.WithoutRangeChecksum())
	}
	api := &activatingBlobAPI{
		api:      newSASBlobAPI(sas, sasOptions...),
		activeBy: time.Now().Add(sasActivationTimeout),
	}
	openOptions := options.Open
	openOptions.Format = azurediskfile.FormatVHD
	if fileFormat == armcompute.FileFormatVHDX {
		openOptions.Format = azurediskfile.FormatVHDX
	}
	reader, err := azurediskfile.OpenWithOptions(ctx, api, cache, openOptions)
	if err != nil {
		return nil, nil, errors.Join(err, revokeDetached(client))
	}

	var (
		mu      sync.Mutex
		revoked bool
	)
	cleanup := func() error {
		if options.SkipRevokeOnCleanup {
			return nil
		}
		mu.Lock()
		defer mu.Unlock()
		if revoked {
			return nil
		}
		if err := revokeDetached(client); err != nil {
			return err
		}
		revoked = true
		return nil
	}
	return reader, cleanup, nil
}

// revokeDetached revokes on a context of its own, so the SAS is still
// withdrawn when the caller's context has already been cancelled.
func revokeDetached(client snapshotClient) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	err := client.RevokeAccess(ctx)
	// A deleted snapshot has no SAS left to revoke.
	var responseErr *azcore.ResponseError
	if err == nil || errors.As(err, &responseErr) && responseErr.StatusCode == http.StatusNotFound {
		return nil
	}
	return fmt.Errorf("snapshot: revoke SAS: %w", err)
}

type armSnapshotClient struct {
	client        *armcompute.SnapshotsClient
	resourceGroup string
	name          string
}

func (c *armSnapshotClient) Properties(ctx context.Context) (snapshotProperties, error) {
	resp, err := c.client.Get(ctx, c.resourceGroup, c.name, nil)
	if err != nil {
		return snapshotProperties{}, err
	}
	var properties snapshotProperties
	if p := resp.Properties; p != nil {
		if p.UniqueID != nil {
			properties.UniqueID = *p.UniqueID
		}
		if p.DiskState != nil {
			properties.DiskState = string(*p.DiskState)
		}
		if p.CreationData != nil && p.CreationData.LogicalSectorSize != nil {
			properties.LogicalSectorSize = *p.CreationData.LogicalSectorSize
		}
	}
	return properties, nil
}

func (c *armSnapshotClient) GrantReadAccess(ctx context.Context, durationSeconds int32, fileFormat armcompute.FileFormat) (string, error) {
	data := armcompute.GrantAccessData{
		Access:                   new(armcompute.AccessLevelRead),
		DurationInSeconds:        new(durationSeconds),
		GetSecureVMGuestStateSAS: new(false),
	}
	if fileFormat != "" {
		data.FileFormat = &fileFormat
	}
	poller, err := c.client.BeginGrantAccess(ctx, c.resourceGroup, c.name, data, nil)
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

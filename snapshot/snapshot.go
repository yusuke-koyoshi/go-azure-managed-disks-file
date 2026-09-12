// Package snapshot grants short-lived access to an Azure managed disk snapshot
// and opens it as a random-access reader.
package snapshot

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute"
	azurediskfile "github.com/yusuke-koyoshi/go-azure-managed-disks-file"
)

const accessDurationSeconds int32 = 20 * 60

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
	client, err := armcompute.NewSnapshotsClient(id.SubscriptionID, cred, nil)
	if err != nil {
		return nil, nil, err
	}

	state, err := client.Get(ctx, id.ResourceGroupName, id.Name, nil)
	if err != nil {
		return nil, nil, err
	}
	if state.Properties != nil && state.Properties.DiskState != nil &&
		*state.Properties.DiskState == armcompute.DiskStateActiveSAS {
		if err := revoke(ctx, client, id.ResourceGroupName, id.Name); err != nil {
			return nil, nil, err
		}
	}

	grantPoller, err := client.BeginGrantAccess(ctx, id.ResourceGroupName, id.Name, armcompute.GrantAccessData{
		Access:                   toAccessLevel(armcompute.AccessLevelRead),
		DurationInSeconds:        new(accessDurationSeconds),
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

//go:fix inline
func toAccessLevel(value armcompute.AccessLevel) *armcompute.AccessLevel { return new(value) }

//go:fix inline
func toInt32(value int32) *int32 { return new(value) }

//go:fix inline
func toBool(value bool) *bool { return new(value) }

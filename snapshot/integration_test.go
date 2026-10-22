//go:build integration

package snapshot

import (
	"errors"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v9"
)

// noCache makes every read reach Azure, so a revoked SAS shows up as 403.
type noCache struct{}

func (noCache) Add(string, []byte) bool   { return false }
func (noCache) Get(string) ([]byte, bool) { return nil, false }

func integrationTarget(t *testing.T) (azcore.TokenCredential, string) {
	t.Helper()
	return integrationTargetFrom(t, "AZURE_SNAPSHOT_ID")
}

func integrationTargetFrom(t *testing.T, variable string) (azcore.TokenCredential, string) {
	t.Helper()
	resourceID := os.Getenv(variable)
	if resourceID == "" {
		t.Skip(variable + " is not set")
	}
	cred, err := azidentity.NewAzureCLICredential(nil)
	if err != nil {
		t.Fatal(err)
	}
	return cred, resourceID
}

func isStatus(err error, want int) bool {
	var responseErr *azcore.ResponseError
	return errors.As(err, &responseErr) && responseErr.StatusCode == want
}

// A revoked or replaced SAS keeps working until the change propagates.
func waitForForbidden(t *testing.T, reader *io.SectionReader) {
	t.Helper()
	deadline := time.Now().Add(sasActivationTimeout)
	for {
		_, err := reader.ReadAt(make([]byte, 8), 512)
		if isStatus(err, http.StatusForbidden) {
			return
		}
		if err != nil {
			t.Fatalf("read error = %v, want HTTP 403", err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("SAS still readable %s after it was revoked", sasActivationTimeout)
		}
		time.Sleep(sasActivationInterval)
	}
}

func TestIntegrationGrantReadRevoke(t *testing.T) {
	cred, resourceID := integrationTarget(t)
	reader, cleanup, err := GrantAccessAndOpen(t.Context(), cred, resourceID, noCache{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cleanup() })

	gpt := make([]byte, 8)
	if _, err := reader.ReadAt(gpt, 512); err != nil {
		t.Fatal(err)
	}
	if string(gpt) != "EFI PART" {
		t.Errorf("LBA 1 = %q, want EFI PART", gpt)
	}
	footer := make([]byte, 8)
	if _, err := reader.ReadAt(footer, reader.Size()-512); err != nil {
		t.Fatal(err)
	}
	if string(footer) != "conectix" {
		t.Errorf("VHD footer = %q, want conectix", footer)
	}

	if err := cleanup(); err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	waitForForbidden(t, reader)
}

func TestIntegrationGrantReplacesActiveSAS(t *testing.T) {
	cred, resourceID := integrationTarget(t)
	first, cleanupFirst, err := GrantAccessAndOpen(t.Context(), cred, resourceID, noCache{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cleanupFirst() })

	_, cleanupSecond, err := GrantAccessAndOpenWithOptions(t.Context(), cred, resourceID, noCache{},
		Options{SkipActiveSASCheck: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cleanupSecond() })

	waitForForbidden(t, first)
}

func TestIntegrationRefusesSnapshotInUse(t *testing.T) {
	cred, resourceID := integrationTarget(t)
	first, cleanup, err := GrantAccessAndOpen(t.Context(), cred, resourceID, noCache{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cleanup() })

	if _, _, err := GrantAccessAndOpen(t.Context(), cred, resourceID, noCache{}); !errors.Is(err, ErrSnapshotInUse) {
		t.Fatalf("second open error = %v, want ErrSnapshotInUse", err)
	}
	if _, err := first.ReadAt(make([]byte, 8), 512); err != nil {
		t.Fatalf("first reader after the refused open: %v", err)
	}
}

func TestIntegrationRevokeMissingSnapshot(t *testing.T) {
	cred, resourceID := integrationTarget(t)
	id, err := parseSnapshotID(resourceID)
	if err != nil {
		t.Fatal(err)
	}
	client, err := armcompute.NewSnapshotsClient(id.SubscriptionID, cred, nil)
	if err != nil {
		t.Fatal(err)
	}
	missing := &armSnapshotClient{
		client:        client,
		resourceGroup: id.ResourceGroupName,
		name:          id.Name + "-missing-integration-test",
	}
	if err := missing.RevokeAccess(t.Context()); !isStatus(err, http.StatusNotFound) {
		t.Fatalf("ARM revoke error = %v, want HTTP 404", err)
	}
	if err := revokeDetached(missing); err != nil {
		t.Fatalf("revoke of a missing snapshot: %v", err)
	}
}

func TestIntegrationStateFollowsSAS(t *testing.T) {
	cred, resourceID := integrationTarget(t)
	client, err := newARMSnapshotClient(cred, resourceID, Options{})
	if err != nil {
		t.Fatal(err)
	}
	_, cleanup, err := GrantAccessAndOpen(t.Context(), cred, resourceID, noCache{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cleanup() })

	if p, err := client.Properties(t.Context()); err != nil || p.DiskState != string(armcompute.DiskStateActiveSAS) {
		t.Fatalf("state while granted = %q, %v; want ActiveSAS", p.DiskState, err)
	}
	if err := cleanup(); err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	if p, err := client.Properties(t.Context()); err != nil || p.DiskState != string(armcompute.DiskStateUnattached) {
		t.Fatalf("state after revoke = %q, %v; want Unattached", p.DiskState, err)
	}
}

// A snapshot with 4096-byte sectors is exported as VHDX.
func TestIntegrationVHDX(t *testing.T) {
	cred, resourceID := integrationTargetFrom(t, "AZURE_VHDX_SNAPSHOT_ID")
	reader, cleanup, err := GrantAccessAndOpen(t.Context(), cred, resourceID, noCache{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cleanup() })

	// The GPT header is in LBA 1 and its backup in the last LBA, so both also
	// check the virtual disk size.
	for _, off := range []int64{4096, reader.Size() - 4096} {
		header := make([]byte, 8)
		if _, err := reader.ReadAt(header, off); err != nil {
			t.Fatal(err)
		}
		if string(header) != "EFI PART" {
			t.Errorf("offset %d = %q, want EFI PART", off, header)
		}
	}

	if err := cleanup(); err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	waitForForbidden(t, reader)
}

func TestIntegrationUniqueID(t *testing.T) {
	cred, resourceID := integrationTarget(t)
	id, err := UniqueID(t.Context(), cred, resourceID, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if len(id) != len("00000000-0000-0000-0000-000000000000") {
		t.Fatalf("uniqueId = %q, want a GUID", id)
	}
}

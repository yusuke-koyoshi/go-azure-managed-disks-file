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
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute"
)

// noCache makes every read reach Azure, so a revoked SAS shows up as 403.
type noCache struct{}

func (noCache) Add(string, []byte) bool   { return false }
func (noCache) Get(string) ([]byte, bool) { return nil, false }

func integrationTarget(t *testing.T) (azcore.TokenCredential, string) {
	t.Helper()
	resourceID := os.Getenv("AZURE_SNAPSHOT_ID")
	if resourceID == "" {
		t.Skip("AZURE_SNAPSHOT_ID is not set")
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

	_, cleanupSecond, err := GrantAccessAndOpen(t.Context(), cred, resourceID, noCache{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cleanupSecond() })

	waitForForbidden(t, first)
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

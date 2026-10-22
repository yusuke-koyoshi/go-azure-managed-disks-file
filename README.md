# go-azure-managed-disks-file

Expose Azure managed disk snapshots as random-access `io.SectionReader`
instances.

The library reads only the required ranges through an
[export SAS](https://learn.microsoft.com/rest/api/compute/snapshots/grant-access)
without downloading the disk image locally.

This is the Azure counterpart to
[masahiro331/go-ebs-file](https://github.com/masahiro331/go-ebs-file). Both
read a snapshot in blocks through a cache, but their APIs differ.

## Example

```go
package main

import (
    "context"
    "encoding/hex"
    "fmt"
    "log"
    "os"

    lru "github.com/hashicorp/golang-lru/v2"

    azurediskfile "github.com/yusuke-koyoshi/go-azure-managed-disks-file"
)

func main() {
    // os.Args[1] is the snapshot export SAS URL.
    api := azurediskfile.NewSASBlobAPI(os.Args[1])

    cache, err := lru.New[string, []byte](512)
    if err != nil {
        log.Fatal(err)
    }

    sr, err := azurediskfile.Open(context.Background(), api, cache)
    if err != nil {
        log.Fatal(err)
    }

    buf := make([]byte, 512)
    if _, err := sr.Read(buf); err != nil {
        log.Fatal(err)
    }
    fmt.Println(hex.Dump(buf))
}
```

Use the `snapshot` subpackage when the resource ID and SAS lifecycle should be
managed by the library.

```go
sr, cleanup, err := snapshot.GrantAccessAndOpen(ctx, cred, resourceID, cache)
if err != nil {
    log.Fatal(err)
}
defer func() {
    // A failed revocation leaves the SAS active until it expires.
    if err := cleanup(); err != nil {
        log.Printf("revoke snapshot SAS: %v", err)
    }
}()
```

Use `snapshot.GrantAccessAndOpenWithOptions` when the SAS duration,
revocation, reader or retry behavior needs to be configured (with `time` and
`azurediskfile` imported):

```go
sr, cleanup, err := snapshot.GrantAccessAndOpenWithOptions(ctx, cred, resourceID, cache, snapshot.Options{
    Duration: 45 * time.Minute,
    Open:     azurediskfile.OpenOptions{MaxConcurrentBlocks: 8},
})
```

`Open` takes the same `OpenOptions` as `OpenWithOptions`; a `BlockSize` above
4 MiB reads without range checksum validation. `Retry` replaces the Azure SDK
retry policy for the blob requests, not for the grant and revocation. Leave
403 out of `Retry.StatusCodes`: the package already retries it while a new
SAS propagates. `PerRetryPolicies` run on every attempt of the blob and ARM
requests, including those the SDK retries, for example to count throttled
responses that never reach the caller. `Transport` replaces the HTTP transport
of both the ARM and the blob clients, for example for tests or a proxy; the
retry and per-retry policies still run in front of it.

`snapshot.UniqueID` returns the snapshot's `uniqueId`. A resource ID is reused
when a snapshot is deleted and recreated under the same name, so key results
cached for a snapshot by its `uniqueId` rather than by the resource ID alone.

`Duration: 0` uses a 20-minute SAS. Both `GrantAccessAndOpen` and
`GrantAccessAndOpenWithOptions` refuse a snapshot that already has an active
SAS, revoke the SAS during cleanup and validate range checksums by default.
Set `SkipActiveSASCheck`, `SkipRevokeOnCleanup` or `SkipRangeChecksum` only to
explicitly opt out. While a SAS left behind by `SkipRevokeOnCleanup` is
active, later opens of the snapshot return `ErrSnapshotInUse`.

A snapshot supports one reader at a time. Granting access replaces any SAS
already active on the snapshot, and the replaced SAS stops working. So by
default the `snapshot` package reads the snapshot's `diskState` right before
granting, and returns `ErrSnapshotInUse` without granting or revoking when the
snapshot already has an active SAS or upload. Two readers that check before
either grants can still both grant, and the later grant cuts off the earlier
reader, so give each reader its own snapshot. A SAS left behind by a crashed
process keeps the snapshot refused until it is revoked
(`az snapshot revoke-access`) or replaced with `SkipActiveSASCheck`. The
package reads the snapshot before granting, for this check and for its logical
sector size, so the credential needs `Microsoft.Compute/snapshots/read` in
addition to `Microsoft.Compute/snapshots/beginGetAccess/action` and
`Microsoft.Compute/snapshots/endGetAccess/action`.

The snapshot's export settings must allow reading with the SAS alone. A
`networkAccessPolicy` of `DenyAll` blocks the export, and `AllowPrivate`
limits it to clients in the subnet of the disk access resource's private
endpoint. A `publicNetworkAccess` of `Disabled` blocks reads over the internet
even under `AllowAll`. A `dataAccessAuthMode` of `AzureActiveDirectory` also
requires a Microsoft Entra ID token on the download, which the package does
not send, so such a snapshot cannot be read.

SAS changes take up to about 30 seconds to reach Azure Storage, and they reach
its front ends one at a time, so a new SAS can be rejected with 403 even after
an earlier request succeeded. The `snapshot` package therefore retries 403 on
every request for 60 seconds after granting. A revoked or replaced SAS
likewise keeps working for about 30 seconds.

When a cache is shared between readers, the cache key includes the blob
identifier. Implementations that do not expose `BlobIdentifier` receive a
unique per-reader identifier, so those readers deliberately do not share
cached blocks. Provide a stable identifier when cross-reader cache reuse is
desired.

`OpenWithOptions` can set `MaxConcurrentBlocks` to bound in-flight range
requests (the default is unlimited) and `PrefetchBlocks` to enable
cancellable read-ahead. Read-ahead only follows a read that continues where
the previous one ended, so random access issues no extra requests.

With the default transport, a request that gets no response, such as on a
refused connection, fails with a `*url.Error` that carries the request URL.
`NewSASBlobAPI` replaces the values of its query, the SAS signature included,
with `REDACTED` before the error reaches the SDK retry log,
`PerRetryPolicies` or the caller. Only `*url.Error` is redacted, so a
replacement `Transport` must not put the request URL into other errors.

### Range checksums

`NewSASBlobAPI` asks for `x-ms-range-get-content-md5` and validates the
returned `Content-MD5` on every range of 4 MiB or less, which covers the
default 1 MiB block size. Validation fails closed: a range response without
`Content-MD5` is an error rather than unverified data. Larger ranges are not
eligible for the header and are read without validation.

Pass `WithoutRangeChecksum` when the endpoint does not support the header.
The equivalents are `snapshot.Options.SkipRangeChecksum` and the
`-skip-checksum` flag of the verification CLI.

```go
api := azurediskfile.NewSASBlobAPI(sasURL, azurediskfile.WithoutRangeChecksum())
```

### VHDX

Azure exports a snapshot with a 4096-byte logical sector size, such as one of
a Premium SSD v2 or an Ultra Disk, only as VHDX, and rejects the grant unless
the request asks for it. The `snapshot` package asks for VHDX for such a
snapshot and reads the virtual disk inside it, whose size is the reader's
size. A VHD blob is read as is, so its size includes the 512-byte footer.
Differencing disks and files with a log to replay are rejected. The disk keeps
its 4096-byte sectors, so its GPT header is at offset 4096 rather than 512.

`Open` reads a blob that starts with the VHDX file identifier as VHDX. Set
`OpenOptions.Format` when the format is known, as the `snapshot` package does:
the start of a VHD blob is the disk's first sector, which whoever controls the
disk can write.

The verification CLI accepts its SAS URL from `AZURE_SAS_URL` (preferred, so
the URL is not placed in the command line), while the legacy `-url` flag and
positional argument remain supported:

```sh
AZURE_SAS_URL='https://...' go run ./cmd/azure-disk-hexdump -offset 0 -length 512
```

## Integration tests

The `snapshot` package has tests that grant and revoke access on a real
snapshot. They run only with the `integration` build tag and an
`AZURE_SNAPSHOT_ID`, and they authenticate with the Azure CLI login. The
snapshot must hold a GPT disk image, and no one else may be reading it.

```sh
AZURE_SNAPSHOT_ID=/subscriptions/.../providers/Microsoft.Compute/snapshots/NAME \
  go test -tags integration ./snapshot
```

`AZURE_VHDX_SNAPSHOT_ID` additionally runs the VHDX test against a GPT
snapshot with a 4096-byte logical sector size.

## Packages

| Package | Role | Dependencies |
|---|---|---|
| `azurediskfile` (root package) | SAS URL → `*io.SectionReader` | `azblob` only |
| `snapshot` (subpackage) | Resource ID → `GrantAccess` → SAS → `Open` | Adds `armcompute` |

The two layers keep the core dependency surface small. Consumers that already
have a SAS URL only need the root package.

## License

MIT

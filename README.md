# go-azure-managed-disks-file

Expose Azure managed disk snapshots as random-access `io.SectionReader`
instances.

The library reads only the required ranges through an
[export SAS](https://learn.microsoft.com/rest/api/compute/snapshots/grant-access)
without downloading the disk image locally.

This is the Azure counterpart to
[masahiro331/go-ebs-file](https://github.com/masahiro331/go-ebs-file), and
follows the same API shape.

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

Use `snapshot.GrantAccessAndOpenWithOptions` when the SAS duration or
revocation behavior needs to be configured (with `time` imported):

```go
sr, cleanup, err := snapshot.GrantAccessAndOpenWithOptions(ctx, cred, resourceID, cache, snapshot.Options{
    Duration: 45 * time.Minute,
})
```

`Duration: 0` uses a 20-minute SAS. Both `GrantAccessAndOpen` and
`GrantAccessAndOpenWithOptions` revoke the SAS during cleanup and validate
range checksums by default. Set `SkipRevokeOnCleanup` or `SkipRangeChecksum`
only to explicitly opt out.

A snapshot supports one reader at a time. Granting access replaces any SAS
already active on the snapshot, and the replaced SAS stops working, so a
concurrent reader of the same snapshot fails with 403. Give each reader its own
snapshot.

SAS changes take up to about 30 seconds to reach Azure Storage. A new SAS can
be rejected with 403 until then, so the `snapshot` package retries 403 for up
to 60 seconds while opening. A revoked or replaced SAS can likewise keep
working for that long.

When a cache is shared between readers, the cache key includes the blob
identifier. Implementations that do not expose `BlobIdentifier` receive a
unique per-reader identifier, so those readers deliberately do not share
cached blocks. Provide a stable identifier when cross-reader cache reuse is
desired.

`OpenWithOptions` can set `MaxConcurrentBlocks` to bound in-flight range
requests (the default is unlimited) and `PrefetchBlocks` to enable
cancellable read-ahead. Read-ahead only follows a read that continues where
the previous one ended, so random access issues no extra requests.

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

The verification CLI accepts its SAS URL from `AZURE_SAS_URL` (preferred, so
the URL is not placed in the command line), while the legacy `-url` flag and
positional argument remain supported:

```sh
AZURE_SAS_URL='https://...' go run ./cmd/azure-disk-hexdump -offset 0 -length 512
```

## Packages

| Package | Role | Dependencies |
|---|---|---|
| `azurediskfile` (root package) | SAS URL → `*io.SectionReader` | `azblob` only |
| `snapshot` (subpackage) | Resource ID → `GrantAccess` → SAS → `Open` | Adds `armcompute` |

The two layers keep the core dependency surface small. Consumers that already
have a SAS URL only need the root package.

## License

MIT

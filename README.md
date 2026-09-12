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
defer cleanup() // RevokeAccess
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

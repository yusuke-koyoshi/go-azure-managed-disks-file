package main

import (
	"context"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"

	azurediskfile "github.com/yusuke-koyoshi/go-azure-managed-disks-file"
)

func main() {
	urlValue := flag.String("url", "", "snapshot export SAS URL (prefer AZURE_SAS_URL)")
	offset := flag.Int64("offset", 0, "byte offset")
	length := flag.Int("length", 512, "number of bytes")
	flag.Parse()
	if *urlValue == "" {
		*urlValue = os.Getenv("AZURE_SAS_URL")
	}
	if *urlValue == "" && flag.NArg() > 0 {
		*urlValue = flag.Arg(0)
	}
	if *urlValue == "" {
		fmt.Fprintln(os.Stderr, "usage: AZURE_SAS_URL='...' azure-disk-hexdump [-offset N] [-length N]")
		fmt.Fprintln(os.Stderr, "       azure-disk-hexdump -url SAS_URL [-offset N] [-length N] (legacy)")
		os.Exit(2)
	}
	if *offset < 0 || *length < 0 {
		log.Fatal("offset and length must be non-negative")
	}

	api := azurediskfile.NewSASBlobAPI(*urlValue)
	reader, err := azurediskfile.Open(context.Background(), api, nil)
	if err != nil {
		log.Fatal(err)
	}
	buf := make([]byte, *length)
	n, err := reader.ReadAt(buf, *offset)
	if err != nil && n == 0 {
		log.Fatal(err)
	}
	fmt.Printf("offset=%s bytes=%d\n%s", strconv.FormatInt(*offset, 10), n, hex.Dump(buf[:n]))
	if err != nil {
		fmt.Fprintf(os.Stderr, "read: %v\n", err)
	}
}

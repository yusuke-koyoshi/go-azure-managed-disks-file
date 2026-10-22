package azurediskfile

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
)

// testdata/premium-ssd-v2-vhdx-head.bin.gz holds the first 3 MiB + 4 KiB of
// the VHDX export of an 8 GiB Premium SSD v2 snapshot: the headers, log,
// metadata and the first 512 BAT entries. Its 32 payload blocks of 256 MiB lie
// in order from 6 MiB.
const (
	azureVHDXFileSize   = 8<<30 + 6<<20
	azureVHDXDiskSize   = 8 << 30
	azureVHDXDataOffset = 6 << 20
	azureVHDXBlockSize  = 256 << 20
	azureVHDXBAT        = 3 << 20
	azureVHDXMetadata   = 2 << 20
)

func loadAzureVHDX(t *testing.T) []byte {
	t.Helper()
	file, err := os.Open("testdata/premium-ssd-v2-vhdx-head.bin.gz")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	reader, err := gzip.NewReader(file)
	if err != nil {
		t.Fatal(err)
	}
	head, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	return head
}

// vhdxPattern differs between nearby offsets and between blocks, so a read
// from the wrong place shows up.
func vhdxPattern(off int64) byte {
	return byte(off ^ off>>8 ^ off>>16 ^ off>>24 ^ off>>32)
}

// vhdxBlob serves head, then zeros, then vhdxPattern from the payload on.
type vhdxBlob struct {
	head []byte
	// size is azureVHDXFileSize when zero.
	size   int64
	ranges []Range
	mu     sync.Mutex
	reads  []Range
}

func (b *vhdxBlob) BlobIdentifier() string { return "blob" }
func (b *vhdxBlob) Size(context.Context) (int64, error) {
	if b.size == 0 {
		return azureVHDXFileSize, nil
	}
	return b.size, nil
}
func (b *vhdxBlob) PageRanges(context.Context) ([]Range, error) { return b.ranges, nil }
func (b *vhdxBlob) ReadRange(ctx context.Context, off, count int64) ([]byte, error) {
	size, _ := b.Size(ctx)
	if off < 0 || count < 0 || off+count > size {
		return nil, fmt.Errorf("range %d+%d outside the blob", off, count)
	}
	b.mu.Lock()
	b.reads = append(b.reads, Range{Start: off, End: off + count - 1})
	b.mu.Unlock()
	data := make([]byte, count)
	for i := range data {
		position := off + int64(i)
		switch {
		case position < int64(len(b.head)):
			data[i] = b.head[position]
		case position >= azureVHDXDataOffset:
			data[i] = vhdxPattern(position)
		}
	}
	return data, nil
}

func (b *vhdxBlob) takeReads() []Range {
	b.mu.Lock()
	defer b.mu.Unlock()
	reads := b.reads
	b.reads = nil
	return reads
}

func setVHDXBATEntry(head []byte, block int, entry uint64) {
	binary.LittleEndian.PutUint64(head[azureVHDXBAT+8*(block+block/128):], entry)
}

func fixVHDXChecksum(structure []byte) {
	clear(structure[4:8])
	binary.LittleEndian.PutUint32(structure[4:], crc32.Checksum(structure, crc32.MakeTable(crc32.Castagnoli)))
}

func readVHDXAt(t *testing.T, reader *io.SectionReader, off, count int64) []byte {
	t.Helper()
	data := make([]byte, count)
	if _, err := reader.ReadAt(data, off); err != nil {
		t.Fatalf("read %d+%d: %v", off, count, err)
	}
	return data
}

func wantVHDXPattern(t *testing.T, got []byte, fileOffset int64) {
	t.Helper()
	for i, b := range got {
		if want := vhdxPattern(fileOffset + int64(i)); b != want {
			t.Fatalf("byte %d = %#x, want %#x from blob offset %d", i, b, want, fileOffset+int64(i))
		}
	}
}

func TestOpenVHDXReadsTheVirtualDisk(t *testing.T) {
	blob := &vhdxBlob{head: loadAzureVHDX(t)}
	reader, err := Open(t.Context(), blob, nil)
	if err != nil {
		t.Fatal(err)
	}
	if reader.Size() != azureVHDXDiskSize {
		t.Fatalf("size = %d, want %d", reader.Size(), azureVHDXDiskSize)
	}
	for _, off := range []int64{0, 4096, azureVHDXBlockSize - 4, azureVHDXDiskSize - 8} {
		wantVHDXPattern(t, readVHDXAt(t, reader, off, 8), azureVHDXDataOffset+off)
	}
}

func TestOpenVHDXFollowsTheBAT(t *testing.T) {
	head := loadAzureVHDX(t)
	// Swap the first two blocks and mark the third as zeros.
	setVHDXBATEntry(head, 0, 262<<20|vhdxBlockFullyPresent)
	setVHDXBATEntry(head, 1, 6<<20|vhdxBlockFullyPresent)
	setVHDXBATEntry(head, 2, vhdxBlockZero)
	blob := &vhdxBlob{head: head}
	reader, err := Open(t.Context(), blob, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Crosses from block 0 into block 1.
	got := readVHDXAt(t, reader, azureVHDXBlockSize-4, 8)
	wantVHDXPattern(t, got[:4], 262<<20+azureVHDXBlockSize-4)
	wantVHDXPattern(t, got[4:], 6<<20)

	blob.takeReads()
	if got := readVHDXAt(t, reader, 2*azureVHDXBlockSize+4096, 8); !bytes.Equal(got, make([]byte, 8)) {
		t.Errorf("zero block = %x, want zeros", got)
	}
	if reads := blob.takeReads(); len(reads) != 0 {
		t.Errorf("zero block read %v from the blob, want nothing", reads)
	}
}

func TestOpenVHDXSkipsSectorBitmapEntries(t *testing.T) {
	head := loadAzureVHDX(t)
	// At 40 GiB, payload block 128 follows the first sector bitmap entry. The
	// BAT of the export already lays the blocks out that way.
	const diskSize = 40 << 30
	binary.LittleEndian.PutUint64(head[azureVHDXMetadata+64<<10+8:], diskSize)
	reader, err := Open(t.Context(), &vhdxBlob{head: head, size: azureVHDXDataOffset + diskSize}, nil)
	if err != nil {
		t.Fatal(err)
	}
	off := int64(128 * azureVHDXBlockSize)
	wantVHDXPattern(t, readVHDXAt(t, reader, off, 8), azureVHDXDataOffset+off)
}

func TestOpenVHDXMapsPageRanges(t *testing.T) {
	// Only the first page of block 1 is allocated in the blob.
	blob := &vhdxBlob{
		head:   loadAzureVHDX(t),
		ranges: []Range{{Start: 262 << 20, End: 262<<20 + 4095}},
	}
	reader, err := OpenWithOptions(t.Context(), blob, nil, OpenOptions{BlockSize: 4096})
	if err != nil {
		t.Fatal(err)
	}
	blob.takeReads()

	wantVHDXPattern(t, readVHDXAt(t, reader, azureVHDXBlockSize, 8), 262<<20)
	if got := readVHDXAt(t, reader, azureVHDXBlockSize+4096, 8); !bytes.Equal(got, make([]byte, 8)) {
		t.Errorf("unallocated page = %x, want zeros", got)
	}
	if got := readVHDXAt(t, reader, 0, 8); !bytes.Equal(got, make([]byte, 8)) {
		t.Errorf("unallocated block 0 = %x, want zeros", got)
	}
	want := []Range{{Start: 262 << 20, End: 262<<20 + 4095}}
	if reads := blob.takeReads(); fmt.Sprint(reads) != fmt.Sprint(want) {
		t.Errorf("blob reads = %v, want only %v", reads, want)
	}
}

func TestOpenVHDXKeysTheCacheApartFromTheBlob(t *testing.T) {
	cache := &testCache{values: map[string][]byte{}}
	blob := &vhdxBlob{head: loadAzureVHDX(t)}
	raw, err := OpenWithOptions(t.Context(), blob, cache, OpenOptions{Format: FormatRaw})
	if err != nil {
		t.Fatal(err)
	}
	if got := readVHDXAt(t, raw, 0, 8); string(got) != "vhdxfile" {
		t.Fatalf("raw blob starts with %q, want vhdxfile", got)
	}
	vhdx, err := Open(t.Context(), blob, cache)
	if err != nil {
		t.Fatal(err)
	}
	wantVHDXPattern(t, readVHDXAt(t, vhdx, 0, 8), azureVHDXDataOffset)
}

func TestOpenFormatRawReadsAVHDXBlobAsIs(t *testing.T) {
	blob := &vhdxBlob{head: loadAzureVHDX(t)}
	reader, err := OpenWithOptions(t.Context(), blob, nil, OpenOptions{Format: FormatRaw})
	if err != nil {
		t.Fatal(err)
	}
	if reader.Size() != azureVHDXFileSize {
		t.Errorf("size = %d, want the blob size %d", reader.Size(), azureVHDXFileSize)
	}
	if reads := blob.takeReads(); len(reads) != 0 {
		t.Errorf("Open read %v, want nothing", reads)
	}
}

func TestOpenFormatAutoReadsOtherBlobsAsIs(t *testing.T) {
	blob := &testBlob{id: "raw", data: bytes.Repeat([]byte("raw disk"), vhdxHeadSize/8)}
	reader, err := Open(t.Context(), blob, nil)
	if err != nil {
		t.Fatal(err)
	}
	if reader.Size() != vhdxHeadSize {
		t.Errorf("size = %d, want %d", reader.Size(), vhdxHeadSize)
	}
	// Only the file identifier is read.
	if n := blob.requests.Load(); n != 1 {
		t.Errorf("Open made %d requests, want 1", n)
	}
	if got := readVHDXAt(t, reader, 0, 8); string(got) != "raw disk" {
		t.Errorf("read %q, want raw disk", got)
	}
}

func TestOpenFormatVHDXRejectsOtherBlobs(t *testing.T) {
	blob := &testBlob{id: "raw", data: make([]byte, vhdxHeadSize)}
	_, err := OpenWithOptions(t.Context(), blob, nil, OpenOptions{Format: FormatVHDX})
	if err == nil || !strings.Contains(err.Error(), "no file identifier") {
		t.Fatalf("error = %v, want no file identifier", err)
	}
}

func TestOpenVHDXUsesTheOtherValidHeader(t *testing.T) {
	head := loadAzureVHDX(t)
	// The header at 128 KiB has the higher sequence number.
	head[128<<10+100] ^= 1
	if _, err := Open(t.Context(), &vhdxBlob{head: head}, nil); err != nil {
		t.Fatal(err)
	}
}

func TestOpenVHDXRejects(t *testing.T) {
	headers := func(head []byte, change func(header []byte)) {
		for _, off := range vhdxHeaderOffsets {
			header := head[off : off+vhdxHeaderSize]
			change(header)
			fixVHDXChecksum(header)
		}
	}
	tests := []struct {
		name   string
		change func(head []byte)
		want   string
	}{
		{"corrupt headers", func(head []byte) {
			head[64<<10+100] ^= 1
			head[128<<10+100] ^= 1
		}, "no valid header"},
		{"a log to replay", func(head []byte) {
			headers(head, func(header []byte) { header[48] = 1 })
		}, "log to replay"},
		{"another version", func(head []byte) {
			headers(head, func(header []byte) { header[66] = 2 })
		}, "unsupported version"},
		{"an unknown required region", func(head []byte) {
			for _, off := range vhdxRegionTableOffsets {
				table := head[off : off+vhdxRegionTableSize]
				binary.LittleEndian.PutUint32(table[8:], 3)
				entry := table[16+32*2 : 48+32*2]
				copy(entry, "unknown region..")
				binary.LittleEndian.PutUint64(entry[16:], 4<<20)
				binary.LittleEndian.PutUint32(entry[24:], 1<<20)
				binary.LittleEndian.PutUint32(entry[28:], 1)
				fixVHDXChecksum(table)
			}
		}, "unknown required region"},
		{"an unknown required metadata item", func(head []byte) {
			// The fifth entry is the virtual disk ID, which is required.
			head[azureVHDXMetadata+32+32*4] ^= 1
		}, "unknown required metadata item"},
		{"a differencing disk", func(head []byte) {
			head[azureVHDXMetadata+64<<10+4] |= vhdxHasParent
		}, "differencing"},
		{"a partially present block", func(head []byte) {
			setVHDXBATEntry(head, 0, 6<<20|7)
		}, "unsupported state 7"},
		{"a block past the blob", func(head []byte) {
			setVHDXBATEntry(head, 31, 7943<<20|vhdxBlockFullyPresent)
		}, "outside"},
		{"a block in the header section", func(head []byte) {
			setVHDXBATEntry(head, 0, vhdxBlockFullyPresent)
		}, "outside"},
		{"a large metadata region", func(head []byte) {
			for _, off := range vhdxRegionTableOffsets {
				table := head[off : off+vhdxRegionTableSize]
				// The second entry is the metadata region.
				binary.LittleEndian.PutUint32(table[16+32+24:], 2<<20)
				fixVHDXChecksum(table)
			}
		}, "metadata region"},
		{"too many blocks", func(head []byte) {
			binary.LittleEndian.PutUint32(head[azureVHDXMetadata+64<<10:], 1<<20)
			binary.LittleEndian.PutUint64(head[azureVHDXMetadata+64<<10+8:], 8<<40)
		}, "payload blocks"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			head := loadAzureVHDX(t)
			test.change(head)
			_, err := Open(t.Context(), &vhdxBlob{head: head}, nil)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want one containing %q", err, test.want)
			}
		})
	}
}

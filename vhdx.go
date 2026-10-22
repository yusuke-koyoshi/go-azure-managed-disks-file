package azurediskfile

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"sort"
	"strings"
)

// The layout follows [MS-VHDX].

const (
	vhdxHeaderSize      = 4 << 10
	vhdxRegionTableSize = 64 << 10
	// The file identifier, both headers and both region tables.
	vhdxHeadSize   = 320 << 10
	vhdxMaxEntries = 2047
	vhdxMaxSize    = 64 << 40
	// The caps bound the memory that a forged header can make Open allocate.
	// Azure writes a 1 MiB metadata region and at most 2^18 blocks.
	vhdxMaxMetadata = 1 << 20
	vhdxMaxBlocks   = 1 << 22

	vhdxBlockNotPresent   = 0
	vhdxBlockUndefined    = 1
	vhdxBlockZero         = 2
	vhdxBlockUnmapped     = 3
	vhdxBlockFullyPresent = 6

	vhdxMetadataRequired = 1 << 2
	vhdxHasParent        = 1 << 1
)

var (
	vhdxSignature          = []byte("vhdxfile")
	vhdxHeaderOffsets      = [2]int{64 << 10, 128 << 10}
	vhdxRegionTableOffsets = [2]int{192 << 10, 256 << 10}

	vhdxBATRegion          = vhdxGUID("2DC27766-F623-4200-9D64-115E9BFD4A08")
	vhdxMetadataRegion     = vhdxGUID("8B7CA206-4790-4B9A-B8FE-575F050F886E")
	vhdxFileParameters     = vhdxGUID("CAA16737-FA36-4D43-B3B6-33F0AA44E76B")
	vhdxVirtualDiskSize    = vhdxGUID("2FA54224-CD1B-4876-B211-5DBED83BF4B8")
	vhdxLogicalSectorSize  = vhdxGUID("8141BF1D-A96F-4709-BA47-F233A8FAAB5F")
	vhdxPhysicalSectorSize = vhdxGUID("CDA348C7-445D-4471-9CC9-E9885251C556")
	vhdxVirtualDiskID      = vhdxGUID("BECA12AB-B2E6-4523-93EF-C309E000C746")

	crc32c = crc32.MakeTable(crc32.Castagnoli)
)

// vhdxGUID returns a GUID in its on-disk form, whose first three fields are
// little-endian.
func vhdxGUID(s string) [16]byte {
	b, err := hex.DecodeString(strings.ReplaceAll(s, "-", ""))
	if err != nil || len(b) != 16 {
		panic("azurediskfile: invalid GUID " + s)
	}
	var g [16]byte
	copy(g[:], b)
	g[0], g[1], g[2], g[3] = b[3], b[2], b[1], b[0]
	g[4], g[5] = b[5], b[4]
	g[6], g[7] = b[7], b[6]
	return g
}

type vhdxBlobAPI struct {
	inner     BlobAPI
	size      int64
	blockSize int64
	// fileOffsets holds where each payload block starts in the blob, or -1 for
	// a block that reads as zeros.
	fileOffsets []int64
}

type vhdxRegion struct {
	offset, length int64
}

func unwrapVHDX(ctx context.Context, api BlobAPI, size int64, format Format) (BlobAPI, int64, error) {
	switch format {
	case FormatVHD:
		return api, size, nil
	case FormatAuto:
		if size < vhdxHeadSize {
			return api, size, nil
		}
		prefix, err := readFull(ctx, api, 0, int64(len(vhdxSignature)))
		if err != nil {
			return nil, 0, fmt.Errorf("azurediskfile: read the file identifier: %w", err)
		}
		if !bytes.Equal(prefix, vhdxSignature) {
			return api, size, nil
		}
	case FormatVHDX:
		if size < vhdxHeadSize {
			return nil, 0, fmt.Errorf("azurediskfile: VHDX: the %d-byte blob is too small", size)
		}
	default:
		return nil, 0, fmt.Errorf("azurediskfile: unknown format %d", format)
	}
	vhdx, err := openVHDX(ctx, api, size)
	if err != nil {
		return nil, 0, err
	}
	return vhdx, vhdx.size, nil
}

func openVHDX(ctx context.Context, inner BlobAPI, fileSize int64) (*vhdxBlobAPI, error) {
	head, err := readFull(ctx, inner, 0, vhdxHeadSize)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(head[:len(vhdxSignature)], vhdxSignature) {
		return nil, errors.New("azurediskfile: VHDX: no file identifier")
	}
	if err := checkVHDXHeader(head); err != nil {
		return nil, err
	}
	regions, err := parseVHDXRegions(head, fileSize)
	if err != nil {
		return nil, err
	}
	metadataRegion, batRegion := regions[vhdxMetadataRegion], regions[vhdxBATRegion]
	if metadataRegion.length > vhdxMaxMetadata {
		return nil, fmt.Errorf("azurediskfile: VHDX: unsupported %d-byte metadata region", metadataRegion.length)
	}
	metadata, err := readFull(ctx, inner, metadataRegion.offset, metadataRegion.length)
	if err != nil {
		return nil, err
	}
	size, blockSize, sectorSize, err := parseVHDXMetadata(metadata)
	if err != nil {
		return nil, err
	}

	blocks := (size + blockSize - 1) / blockSize
	if blocks > vhdxMaxBlocks {
		return nil, fmt.Errorf("azurediskfile: VHDX: unsupported %d payload blocks", blocks)
	}
	// A sector bitmap entry follows every chunkRatio payload entries.
	chunkRatio := (1 << 23) * sectorSize / blockSize
	entries := blocks + (blocks-1)/chunkRatio
	if entries*8 > batRegion.length {
		return nil, fmt.Errorf("azurediskfile: VHDX: the BAT holds %d bytes, want %d", batRegion.length, entries*8)
	}
	bat, err := readFull(ctx, inner, batRegion.offset, entries*8)
	if err != nil {
		return nil, err
	}
	fileOffsets := make([]int64, blocks)
	for block := range blocks {
		entry := binary.LittleEndian.Uint64(bat[(block+block/chunkRatio)*8:])
		switch state := entry & 7; state {
		case vhdxBlockNotPresent, vhdxBlockUndefined, vhdxBlockZero, vhdxBlockUnmapped:
			fileOffsets[block] = -1
		case vhdxBlockFullyPresent:
			offset := int64(entry>>20) << 20
			length := min(blockSize, size-block*blockSize)
			if offset < 1<<20 || offset > fileSize-length {
				return nil, fmt.Errorf("azurediskfile: VHDX: payload block %d at %d is outside the %d-byte blob", block, offset, fileSize)
			}
			fileOffsets[block] = offset
		default:
			return nil, fmt.Errorf("azurediskfile: VHDX: payload block %d has unsupported state %d", block, state)
		}
	}
	return &vhdxBlobAPI{inner: inner, size: size, blockSize: blockSize, fileOffsets: fileOffsets}, nil
}

func checkVHDXHeader(head []byte) error {
	var current []byte
	var sequence uint64
	for _, offset := range vhdxHeaderOffsets {
		header := head[offset : offset+vhdxHeaderSize]
		if string(header[:4]) != "head" || !validVHDXChecksum(header) {
			continue
		}
		if s := binary.LittleEndian.Uint64(header[8:]); current == nil || s > sequence {
			current, sequence = header, s
		}
	}
	if current == nil {
		return errors.New("azurediskfile: VHDX: no valid header")
	}
	if version := binary.LittleEndian.Uint16(current[66:]); version != 1 {
		return fmt.Errorf("azurediskfile: VHDX: unsupported version %d", version)
	}
	// A log GUID means the metadata is only consistent after replaying the log.
	if !bytes.Equal(current[48:64], make([]byte, 16)) {
		return errors.New("azurediskfile: VHDX: the file has a log to replay, which is not supported")
	}
	return nil
}

func parseVHDXRegions(head []byte, fileSize int64) (map[[16]byte]vhdxRegion, error) {
	var table []byte
	for _, offset := range vhdxRegionTableOffsets {
		candidate := head[offset : offset+vhdxRegionTableSize]
		if string(candidate[:4]) == "regi" && validVHDXChecksum(candidate) {
			table = candidate
			break
		}
	}
	if table == nil {
		return nil, errors.New("azurediskfile: VHDX: no valid region table")
	}
	count := binary.LittleEndian.Uint32(table[8:])
	if count > vhdxMaxEntries {
		return nil, fmt.Errorf("azurediskfile: VHDX: %d region table entries", count)
	}
	regions := make(map[[16]byte]vhdxRegion)
	for i := range int(count) {
		entry := table[16+32*i : 48+32*i]
		id := [16]byte(entry[:16])
		offset := binary.LittleEndian.Uint64(entry[16:])
		length := int64(binary.LittleEndian.Uint32(entry[24:]))
		required := binary.LittleEndian.Uint32(entry[28:])&1 != 0
		if id != vhdxBATRegion && id != vhdxMetadataRegion {
			if required {
				return nil, fmt.Errorf("azurediskfile: VHDX: unknown required region %x", id)
			}
			continue
		}
		if offset < 1<<20 || offset > uint64(fileSize) || length > fileSize-int64(offset) {
			return nil, fmt.Errorf("azurediskfile: VHDX: region %x at %d+%d is outside the %d-byte blob", id, offset, length, fileSize)
		}
		regions[id] = vhdxRegion{offset: int64(offset), length: length}
	}
	if _, ok := regions[vhdxBATRegion]; !ok {
		return nil, errors.New("azurediskfile: VHDX: no BAT region")
	}
	if _, ok := regions[vhdxMetadataRegion]; !ok {
		return nil, errors.New("azurediskfile: VHDX: no metadata region")
	}
	return regions, nil
}

func parseVHDXMetadata(metadata []byte) (size, blockSize, sectorSize int64, err error) {
	if len(metadata) < 32 || string(metadata[:8]) != "metadata" {
		return 0, 0, 0, errors.New("azurediskfile: VHDX: no metadata table")
	}
	count := int(binary.LittleEndian.Uint16(metadata[10:]))
	if count > vhdxMaxEntries || 32+32*count > len(metadata) {
		return 0, 0, 0, fmt.Errorf("azurediskfile: VHDX: %d metadata entries", count)
	}
	items := make(map[[16]byte][]byte)
	for i := range count {
		entry := metadata[32+32*i : 64+32*i]
		id := [16]byte(entry[:16])
		offset := uint64(binary.LittleEndian.Uint32(entry[16:]))
		length := uint64(binary.LittleEndian.Uint32(entry[20:]))
		flags := binary.LittleEndian.Uint32(entry[24:])
		if offset+length > uint64(len(metadata)) {
			return 0, 0, 0, fmt.Errorf("azurediskfile: VHDX: metadata item %x is outside the region", id)
		}
		switch id {
		case vhdxFileParameters, vhdxVirtualDiskSize, vhdxLogicalSectorSize:
			items[id] = metadata[offset : offset+length]
		case vhdxPhysicalSectorSize, vhdxVirtualDiskID:
		default:
			if flags&vhdxMetadataRequired != 0 {
				return 0, 0, 0, fmt.Errorf("azurediskfile: VHDX: unknown required metadata item %x", id)
			}
		}
	}
	parameters, sizeItem, sectorItem := items[vhdxFileParameters], items[vhdxVirtualDiskSize], items[vhdxLogicalSectorSize]
	if len(parameters) != 8 || len(sizeItem) != 8 || len(sectorItem) != 4 {
		return 0, 0, 0, errors.New("azurediskfile: VHDX: missing file parameters, virtual disk size or logical sector size")
	}
	if binary.LittleEndian.Uint32(parameters[4:])&vhdxHasParent != 0 {
		return 0, 0, 0, errors.New("azurediskfile: VHDX: differencing disks are not supported")
	}
	blockSize = int64(binary.LittleEndian.Uint32(parameters))
	sectorSize = int64(binary.LittleEndian.Uint32(sectorItem))
	size = int64(min(binary.LittleEndian.Uint64(sizeItem), vhdxMaxSize+1))
	if blockSize < 1<<20 || blockSize > 256<<20 || blockSize&(blockSize-1) != 0 {
		return 0, 0, 0, fmt.Errorf("azurediskfile: VHDX: invalid block size %d", blockSize)
	}
	if sectorSize != 512 && sectorSize != 4096 {
		return 0, 0, 0, fmt.Errorf("azurediskfile: VHDX: invalid logical sector size %d", sectorSize)
	}
	if size <= 0 || size > vhdxMaxSize || size%sectorSize != 0 {
		return 0, 0, 0, fmt.Errorf("azurediskfile: VHDX: invalid virtual disk size %d", size)
	}
	return size, blockSize, sectorSize, nil
}

func validVHDXChecksum(structure []byte) bool {
	want := binary.LittleEndian.Uint32(structure[4:])
	zeroed := bytes.Clone(structure)
	clear(zeroed[4:8])
	return crc32.Checksum(zeroed, crc32c) == want
}

// readFull reads in ranges of at most 4 MiB, so that each still gets a range
// checksum.
func readFull(ctx context.Context, api BlobAPI, off, count int64) ([]byte, error) {
	data := make([]byte, 0, count)
	for done := int64(0); done < count; {
		n := min(count-done, maxRangeGetContentMD5)
		part, err := api.ReadRange(ctx, off+done, n)
		if err != nil {
			return nil, fmt.Errorf("azurediskfile: range %d-%d: %w", off+done, off+done+n-1, err)
		}
		if int64(len(part)) != n {
			return nil, fmt.Errorf("azurediskfile: range %d-%d returned %d bytes, want %d", off+done, off+done+n-1, len(part), n)
		}
		data = append(data, part...)
		done += n
	}
	return data, nil
}

func (v *vhdxBlobAPI) BlobIdentifier() string {
	if identified, ok := v.inner.(blobIdentifier); ok {
		return identified.BlobIdentifier()
	}
	return ""
}

func (v *vhdxBlobAPI) Size(context.Context) (int64, error) { return v.size, nil }

func (v *vhdxBlobAPI) ReadRange(ctx context.Context, off, count int64) ([]byte, error) {
	if off < 0 || count < 0 || off > v.size-count {
		return nil, fmt.Errorf("azurediskfile: VHDX: range %d+%d is outside the %d-byte disk", off, count, v.size)
	}
	data := make([]byte, count)
	for done := int64(0); done < count; {
		position := off + done
		block, within := position/v.blockSize, position%v.blockSize
		n := min(count-done, v.blockSize-within)
		if fileOffset := v.fileOffsets[block]; fileOffset >= 0 {
			part, err := v.inner.ReadRange(ctx, fileOffset+within, n)
			if err != nil {
				return nil, err
			}
			if int64(len(part)) != n {
				return nil, fmt.Errorf("azurediskfile: VHDX: blob range at %d returned %d bytes, want %d", fileOffset+within, len(part), n)
			}
			copy(data[done:], part)
		}
		done += n
	}
	return data, nil
}

// PageRanges maps the blob's allocated pages into the virtual disk, so that the
// reader still skips unallocated pages. When the blob's ranges are unknown,
// every present block counts as allocated.
func (v *vhdxBlobAPI) PageRanges(ctx context.Context) ([]Range, error) {
	fileRanges, err := v.inner.PageRanges(ctx)
	if err != nil {
		fileRanges = nil
	}
	fileRanges = normalizeRanges(fileRanges)
	var ranges []Range
	for block, fileOffset := range v.fileOffsets {
		if fileOffset < 0 {
			continue
		}
		start := int64(block) * v.blockSize
		length := min(v.blockSize, v.size-start)
		if len(fileRanges) == 0 {
			ranges = append(ranges, Range{Start: start, End: start + length - 1})
			continue
		}
		fileEnd := fileOffset + length - 1
		i := sort.Search(len(fileRanges), func(i int) bool { return fileRanges[i].End >= fileOffset })
		for ; i < len(fileRanges) && fileRanges[i].Start <= fileEnd; i++ {
			ranges = append(ranges, Range{
				Start: start + max(fileRanges[i].Start, fileOffset) - fileOffset,
				End:   start + min(fileRanges[i].End, fileEnd) - fileOffset,
			})
		}
	}
	return ranges, nil
}

package dfile

import (
	"syscall"
	"unsafe"
)

const (
	O_DIRECT = syscall.O_DIRECT
	O_DSYNC = syscall.O_DSYNC

	// what to align the block buffer to
	alignment = 4096

	// BlockSize sic
	BlockSize = alignment
)

// AllocateAligned returns []byte of size aligned to alignment
func AllocateAligned(size int) []byte {
	block := make([]byte, size+alignment)
	shift := alignmentShift(block)
	offset := 0
	if shift != 0 {
		offset = alignment - shift
	}
	block = block[offset:size]
	shift = alignmentShift(block)
	if shift != 0 {
		panic("Alignment failure")
	}
	return block
}

// alignmentShift returns alignment of the block in memory
func alignmentShift(block []byte) int {
	if len(block) == 0 {
		return 0
	}
	return int(uintptr(unsafe.Pointer(&block[0])) & uintptr(alignment-1))
}

package os

import (
	stdos "os"
)

// UseLargePages controls whether large-page allocation is requested.
var UseLargePages bool

// LargePageSize stores the detected large page size.
var LargePageSize uint64

// ProcVarInit resets process helper state.
func ProcVarInit() {
	UseLargePages = false
	LargePageSize = 0
}

// ProcGetNumber returns the current process id.
func ProcGetNumber() uint64 {
	return uint64(stdos.Getpid())
}

// MemAllocLarge allocates a page-aligned buffer and updates size.
func MemAllocLarge(size *uint64) []byte {
	if size == nil || *size == 0 {
		return nil
	}
	pageSize := uint64(stdos.Getpagesize())
	if pageSize == 0 {
		pageSize = 4096
	}
	if rem := *size % pageSize; rem != 0 {
		*size += pageSize - rem
	}
	maxInt := uint64(^uint(0) >> 1)
	if *size > maxInt {
		return nil
	}
	return make([]byte, int(*size))
}

// MemFreeLarge releases a buffer allocated with MemAllocLarge.
func MemFreeLarge(_ []byte) {
	// Go GC handles deallocation.
}

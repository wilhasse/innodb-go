package fil

import (
	"errors"
	"hash/crc32"
	"sync/atomic"

	iblog "github.com/wilhasse/innodb-go/log"
	"github.com/wilhasse/innodb-go/mach"
	"github.com/wilhasse/innodb-go/ut"
)

// ErrChecksumMismatch reports a failed checksum validation.
var ErrChecksumMismatch = errors.New("fil: page checksum mismatch")

var checksumsEnabled uint32 = 1

// SetChecksumsEnabled updates checksum verification behavior.
func SetChecksumsEnabled(enabled bool) {
	if enabled {
		atomic.StoreUint32(&checksumsEnabled, 1)
		return
	}
	atomic.StoreUint32(&checksumsEnabled, 0)
}

func checksumsOn() bool {
	return atomic.LoadUint32(&checksumsEnabled) == 1
}

func applyPageChecksum(page []byte) {
	if len(page) < ut.UNIV_PAGE_SIZE {
		return
	}
	if mach.ReadUll(page[PageLSN:]) == 0 {
		mach.WriteUll(page[PageLSN:], iblog.CurrentLSN())
	}
	if !checksumsOn() {
		return
	}
	mach.WriteTo4(page[PageSpaceOrChecksum:], 0)
	checksum := crc32.ChecksumIEEE(page[:ut.UNIV_PAGE_SIZE])
	mach.WriteTo4(page[PageSpaceOrChecksum:], checksum)
}

func verifyPageChecksum(page []byte) error {
	if !checksumsOn() || len(page) < ut.UNIV_PAGE_SIZE {
		return nil
	}
	stored := mach.ReadFrom4(page[PageSpaceOrChecksum:])
	if stored == 0 {
		return nil
	}
	mach.WriteTo4(page[PageSpaceOrChecksum:], 0)
	checksum := crc32.ChecksumIEEE(page[:ut.UNIV_PAGE_SIZE])
	mach.WriteTo4(page[PageSpaceOrChecksum:], stored)
	if checksum != stored {
		return ErrChecksumMismatch
	}
	return nil
}

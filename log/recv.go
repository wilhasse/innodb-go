package log

import (
	"sync"

	"github.com/wilhasse/innodb-go/mach"
)

// RecoveryMode describes recovery behavior.
type RecoveryMode int

const (
	RecoveryNormal RecoveryMode = iota
	RecoveryCrash
)

// RecvAddrState tracks log record application state.
type RecvAddrState int

const (
	RecvNotProcessed RecvAddrState = iota
	RecvBeingRead
	RecvBeingProcessed
	RecvProcessed
)

// RecvRecord stores a parsed log record.
type RecvRecord struct {
	Type     byte
	StartLSN uint64
	EndLSN   uint64
	Data     []byte
}

// RecvAddr tracks log records for a page.
type RecvAddr struct {
	State   RecvAddrState
	Space   uint32
	PageNo  uint32
	Records []RecvRecord
}

// RecvSys holds recovery state.
type RecvSys struct {
	mu                  sync.Mutex
	ApplyLogRecs        bool
	ApplyBatchOn        bool
	LSN                 uint64
	LastLogBufSize      uint32
	LastBlock           []byte
	Buf                 []byte
	Len                 int
	ParseStartLSN       uint64
	ScannedLSN          uint64
	ScannedCheckpointNo uint32
	RecoveredOffset     int
	RecoveredLSN        uint64
	LimitLSN            uint64
	FoundCorruptLog     bool
	Hash                map[recvAddrKey]*RecvAddr
	NAddrs              uint64
}

type recvAddrKey struct {
	space  uint32
	pageNo uint32
}

// Global recovery state.
var (
	RecvSysState           *RecvSys
	RecvRecoveryOn         bool
	RecvRecoveryFromBackup bool
	RecvNeededRecovery     bool
	RecvLSNChecksOn        bool
	RecvNoIbufOperations   bool
	RecvScanPrintCounter   uint64
	RecvMaxParsedPageNo    uint32
	RecvNPoolFreeFrames    uint64
	RecvMaxPageLSN         uint64
)

const (
	RecvParsingBufSize = 2 * 1024 * 1024
)

// RecvSysVarInit resets recovery globals.
func RecvSysVarInit() {
	RecvSysState = nil
	RecvRecoveryOn = false
	RecvRecoveryFromBackup = false
	RecvNeededRecovery = false
	RecvLSNChecksOn = false
	RecvNoIbufOperations = false
	RecvScanPrintCounter = 0
	RecvMaxParsedPageNo = 0
	RecvNPoolFreeFrames = 256
	RecvMaxPageLSN = 0
}

// RecvSysCreate initializes the recovery system container.
func RecvSysCreate() {
	if RecvSysState != nil {
		return
	}
	RecvSysState = &RecvSys{}
}

// RecvSysClose releases recovery system mutexes.
func RecvSysClose() {
	if RecvSysState == nil {
		return
	}
	RecvSysState.mu = sync.Mutex{}
}

// RecvSysMemFree clears recovery system memory.
func RecvSysMemFree() {
	if RecvSysState == nil {
		return
	}
	RecvSysState = nil
}

// RecvSysInit prepares the recovery system for a scan.
func RecvSysInit(_ uint64) {
	if RecvSysState == nil {
		RecvSysCreate()
	}
	recv := RecvSysState
	recv.mu.Lock()
	defer recv.mu.Unlock()
	if recv.Buf != nil {
		return
	}
	recv.Buf = make([]byte, RecvParsingBufSize)
	recv.Len = 0
	recv.RecoveredOffset = 0
	recv.Hash = make(map[recvAddrKey]*RecvAddr)
	recv.NAddrs = 0
	recv.ApplyLogRecs = false
	recv.ApplyBatchOn = false
	recv.LastBlock = make([]byte, 0)
	recv.FoundCorruptLog = false
	RecvMaxPageLSN = 0
}

// RecvRecoveryIsOn reports whether recovery is active.
func RecvRecoveryIsOn() bool {
	return RecvRecoveryOn
}

// RecvRecoveryFromBackupIsOn reports recovery from backup state.
func RecvRecoveryFromBackupIsOn() bool {
	return RecvRecoveryFromBackup
}

// RecvAddRecord stores a parsed log record for a page.
func RecvAddRecord(spaceID, pageNo uint32, recType byte, data []byte, startLSN, endLSN uint64) {
	if RecvSysState == nil {
		RecvSysCreate()
	}
	recv := RecvSysState
	recv.mu.Lock()
	defer recv.mu.Unlock()
	key := recvAddrKey{space: spaceID, pageNo: pageNo}
	addr := recv.Hash[key]
	if addr == nil {
		addr = &RecvAddr{State: RecvNotProcessed, Space: spaceID, PageNo: pageNo}
		recv.Hash[key] = addr
		recv.NAddrs++
	}
	rec := RecvRecord{
		Type:     recType,
		StartLSN: startLSN,
		EndLSN:   endLSN,
		Data:     append([]byte(nil), data...),
	}
	addr.Records = append(addr.Records, rec)
	if pageNo > RecvMaxParsedPageNo {
		RecvMaxParsedPageNo = pageNo
	}
	if endLSN > RecvMaxPageLSN {
		RecvMaxPageLSN = endLSN
	}
}

// RecvRecoverPage applies stored records to a page buffer.
func RecvRecoverPage(spaceID, pageNo uint32, page []byte) bool {
	if RecvSysState == nil || page == nil {
		return false
	}
	recv := RecvSysState
	recv.mu.Lock()
	defer recv.mu.Unlock()
	key := recvAddrKey{space: spaceID, pageNo: pageNo}
	addr := recv.Hash[key]
	if addr == nil {
		return false
	}
	maxLSN := pageLSN(page)
	for _, rec := range addr.Records {
		if rec.EndLSN > maxLSN {
			maxLSN = rec.EndLSN
		}
	}
	setPageLSN(page, maxLSN)
	delete(recv.Hash, key)
	if recv.NAddrs > 0 {
		recv.NAddrs--
	}
	return true
}

// RecvApplyHashedLogRecs clears the stored log records.
func RecvApplyHashedLogRecs(allowIbuf bool) {
	if RecvSysState == nil {
		return
	}
	recv := RecvSysState
	recv.mu.Lock()
	defer recv.mu.Unlock()
	recv.Hash = make(map[recvAddrKey]*RecvAddr)
	recv.NAddrs = 0
	if !allowIbuf {
		RecvNoIbufOperations = true
	}
}

// RecvRecoveryFromCheckpointStart starts recovery from a checkpoint.
func RecvRecoveryFromCheckpointStart(_ RecoveryMode, minFlushedLSN, maxFlushedLSN uint64) int {
	RecvRecoveryOn = true
	RecvNeededRecovery = true
	RecvLSNChecksOn = true
	if maxFlushedLSN > minFlushedLSN {
		RecvMaxPageLSN = maxFlushedLSN
	}
	return 0
}

// RecvRecoveryFromCheckpointFinish completes recovery.
func RecvRecoveryFromCheckpointFinish(_ RecoveryMode) {
	RecvRecoveryOn = false
	RecvNeededRecovery = false
	RecvLSNChecksOn = false
	RecvNoIbufOperations = false
	if RecvSysState != nil {
		RecvApplyHashedLogRecs(true)
	}
}

// RecvRecoveryRollbackActive marks recovery rollback invocation.
func RecvRecoveryRollbackActive() {
	RecvRecoveryOn = false
}

// RecvScanLogRecs updates scan progress and optionally stores data.
func RecvScanLogRecs(storeToHash bool, buf []byte, startLSN uint64, contiguousLSN *uint64, groupScannedLSN *uint64) bool {
	if RecvSysState == nil {
		RecvSysCreate()
	}
	offset := 0
	for offset < len(buf) {
		rec, size, err := DecodeRecord(buf[offset:])
		if err != nil {
			if err == errShortRecord {
				break
			}
			RecvSysState.FoundCorruptLog = true
			break
		}
		recStart := startLSN + uint64(offset)
		recEnd := recStart + uint64(size)
		if storeToHash {
			RecvAddRecord(rec.SpaceID, rec.PageNo, rec.Type, rec.Payload, recStart, recEnd)
		}
		offset += size
	}
	end := startLSN + uint64(offset)
	if groupScannedLSN != nil {
		*groupScannedLSN = end
	}
	if contiguousLSN != nil && end > *contiguousLSN {
		*contiguousLSN = end
	}
	return offset == len(buf)
}

// RecvResetLogs resets the log system to a new start lsn.
func RecvResetLogs(lsn uint64) {
	Init()
	System.lsn = lsn
	System.flushed = lsn
	System.entries = nil
}

const pageLSNOffset = 16

func pageLSN(page []byte) uint64 {
	offs := int(pageLSNOffset)
	if len(page) < offs+8 {
		return 0
	}
	return mach.ReadUll(page[offs:])
}

func setPageLSN(page []byte, lsn uint64) {
	offs := int(pageLSNOffset)
	if len(page) < offs+8 {
		return
	}
	mach.WriteUll(page[offs:], lsn)
}

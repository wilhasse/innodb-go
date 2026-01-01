package log

import "errors"

// NeedsRecovery reports whether redo recovery should run.
func NeedsRecovery() bool {
	if System == nil || System.file == nil {
		return false
	}
	if System.lsn > System.checkpoint {
		return true
	}
	if System.flushed < System.lsn {
		return true
	}
	return false
}

// Recover scans the log file from the checkpoint and populates the recv hash.
func Recover() error {
	if System == nil || System.file == nil {
		return nil
	}
	start := System.checkpoint
	end := System.lsn
	if end < start {
		return errors.New("log: invalid recovery range")
	}
	RecvSysVarInit()
	RecvSysCreate()
	RecvSysInit(start)
	RecvRecoveryFromCheckpointStart(RecoveryCrash, start, System.flushed)
	if _, _, err := RecvScanLogFile(System.file, start, end); err != nil {
		return err
	}
	RecvRecoveryFromCheckpointFinish(RecoveryCrash)
	return nil
}

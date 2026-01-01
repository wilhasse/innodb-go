package log

import ibos "github.com/wilhasse/innodb-go/os"

// CloseFileForCrash closes the log file without checkpointing (test helper).
func CloseFileForCrash() {
	if System == nil || System.file == nil {
		return
	}
	_ = ibos.FileClose(System.file)
	System.file = nil
}

package api

import "github.com/wilhasse/innodb-go/log"

func configureLog() {
	var logDir string
	_ = CfgGet("log_group_home_dir", &logDir)
	var fileSize Ulint
	_ = CfgGet("log_file_size", &fileSize)
	var files Ulint
	_ = CfgGet("log_files_in_group", &files)
	var bufferSize Ulint
	_ = CfgGet("log_buffer_size", &bufferSize)
	dataDir := dataHomeDir()
	enabled := fileSize > 0 && (dataDir != "." || logDir != "")
	log.Configure(log.Config{
		Enabled:    enabled,
		DataDir:    dataDir,
		LogDir:     logDir,
		FileSize:   uint64(fileSize),
		Files:      int(files),
		BufferSize: uint64(bufferSize),
	})
}

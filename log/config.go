package log

import "path/filepath"

// Config controls redo log file setup.
type Config struct {
	Enabled     bool
	DataDir     string
	LogDir      string
	FileSize    uint64
	Files       int
	BufferSize  uint64
	Preallocate bool
}

var (
	config    Config
	configSet bool
)

// Configure sets log system configuration used by Init.
func Configure(cfg Config) {
	config = cfg
	configSet = true
}

func currentConfig() (Config, bool) {
	return config, configSet
}

func resolveLogDir(cfg Config) string {
	base := cfg.DataDir
	if base == "" {
		base = "."
	}
	dir := cfg.LogDir
	if dir == "" {
		return base
	}
	if filepath.IsAbs(dir) {
		return dir
	}
	return filepath.Join(base, dir)
}

func normalizeLogFiles(cfg Config) int {
	if cfg.Files <= 0 {
		return 1
	}
	return cfg.Files
}

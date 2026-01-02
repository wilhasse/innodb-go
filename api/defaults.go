package api

import (
	"os"
	"strings"
)

const defaultDataFilePath = "ibdata1:32M:autoextend"

func defaultFilePreallocate() Bool {
	raw := strings.TrimSpace(os.Getenv("INNODB_FILE_PREALLOCATE"))
	if raw == "" {
		return IBFalse
	}
	switch strings.ToLower(raw) {
	case "1", "true", "yes", "on":
		return IBTrue
	case "0", "false", "no", "off":
		return IBFalse
	default:
		return IBFalse
	}
}

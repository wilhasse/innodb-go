package api

import (
	"bytes"
	"strings"
)

const (
	apiVersionCurrent  = 3
	apiVersionRevision = 0
	apiVersionAge      = 0
)

var (
	initialized      bool
	started          bool
	activeDBFormat   string
	clientComparator ClientCompare = DefaultCompare
)

// APIVersion returns the packed API version number.
func APIVersion() uint64 {
	return (uint64(apiVersionCurrent) << 32) |
		(uint64(apiVersionRevision) << 16) |
		uint64(apiVersionAge)
}

// Init initializes the API layer.
func Init() ErrCode {
	if initialized {
		return DB_SUCCESS
	}
	if err := CfgInit(); err != DB_SUCCESS {
		return err
	}
	initialized = true
	return DB_SUCCESS
}

// Startup initializes internal state and validates file format (if provided).
func Startup(format string) ErrCode {
	if !initialized {
		return DB_ERROR
	}
	if format != "" && !isSupportedFormat(format) {
		Log(nil, "InnoDB: format '%s' unknown.", format)
		return DB_UNSUPPORTED
	}
	activeDBFormat = format
	started = true
	return DB_SUCCESS
}

// Shutdown resets API state.
func Shutdown(_ ShutdownFlag) ErrCode {
	if !initialized {
		return DB_ERROR
	}
	if err := CfgShutdown(); err != DB_SUCCESS {
		return err
	}
	started = false
	activeDBFormat = ""
	initialized = false
	return DB_SUCCESS
}

// SetClientCompare sets the client comparison hook.
func SetClientCompare(compare ClientCompare) {
	if compare == nil {
		compare = DefaultCompare
	}
	clientComparator = compare
}

// ClientCompareFunc returns the active comparison hook.
func ClientCompareFunc() ClientCompare {
	return clientComparator
}

// DefaultCompare provides a bytewise comparison compatible with memcmp.
func DefaultCompare(_ *ColMeta, p1 []byte, p2 []byte) int {
	return bytes.Compare(p1, p2)
}

func isSupportedFormat(format string) bool {
	switch strings.ToLower(format) {
	case "antelope", "barracuda":
		return true
	default:
		return false
	}
}

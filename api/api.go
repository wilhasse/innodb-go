package api

import (
	"bytes"
	"strings"

	"github.com/wilhasse/innodb-go/btr"
	"github.com/wilhasse/innodb-go/buf"
	"github.com/wilhasse/innodb-go/dict"
	"github.com/wilhasse/innodb-go/fil"
	"github.com/wilhasse/innodb-go/fsp"
	"github.com/wilhasse/innodb-go/log"
	"github.com/wilhasse/innodb-go/lock"
	"github.com/wilhasse/innodb-go/page"
	"github.com/wilhasse/innodb-go/trx"
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
	fil.VarInit()
	fsp.Init()
	page.PageRegistry = page.NewRegistry()
	btr.CurVarInit()
	btr.SearchVarInit()
	configureLog()
	log.Init()
	if err := log.InitErr(); err != nil {
		return DB_ERROR
	}
	trx.TrxVarInit()
	trx.TrxSysVarInit()
	trx.PurgeVarInit()
	trx.RsegVarInit()
	lock.SysCreate(0)
	if !fil.SpaceCreate("system", 0, 0, fil.SpaceTablespace) {
		return DB_ERROR
	}
	_ = fil.SpaceCreate("log", 1, 0, fil.SpaceLog)
	dict.SetDataDir(dataHomeDir())
	dict.DictBootstrap()
	if err := loadSchemaFromDict(); err != DB_SUCCESS {
		return err
	}
	if log.NeedsRecovery() {
		if err := log.Recover(); err != nil {
			return DB_ERROR
		}
	}
	var bufSize uint64
	if err := CfgGet("buffer_pool_size", &bufSize); err == DB_SUCCESS && bufSize > 0 {
		pageSize := buf.BufPoolDefaultPageSize
		capacity := int(bufSize / uint64(pageSize))
		if capacity < 1 {
			capacity = 1
		}
		buf.SetDefaultPool(buf.NewPool(capacity, pageSize))
	}
	var ahi Bool
	if err := CfgGet("adaptive_hash_index", &ahi); err == DB_SUCCESS && ahi == IBTrue {
		btr.SearchSysCreate(1024)
	}
	activeDBFormat = format
	started = true
	return DB_SUCCESS
}

// Shutdown resets API state.
func Shutdown(_ ShutdownFlag) ErrCode {
	if err := CfgShutdown(); err != DB_SUCCESS {
		return err
	}
	resetSchemaState()
	log.Shutdown()
	buf.SetDefaultPool(nil)
	dict.DictClose()
	lock.SysClose()
	fil.VarInit()
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

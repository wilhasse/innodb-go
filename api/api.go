package api

import (
	"bytes"
	"strings"
	"time"

	"github.com/wilhasse/innodb-go/btr"
	"github.com/wilhasse/innodb-go/buf"
	"github.com/wilhasse/innodb-go/dict"
	"github.com/wilhasse/innodb-go/fil"
	"github.com/wilhasse/innodb-go/fsp"
	"github.com/wilhasse/innodb-go/lock"
	"github.com/wilhasse/innodb-go/log"
	"github.com/wilhasse/innodb-go/page"
	"github.com/wilhasse/innodb-go/srv"
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
	var checksums Bool
	if err := CfgGet("checksums", &checksums); err == DB_SUCCESS {
		fil.SetChecksumsEnabled(checksums == IBTrue)
	}
	configureLog()
	log.Init()
	if err := log.InitErr(); err != nil {
		return DB_ERROR
	}
	trx.TrxVarInit()
	trx.TrxSysVarInit()
	trx.PurgeVarInit()
	trx.PurgeSysCreate()
	trx.RsegVarInit()
	lock.SysCreate(0)
	var lockWait Ulint
	if err := CfgGet("lock_wait_timeout", &lockWait); err == DB_SUCCESS {
		lock.SetWaitTimeout(time.Duration(lockWait) * time.Second)
	}
	if !fil.SpaceCreate("system", 0, 0, fil.SpaceTablespace) {
		return DB_ERROR
	}
	_ = fil.SpaceCreate("log", 1, 0, fil.SpaceLog)
	if err := openSystemTablespace(); err != DB_SUCCESS {
		return err
	}
	var dblwr Bool
	if err := CfgGet("doublewrite", &dblwr); err == DB_SUCCESS {
		fil.SetDoublewriteEnabled(dblwr == IBTrue)
		if dblwr == IBTrue {
			if err := fil.DoublewriteInit(dataHomeDir()); err != nil {
				return DB_ERROR
			}
			if err := fil.DoublewriteRecover(); err != nil {
				return DB_ERROR
			}
		}
	}
	dict.SetDataDir(dataHomeDir())
	dict.SetSysPersister(&sysTablePersister{})
	dict.DictBootstrap()
	if err := recoverDDLLog(); err != DB_SUCCESS {
		return err
	}
	if err := trx.UndoStoreInit(dataHomeDir()); err != nil {
		return DB_ERROR
	}
	if err := trx.UndoStoreRecover(); err != nil {
		return DB_ERROR
	}
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
		totalPages := int(bufSize / uint64(pageSize))
		if totalPages < 1 {
			totalPages = 1
		}
		var instances Ulint
		if err := CfgGet("buffer_pool_instances", &instances); err != DB_SUCCESS || instances < 1 {
			instances = 1
		}
		instanceCount := int(instances)
		if instanceCount < 1 {
			instanceCount = 1
		}
		if totalPages < instanceCount {
			instanceCount = totalPages
		}
		capacity := totalPages / instanceCount
		remainder := totalPages % instanceCount
		pools := make([]*buf.Pool, instanceCount)
		for i := 0; i < instanceCount; i++ {
			cap := capacity
			if i < remainder {
				cap++
			}
			if cap < 1 {
				cap = 1
			}
			pools[i] = buf.NewPool(cap, pageSize)
		}
		buf.SetDefaultPools(pools)
	}
	var ahi Bool
	if err := CfgGet("adaptive_hash_index", &ahi); err == DB_SUCCESS && ahi == IBTrue {
		btr.SearchSysCreate(1024)
	}
	activeDBFormat = format
	if srv.DefaultMaster != nil {
		srv.DefaultMaster.SetPurgeHook(purgeIfNeeded)
		_ = srv.DefaultMaster.Start()
	}
	if srv.DefaultPageCleaner != nil {
		_ = srv.DefaultPageCleaner.Start()
	}
	started = true
	return DB_SUCCESS
}

// Shutdown resets API state.
func Shutdown(_ ShutdownFlag) ErrCode {
	if srv.DefaultMaster != nil && srv.DefaultMaster.Running() {
		_ = srv.DefaultMaster.Stop()
	}
	if srv.DefaultPageCleaner != nil && srv.DefaultPageCleaner.Running() {
		_ = srv.DefaultPageCleaner.Stop()
	}
	if err := CfgShutdown(); err != DB_SUCCESS {
		return err
	}
	_ = buf.FlushAll()
	resetSchemaState()
	log.Shutdown()
	_ = trx.UndoStoreClose()
	fil.DoublewriteShutdown()
	closeSystemTablespace()
	buf.SetDefaultPools(nil)
	dict.DictClose()
	lock.SysClose()
	trx.PurgeSysClose()
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

package api

import (
	"sort"
	"strings"
	"sync"
)

// CfgType mirrors ib_cfg_type_t.
type CfgType int

const (
	CfgTypeBool CfgType = iota
	CfgTypeUlint
	CfgTypeUlong
	CfgTypeText
	CfgTypeCallback
)

// CfgFlag mirrors ib_cfg_flag_t.
type CfgFlag uint8

const (
	CfgFlagNone CfgFlag = 1 << iota
	CfgFlagReadOnlyAfterStartup
	CfgFlagReadOnly
)

// Callback mirrors ib_cb_t.
type Callback func()

// ConfigVar represents a configuration variable.
type ConfigVar struct {
	Name     string
	Type     CfgType
	Flag     CfgFlag
	MinValue uint64
	MaxValue uint64
	Value    any
}

var (
	cfgMu   sync.RWMutex
	cfgVars map[string]*ConfigVar
)

// CfgInit initializes the configuration registry.
func CfgInit() ErrCode {
	cfgMu.Lock()
	defer cfgMu.Unlock()
	cfgVars = map[string]*ConfigVar{}
	registerDefaults()
	return DB_SUCCESS
}

// CfgShutdown clears the configuration registry.
func CfgShutdown() ErrCode {
	cfgMu.Lock()
	defer cfgMu.Unlock()
	cfgVars = nil
	return DB_SUCCESS
}

// CfgVarGetType returns the type for a configuration variable.
func CfgVarGetType(name string) (CfgType, ErrCode) {
	cfgMu.RLock()
	defer cfgMu.RUnlock()
	cfgVar := cfgVars[keyName(name)]
	if cfgVar == nil {
		return 0, DB_NOT_FOUND
	}
	return cfgVar.Type, DB_SUCCESS
}

// CfgSet updates a configuration variable.
func CfgSet(name string, value any) ErrCode {
	cfgMu.Lock()
	defer cfgMu.Unlock()
	cfgVar := cfgVars[keyName(name)]
	if cfgVar == nil {
		return DB_NOT_FOUND
	}
	if cfgVar.Flag&CfgFlagReadOnly != 0 {
		return DB_READONLY
	}
	if started && cfgVar.Flag&CfgFlagReadOnlyAfterStartup != 0 {
		return DB_READONLY
	}
	assigned, err := assignConfigValue(cfgVar, value)
	if err != DB_SUCCESS {
		return err
	}
	if err := validateConfigValue(cfgVar.Name, assigned); err != DB_SUCCESS {
		return err
	}
	cfgVar.Value = assigned
	return DB_SUCCESS
}

// CfgGet retrieves a configuration variable into the provided pointer.
func CfgGet(name string, out any) ErrCode {
	cfgMu.RLock()
	defer cfgMu.RUnlock()
	cfgVar := cfgVars[keyName(name)]
	if cfgVar == nil {
		return DB_NOT_FOUND
	}
	return assignConfigOut(cfgVar, out)
}

// CfgGetAll returns all config variable names.
func CfgGetAll() ([]string, ErrCode) {
	cfgMu.RLock()
	defer cfgMu.RUnlock()
	names := make([]string, 0, len(cfgVars))
	for _, cfgVar := range cfgVars {
		names = append(names, cfgVar.Name)
	}
	sort.Strings(names)
	return names, DB_SUCCESS
}

func registerDefaults() {
	registerVar(&ConfigVar{
		Name:  "adaptive_hash_index",
		Type:  CfgTypeBool,
		Flag:  CfgFlagReadOnlyAfterStartup,
		Value: IBTrue,
	})
	registerVar(&ConfigVar{
		Name:  "additional_mem_pool_size",
		Type:  CfgTypeUlint,
		Flag:  CfgFlagNone,
		Value: Ulint(0),
	})
	registerVar(&ConfigVar{
		Name:  "autoextend_increment",
		Type:  CfgTypeUlint,
		Flag:  CfgFlagNone,
		Value: Ulint(64),
	})
	registerVar(&ConfigVar{
		Name:  "buffer_pool_size",
		Type:  CfgTypeUlong,
		Flag:  CfgFlagNone,
		Value: uint64(128 << 20),
	})
	registerVar(&ConfigVar{
		Name:  "checksums",
		Type:  CfgTypeBool,
		Flag:  CfgFlagNone,
		Value: IBTrue,
	})
	registerVar(&ConfigVar{
		Name:  "data_file_path",
		Type:  CfgTypeText,
		Flag:  CfgFlagReadOnlyAfterStartup,
		Value: "",
	})
	registerVar(&ConfigVar{
		Name:  "data_home_dir",
		Type:  CfgTypeText,
		Flag:  CfgFlagNone,
		Value: "",
	})
	registerVar(&ConfigVar{
		Name:  "doublewrite",
		Type:  CfgTypeBool,
		Flag:  CfgFlagNone,
		Value: IBTrue,
	})
	registerVar(&ConfigVar{
		Name:  "file_format",
		Type:  CfgTypeText,
		Flag:  CfgFlagNone,
		Value: "",
	})
	registerVar(&ConfigVar{
		Name:  "file_io_threads",
		Type:  CfgTypeUlint,
		Flag:  CfgFlagNone,
		Value: Ulint(4),
	})
	registerVar(&ConfigVar{
		Name:  "file_per_table",
		Type:  CfgTypeBool,
		Flag:  CfgFlagNone,
		Value: IBTrue,
	})
	registerVar(&ConfigVar{
		Name:  "flush_log_at_trx_commit",
		Type:  CfgTypeUlint,
		Flag:  CfgFlagNone,
		Value: Ulint(1),
	})
	registerVar(&ConfigVar{
		Name:  "flush_method",
		Type:  CfgTypeText,
		Flag:  CfgFlagNone,
		Value: "fsync",
	})
	registerVar(&ConfigVar{
		Name:  "force_recovery",
		Type:  CfgTypeUlint,
		Flag:  CfgFlagNone,
		Value: Ulint(0),
	})
	registerVar(&ConfigVar{
		Name:  "lock_wait_timeout",
		Type:  CfgTypeUlint,
		Flag:  CfgFlagNone,
		Value: Ulint(50),
	})
	registerVar(&ConfigVar{
		Name:  "log_buffer_size",
		Type:  CfgTypeUlint,
		Flag:  CfgFlagNone,
		Value: Ulint(8 << 20),
	})
	registerVar(&ConfigVar{
		Name:  "log_file_size",
		Type:  CfgTypeUlint,
		Flag:  CfgFlagNone,
		Value: Ulint(48 << 20),
	})
	registerVar(&ConfigVar{
		Name:  "log_files_in_group",
		Type:  CfgTypeUlint,
		Flag:  CfgFlagNone,
		Value: Ulint(2),
	})
	registerVar(&ConfigVar{
		Name:  "log_group_home_dir",
		Type:  CfgTypeText,
		Flag:  CfgFlagNone,
		Value: "",
	})
	registerVar(&ConfigVar{
		Name:  "max_dirty_pages_pct",
		Type:  CfgTypeUlint,
		Flag:  CfgFlagNone,
		Value: Ulint(75),
	})
	registerVar(&ConfigVar{
		Name:  "max_purge_lag",
		Type:  CfgTypeUlint,
		Flag:  CfgFlagNone,
		Value: Ulint(0),
	})
	registerVar(&ConfigVar{
		Name:     "lru_old_blocks_pct",
		Type:     CfgTypeUlint,
		Flag:     CfgFlagNone,
		MinValue: 5,
		MaxValue: 95,
		Value:    Ulint(37),
	})
	registerVar(&ConfigVar{
		Name:  "lru_block_access_recency",
		Type:  CfgTypeUlint,
		Flag:  CfgFlagNone,
		Value: Ulint(0),
	})
	registerVar(&ConfigVar{
		Name:  "open_files",
		Type:  CfgTypeUlint,
		Flag:  CfgFlagNone,
		Value: Ulint(0),
	})
	registerVar(&ConfigVar{
		Name:  "pre_rollback_hook",
		Type:  CfgTypeCallback,
		Flag:  CfgFlagNone,
		Value: Callback(nil),
	})
	registerVar(&ConfigVar{
		Name:  "print_verbose_log",
		Type:  CfgTypeBool,
		Flag:  CfgFlagNone,
		Value: IBFalse,
	})
	registerVar(&ConfigVar{
		Name:  "rollback_on_timeout",
		Type:  CfgTypeBool,
		Flag:  CfgFlagNone,
		Value: IBFalse,
	})
	registerVar(&ConfigVar{
		Name:  "stats_sample_pages",
		Type:  CfgTypeUlint,
		Flag:  CfgFlagNone,
		Value: Ulint(8),
	})
	registerVar(&ConfigVar{
		Name:  "status_file",
		Type:  CfgTypeText,
		Flag:  CfgFlagNone,
		Value: "",
	})
	registerVar(&ConfigVar{
		Name:  "sync_spin_loops",
		Type:  CfgTypeUlint,
		Flag:  CfgFlagNone,
		Value: Ulint(30),
	})
	registerVar(&ConfigVar{
		Name:  "version",
		Type:  CfgTypeText,
		Flag:  CfgFlagReadOnly,
		Value: "go-port",
	})
}

func validateConfigValue(name string, value any) ErrCode {
	switch keyName(name) {
	case "data_home_dir":
		s, ok := value.(string)
		if !ok {
			return DB_INVALID_INPUT
		}
		if s == "" || strings.HasSuffix(s, "/") {
			return DB_SUCCESS
		}
		return DB_INVALID_INPUT
	case "flush_method":
		s, ok := value.(string)
		if !ok {
			return DB_INVALID_INPUT
		}
		switch strings.ToLower(s) {
		case "fsync", "o_direct", "littlesync":
			return DB_SUCCESS
		default:
			return DB_INVALID_INPUT
		}
	default:
		return DB_SUCCESS
	}
}

func registerVar(cfgVar *ConfigVar) {
	if cfgVars == nil {
		cfgVars = map[string]*ConfigVar{}
	}
	cfgVars[keyName(cfgVar.Name)] = cfgVar
}

func keyName(name string) string {
	return strings.ToLower(strings.TrimSpace(name))
}

func assignConfigValue(cfgVar *ConfigVar, value any) (any, ErrCode) {
	switch cfgVar.Type {
	case CfgTypeBool:
		b, ok := toBool(value)
		if !ok {
			return nil, DB_INVALID_INPUT
		}
		if b {
			return IBTrue, DB_SUCCESS
		}
		return IBFalse, DB_SUCCESS
	case CfgTypeUlint:
		u, ok := toUint64(value)
		if !ok {
			return nil, DB_INVALID_INPUT
		}
		if !inRange(u, cfgVar.MinValue, cfgVar.MaxValue) {
			return nil, DB_INVALID_INPUT
		}
		return Ulint(u), DB_SUCCESS
	case CfgTypeUlong:
		u, ok := toUint64(value)
		if !ok {
			return nil, DB_INVALID_INPUT
		}
		if !inRange(u, cfgVar.MinValue, cfgVar.MaxValue) {
			return nil, DB_INVALID_INPUT
		}
		return u, DB_SUCCESS
	case CfgTypeText:
		s, ok := toString(value)
		if !ok {
			return nil, DB_INVALID_INPUT
		}
		return s, DB_SUCCESS
	case CfgTypeCallback:
		cb, ok := value.(Callback)
		if !ok {
			return nil, DB_INVALID_INPUT
		}
		return cb, DB_SUCCESS
	default:
		return nil, DB_ERROR
	}
}

func assignConfigOut(cfgVar *ConfigVar, out any) ErrCode {
	switch cfgVar.Type {
	case CfgTypeBool:
		switch ptr := out.(type) {
		case *Bool:
			*ptr = cfgVar.Value.(Bool)
		case *bool:
			*ptr = cfgVar.Value.(Bool) != 0
		default:
			return DB_INVALID_INPUT
		}
	case CfgTypeUlint:
		switch ptr := out.(type) {
		case *Ulint:
			*ptr = cfgVar.Value.(Ulint)
		case *uint64:
			*ptr = uint64(cfgVar.Value.(Ulint))
		default:
			return DB_INVALID_INPUT
		}
	case CfgTypeUlong:
		switch ptr := out.(type) {
		case *uint64:
			*ptr = cfgVar.Value.(uint64)
		default:
			return DB_INVALID_INPUT
		}
	case CfgTypeText:
		switch ptr := out.(type) {
		case *string:
			*ptr = cfgVar.Value.(string)
		default:
			return DB_INVALID_INPUT
		}
	case CfgTypeCallback:
		switch ptr := out.(type) {
		case *Callback:
			*ptr = cfgVar.Value.(Callback)
		default:
			return DB_INVALID_INPUT
		}
	default:
		return DB_ERROR
	}
	return DB_SUCCESS
}

func inRange(value, min, max uint64) bool {
	if min == 0 && max == 0 {
		return true
	}
	return min <= value && value <= max
}

func toBool(value any) (bool, bool) {
	switch v := value.(type) {
	case Bool:
		return v != 0, true
	case bool:
		return v, true
	case int:
		return v != 0, true
	case uint:
		return v != 0, true
	default:
		return false, false
	}
}

func toUint64(value any) (uint64, bool) {
	switch v := value.(type) {
	case Ulint:
		return uint64(v), true
	case uint64:
		return v, true
	case uint32:
		return uint64(v), true
	case uint:
		return uint64(v), true
	case int:
		if v < 0 {
			return 0, false
		}
		return uint64(v), true
	default:
		return 0, false
	}
}

func toString(value any) (string, bool) {
	switch v := value.(type) {
	case string:
		return v, true
	case []byte:
		return string(v), true
	default:
		return "", false
	}
}

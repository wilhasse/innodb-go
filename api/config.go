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
		Name:  "data_file_path",
		Type:  CfgTypeText,
		Flag:  CfgFlagReadOnlyAfterStartup,
		Value: "",
	})
	registerVar(&ConfigVar{
		Name:  "file_format",
		Type:  CfgTypeText,
		Flag:  CfgFlagNone,
		Value: "",
	})
	registerVar(&ConfigVar{
		Name:  "version",
		Type:  CfgTypeText,
		Flag:  CfgFlagReadOnly,
		Value: "go-port",
	})
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

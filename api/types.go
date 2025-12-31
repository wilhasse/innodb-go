package api

import "github.com/wilhasse/innodb-go/ut"

// Basic API types mirrored from api0api.h.
type (
	Byte  = uint8
	Ulint = ut.Ulint
	Bool  = ut.IBool
)

const (
	IBTrue  Bool = 1
	IBFalse Bool = 0
)

// SQL NULL marker and related limits.
const (
	IBSQLNull  = 0xFFFFFFFF
	IBNSysCols = 3
	MaxTextLen = 4096
)

// ShutdownFlag mirrors ib_shutdown_t.
type ShutdownFlag int

const (
	ShutdownNormal ShutdownFlag = iota
	ShutdownNoIbufMergePurge
	ShutdownNoBufpoolFlush
)

// Column metadata mirrors ib_col_meta_t.
type ColMeta struct {
	Type       ColType
	Attr       ColAttr
	TypeLen    uint32
	ClientType uint16
	Charset    *Charset
}

type ColType int
type ColAttr uint32
type Charset struct {
	ID   Ulint
	Name string
}

const (
	IB_COL_NONE      ColAttr = 0
	IB_COL_UNSIGNED  ColAttr = 1 << iota
	IB_COL_NOT_NULL
)

const (
	IB_VARCHAR            ColType = 1
	IB_CHAR               ColType = 2
	IB_BINARY             ColType = 3
	IB_VARBINARY          ColType = 4
	IB_BLOB               ColType = 5
	IB_INT                ColType = 6
	IB_SYS                ColType = 8
	IB_FLOAT              ColType = 9
	IB_DOUBLE             ColType = 10
	IB_DECIMAL            ColType = 11
	IB_VARCHAR_ANYCHARSET ColType = 12
	IB_CHAR_ANYCHARSET    ColType = 13
)

// ClientCompare mirrors ib_client_cmp_t.
type ClientCompare func(meta *ColMeta, p1 []byte, p2 []byte) int

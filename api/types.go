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
type Charset struct{}

// ClientCompare mirrors ib_client_cmp_t.
type ClientCompare func(meta *ColMeta, p1 []byte, p2 []byte) int

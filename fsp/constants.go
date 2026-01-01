package fsp

import "github.com/wilhasse/innodb-go/ut"

const (
	Up    byte = 111
	Down  byte = 112
	NoDir byte = 113
)

// ExtentSize mirrors FSP_EXTENT_SIZE.
const ExtentSize = 1 << (20 - ut.UnivPageSizeShift)

const (
	HeaderOffset      = 38
	SpaceIDOffset     = 0
	SizeOffset        = 8
	FreeLimitOffset   = 12
	SpaceFlagsOffset  = 16
	ExtentCountOffset = 20
	ExtentMapOffset   = 24
)

const extentBitmapBytes = (ExtentSize + 7) / 8

const nodeMetaReservedBytes = 1024
const nodeMetaOffset = ut.UNIV_PAGE_SIZE - nodeMetaReservedBytes

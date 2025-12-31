package btr

import "github.com/wilhasse/innodb-go/ut"

// BtrPageMaxRecSize mirrors BTR_PAGE_MAX_REC_SIZE.
const BtrPageMaxRecSize = ut.UNIV_PAGE_SIZE/2 - 200

// BtrMaxLevels mirrors BTR_MAX_LEVELS.
const BtrMaxLevels = 100

// LatchMode mirrors btr_latch_mode for call sites that expect it.
// Values are placeholders until latch modes are ported.
type LatchMode uint32

const (
	BtrSearchLeaf LatchMode = iota + 1
	BtrModifyLeaf
	BtrNoLatches
	BtrModifyTree
	BtrContModifyTree
	BtrSearchPrev
	BtrModifyPrev
)

const (
	BtrInsert          = 512
	BtrEstimate        = 1024
	BtrIgnoreSecUnique = 2048
)

package rec

// Record format constants mirrored from rem0rec.h.
const (
	RecInfoMinRecFlag  = 0x10
	RecInfoDeletedFlag = 0x20

	RecNOldExtraBytes = 6
	RecNNewExtraBytes = 5

	RecStatusOrdinary = 0
	RecStatusNodePtr  = 1
	RecStatusInfimum  = 2
	RecStatusSupremum = 3

	RecNewHeapNo      = 4
	RecHeapNoShift    = 3
	RecNodePtrSize    = 4
	RecOffsHeaderSize = 2
	RecOffsNormalSize = 100
	RecOffsSmallSize  = 10

	RecOffsCompact  = uint32(1 << 31)
	RecOffsSQLNull  = uint32(1 << 31)
	RecOffsExternal = uint32(1 << 30)
	RecOffsMask     = RecOffsExternal - 1
)

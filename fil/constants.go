package fil

// IbdFileInitialSize mirrors FIL_IBD_FILE_INITIAL_SIZE.
const IbdFileInitialSize = 4

// NullPageOffset mirrors FIL_NULL.
const NullPageOffset uint32 = ^uint32(0)

// Addr represents a file space address.
type Addr struct {
	Page   uint32
	Offset uint32
}

// AddrNull is an undefined file space address.
var AddrNull = Addr{Page: NullPageOffset, Offset: 0}

const (
	PageSpaceOrChecksum    uint32 = 0
	PageOffset             uint32 = 4
	PagePrev               uint32 = 8
	PageNext               uint32 = 12
	PageLSN                uint32 = 16
	PageType               uint32 = 24
	PageFileFlushLSN       uint32 = 26
	PageArchLogNoOrSpaceID uint32 = 34
	PageData               uint32 = 38
	PageEndLsnOldChecksum  uint32 = 8
	PageDataEnd            uint32 = 8
)

const (
	PageTypeIndex        uint16 = 17855
	PageTypeUndoLog      uint16 = 2
	PageTypeInode        uint16 = 3
	PageTypeIbufFreeList uint16 = 4
	PageTypeAllocated    uint16 = 0
	PageTypeIbufBitmap   uint16 = 5
	PageTypeSys          uint16 = 6
	PageTypeTrxSys       uint16 = 7
	PageTypeFspHdr       uint16 = 8
	PageTypeXdes         uint16 = 9
	PageTypeBlob         uint16 = 10
	PageTypeZBlob        uint16 = 11
	PageTypeZBlob2       uint16 = 12
)

const (
	SpaceTablespace uint32 = 501
	SpaceLog        uint32 = 502
)

package rec

import "encoding/binary"

const (
	headerInfoBitsMask = 0xF0
	headerNOwnedMask   = 0x0F
)

// HeaderInfoBits returns the info bits from a record header.
func HeaderInfoBits(rec []byte) byte {
	if len(rec) < 1 {
		return 0
	}
	return rec[0] & headerInfoBitsMask
}

// HeaderSetInfoBits sets the info bits in a record header.
func HeaderSetInfoBits(rec []byte, bits byte) {
	if len(rec) < 1 {
		return
	}
	rec[0] = (rec[0] & headerNOwnedMask) | (bits & headerInfoBitsMask)
}

// HeaderNOwned returns the n_owned value from a record header.
func HeaderNOwned(rec []byte) byte {
	if len(rec) < 1 {
		return 0
	}
	return rec[0] & headerNOwnedMask
}

// HeaderSetNOwned sets the n_owned value in a record header.
func HeaderSetNOwned(rec []byte, nOwned byte) {
	if len(rec) < 1 {
		return
	}
	rec[0] = (rec[0] & headerInfoBitsMask) | (nOwned & headerNOwnedMask)
}

// HeaderStatus returns the record status bits from the header.
func HeaderStatus(rec []byte) uint16 {
	if len(rec) < 3 {
		return 0
	}
	val := binary.BigEndian.Uint16(rec[1:3])
	return val & ((1 << RecHeapNoShift) - 1)
}

// HeaderSetStatus sets the record status bits in the header.
func HeaderSetStatus(rec []byte, status uint16) {
	if len(rec) < 3 {
		return
	}
	val := binary.BigEndian.Uint16(rec[1:3])
	mask := uint16((1 << RecHeapNoShift) - 1)
	val = (val &^ mask) | (status & mask)
	binary.BigEndian.PutUint16(rec[1:3], val)
}

// HeaderHeapNo returns the heap number from a record header.
func HeaderHeapNo(rec []byte) uint16 {
	if len(rec) < 3 {
		return 0
	}
	val := binary.BigEndian.Uint16(rec[1:3])
	return val >> RecHeapNoShift
}

// HeaderSetHeapNo sets the heap number in a record header.
func HeaderSetHeapNo(rec []byte, heapNo uint16) {
	if len(rec) < 3 {
		return
	}
	val := binary.BigEndian.Uint16(rec[1:3])
	mask := uint16((1 << RecHeapNoShift) - 1)
	val = (heapNo << RecHeapNoShift) | (val & mask)
	binary.BigEndian.PutUint16(rec[1:3], val)
}

package rec

import (
	"encoding/binary"
	"testing"
)

func TestHeaderBits(t *testing.T) {
	rec := make([]byte, RecNNewExtraBytes)

	HeaderSetNOwned(rec, 5)
	HeaderSetInfoBits(rec, RecInfoMinRecFlag|RecInfoDeletedFlag)

	if got := HeaderNOwned(rec); got != 5 {
		t.Fatalf("n_owned=%d", got)
	}
	if got := HeaderInfoBits(rec); got != (RecInfoMinRecFlag | RecInfoDeletedFlag) {
		t.Fatalf("info_bits=0x%x", got)
	}

	HeaderSetStatus(rec, RecStatusSupremum)
	HeaderSetHeapNo(rec, 123)

	if got := HeaderStatus(rec); got != RecStatusSupremum {
		t.Fatalf("status=%d", got)
	}
	if got := HeaderHeapNo(rec); got != 123 {
		t.Fatalf("heap_no=%d", got)
	}

	wantCombined := uint16(123<<RecHeapNoShift) | uint16(RecStatusSupremum)
	if got := binary.BigEndian.Uint16(rec[1:3]); got != wantCombined {
		t.Fatalf("header=0x%x want=0x%x", got, wantCombined)
	}
}

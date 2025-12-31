package fut

import (
	"testing"

	"github.com/wilhasse/innodb-go/buf"
	"github.com/wilhasse/innodb-go/fil"
	"github.com/wilhasse/innodb-go/ut"
)

func TestGetPtr(t *testing.T) {
	pool := buf.NewPool(1, ut.UnivPageSize)
	SetDefaultPool(pool)

	addr := fil.Addr{Page: 3, Offset: 12}
	ptr, page, err := GetPtr(1, 0, addr, RWShared)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	ptr[0] = 99
	if page.Data[12] != 99 {
		t.Fatalf("expected page data to be updated")
	}
	ReleasePage(page)
}

func TestGetPtrOutOfBounds(t *testing.T) {
	pool := buf.NewPool(1, ut.UnivPageSize)
	SetDefaultPool(pool)

	addr := fil.Addr{Page: 3, Offset: uint32(ut.UnivPageSize)}
	_, _, err := GetPtr(1, 0, addr, RWShared)
	if err != ErrAddrOutOfBounds {
		t.Fatalf("expected out of bounds error, got %v", err)
	}
}

func TestGetPtrNoPool(t *testing.T) {
	SetDefaultPool(nil)
	addr := fil.Addr{Page: 1, Offset: 0}
	_, _, err := GetPtr(1, 0, addr, RWShared)
	if err != ErrNoBufferPool {
		t.Fatalf("expected no buffer pool error, got %v", err)
	}
}

package fut

import (
	"errors"

	"github.com/wilhasse/innodb-go/buf"
	"github.com/wilhasse/innodb-go/fil"
)

// RW latch modes for GetPtr.
const (
	RWShared = iota + 1
	RWExclusive
)

var (
	ErrNoBufferPool    = errors.New("fut: no buffer pool")
	ErrAddrOutOfBounds = errors.New("fut: address out of bounds")
)

// SetDefaultPool configures the default buffer pool used by GetPtr.
func SetDefaultPool(pool *buf.Pool) {
	buf.SetDefaultPool(pool)
}

// GetPtr returns a byte slice starting at the file address offset.
func GetPtr(space uint32, _ uint32, addr fil.Addr, _ int) ([]byte, *buf.Page, error) {
	pool := buf.GetPool(space, addr.Page)
	if pool == nil {
		return nil, nil, ErrNoBufferPool
	}
	page, _, err := pool.Fetch(space, addr.Page)
	if err != nil {
		return nil, nil, err
	}
	if int(addr.Offset) >= len(page.Data) {
		pool.Release(page)
		return nil, nil, ErrAddrOutOfBounds
	}
	return page.Data[addr.Offset:], page, nil
}

// ReleasePage releases a page fetched via GetPtr.
func ReleasePage(page *buf.Page) {
	if page == nil {
		return
	}
	pool := buf.GetPool(page.ID.Space, page.ID.PageNo)
	if pool == nil {
		return
	}
	pool.Release(page)
}

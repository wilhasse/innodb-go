package btr

import (
	"errors"

	"github.com/wilhasse/innodb-go/buf"
)

// ErrNoBufferPool reports missing default buffer pool.
var ErrNoBufferPool = errors.New("btr: no buffer pool")

// PageFetch returns the page bytes from the buffer pool.
func PageFetch(spaceID, pageNo uint32) ([]byte, *buf.Page, error) {
	pool := buf.GetPool(spaceID, pageNo)
	if pool == nil {
		return nil, nil, ErrNoBufferPool
	}
	page, _, err := pool.Fetch(spaceID, pageNo)
	if err != nil {
		return nil, nil, err
	}
	return page.Data, page, nil
}

// PageRelease releases a page fetched with PageFetch.
func PageRelease(page *buf.Page) {
	pool := buf.GetPool(page.ID.Space, page.ID.PageNo)
	if pool == nil {
		return
	}
	pool.Release(page)
}

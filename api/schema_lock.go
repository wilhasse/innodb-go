package api

import (
	"sync"

	"github.com/wilhasse/innodb-go/trx"
)

var (
	schemaLockMu sync.Mutex
	schemaLocks  = map[*trx.Trx]bool{}
)

func lockSchema(trx *trx.Trx) {
	if trx == nil {
		return
	}
	schemaLockMu.Lock()
	schemaLocks[trx] = true
	schemaLockMu.Unlock()
}

func isSchemaLocked(trx *trx.Trx) bool {
	if trx == nil {
		return false
	}
	schemaLockMu.Lock()
	locked := schemaLocks[trx]
	schemaLockMu.Unlock()
	return locked
}

func clearSchemaLock(trx *trx.Trx) {
	if trx == nil {
		return
	}
	schemaLockMu.Lock()
	delete(schemaLocks, trx)
	schemaLockMu.Unlock()
}

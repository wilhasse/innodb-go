package ha

import "sync"

// HashCreate creates a hash table with at least n buckets.
func HashCreate(n int) *HashTable {
	return Create(n)
}

// HashTableFree releases a hash table.
func HashTableFree(_ *HashTable) {
	// Go GC handles memory reclamation.
}

// HashTableClear clears a hash table.
func HashTableClear(table *HashTable) {
	if table == nil {
		return
	}
	table.Clear()
}

// HashCreateMutexes allocates mutexes for a hash table.
func HashCreateMutexes(table *HashTable, n int) {
	if table == nil || n <= 0 {
		return
	}
	table.mutexes = make([]sync.Mutex, n)
}

// HashFreeMutexes releases mutexes for a hash table.
func HashFreeMutexes(table *HashTable) {
	if table == nil {
		return
	}
	table.mutexes = nil
}

// HashMutexEnter locks the mutex guarding a fold.
func HashMutexEnter(table *HashTable, fold uint64) {
	mutex := hashMutex(table, fold)
	if mutex == nil {
		return
	}
	mutex.Lock()
}

// HashMutexExit unlocks the mutex guarding a fold.
func HashMutexExit(table *HashTable, fold uint64) {
	mutex := hashMutex(table, fold)
	if mutex == nil {
		return
	}
	mutex.Unlock()
}

// HashMutexEnterAll locks all mutexes in order.
func HashMutexEnterAll(table *HashTable) {
	if table == nil {
		return
	}
	for i := range table.mutexes {
		table.mutexes[i].Lock()
	}
}

// HashMutexExitAll unlocks all mutexes in order.
func HashMutexExitAll(table *HashTable) {
	if table == nil {
		return
	}
	for i := range table.mutexes {
		table.mutexes[i].Unlock()
	}
}

// HashGetNCells returns the bucket count.
func HashGetNCells(table *HashTable) int {
	if table == nil {
		return 0
	}
	return len(table.buckets)
}

// HashCalcHash returns the bucket index for a fold.
func HashCalcHash(fold uint64, table *HashTable) int {
	if table == nil || len(table.buckets) == 0 {
		return 0
	}
	return table.bucketIndex(fold)
}

func hashMutex(table *HashTable, fold uint64) *sync.Mutex {
	if table == nil || len(table.mutexes) == 0 {
		return nil
	}
	idx := int(fold % uint64(len(table.mutexes)))
	return &table.mutexes[idx]
}

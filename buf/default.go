package buf

var defaultPools []*Pool

// SetDefaultPool assigns a single default buffer pool used by helpers.
func SetDefaultPool(pool *Pool) {
	if pool == nil {
		defaultPools = nil
		return
	}
	defaultPools = []*Pool{pool}
}

// SetDefaultPools assigns multiple buffer pool instances.
func SetDefaultPools(pools []*Pool) {
	if len(pools) == 0 {
		defaultPools = nil
		return
	}
	defaultPools = pools
}

// DefaultPools returns the configured pool instances.
func DefaultPools() []*Pool {
	return defaultPools
}

// DefaultPoolCount returns the number of configured pool instances.
func DefaultPoolCount() int {
	return len(defaultPools)
}

// GetDefaultPool returns the first configured buffer pool instance.
func GetDefaultPool() *Pool {
	if len(defaultPools) == 0 {
		return nil
	}
	return defaultPools[0]
}

// GetPool returns the owning pool instance for a given page.
func GetPool(space, pageNo uint32) *Pool {
	if len(defaultPools) == 0 {
		return nil
	}
	if len(defaultPools) == 1 {
		return defaultPools[0]
	}
	idx := poolIndex(space, pageNo, len(defaultPools))
	return defaultPools[idx]
}

// FlushAll flushes all pool instances and returns the number of flushed pages.
func FlushAll() int {
	flushed := 0
	for _, pool := range defaultPools {
		if pool == nil {
			continue
		}
		flushed += pool.Flush()
	}
	return flushed
}

func poolIndex(space, pageNo uint32, count int) int {
	if count <= 1 {
		return 0
	}
	hash := (uint64(space) << 32) | uint64(pageNo)
	return int(hash % uint64(count))
}

package buf

var defaultPool *Pool

// SetDefaultPool assigns the default buffer pool used by helpers.
func SetDefaultPool(pool *Pool) {
	defaultPool = pool
}

// GetDefaultPool returns the configured default buffer pool.
func GetDefaultPool() *Pool {
	return defaultPool
}

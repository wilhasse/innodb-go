package srv

import (
	"sync"
	"time"

	"github.com/wilhasse/innodb-go/buf"
)

// PageCleanerConfig configures the background page cleaner.
type PageCleanerConfig struct {
	Interval    time.Duration
	WorkerCount int
	FlushLimit  int
	FlushFn     func(workerID int) int
}

// PageCleaner runs background flush workers for dirty pages.
type PageCleaner struct {
	mu          sync.Mutex
	running     bool
	stopCh      chan struct{}
	doneCh      chan struct{}
	interval    time.Duration
	workerCount int
	flushLimit  int
	flushFn     func(workerID int) int
}

const defaultCleanerInterval = 200 * time.Millisecond

// DefaultPageCleaner is the global page cleaner instance.
var DefaultPageCleaner = NewPageCleaner(PageCleanerConfig{
	Interval:    defaultCleanerInterval,
	WorkerCount: 1,
	FlushLimit:  0,
})

// NewPageCleaner constructs a page cleaner with the given config.
func NewPageCleaner(cfg PageCleanerConfig) *PageCleaner {
	interval := cfg.Interval
	if interval <= 0 {
		interval = defaultCleanerInterval
	}
	workers := cfg.WorkerCount
	if workers < 1 {
		workers = 1
	}
	return &PageCleaner{
		interval:    interval,
		workerCount: workers,
		flushLimit:  cfg.FlushLimit,
		flushFn:     cfg.FlushFn,
	}
}

// SetConfig updates cleaner settings and restarts if running.
func (c *PageCleaner) SetConfig(interval time.Duration, workers, limit int) {
	if c == nil {
		return
	}
	if interval <= 0 {
		interval = defaultCleanerInterval
	}
	if workers < 1 {
		workers = 1
	}
	c.mu.Lock()
	c.interval = interval
	c.workerCount = workers
	c.flushLimit = limit
	wasRunning := c.running
	c.mu.Unlock()
	if wasRunning {
		_ = c.Stop()
		_ = c.Start()
	}
}

// SetFlushHook updates the flush hook and restarts if running.
func (c *PageCleaner) SetFlushHook(flushFn func(workerID int) int) {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.flushFn = flushFn
	wasRunning := c.running
	c.mu.Unlock()
	if wasRunning {
		_ = c.Stop()
		_ = c.Start()
	}
}

// Start begins the background flush workers.
func (c *PageCleaner) Start() error {
	if c == nil {
		return ErrNotRunning
	}
	c.mu.Lock()
	if c.running {
		c.mu.Unlock()
		return ErrAlreadyRunning
	}
	c.running = true
	c.stopCh = make(chan struct{})
	c.doneCh = make(chan struct{})
	stop := c.stopCh
	done := c.doneCh
	interval := c.interval
	workers := c.workerCount
	flushLimit := c.flushLimit
	flushFn := c.flushFn
	c.mu.Unlock()

	if workers < 1 {
		workers = 1
	}
	if interval <= 0 {
		interval = defaultCleanerInterval
	}

	go func() {
		var wg sync.WaitGroup
		wg.Add(workers)
		for workerID := 0; workerID < workers; workerID++ {
			id := workerID
			go func() {
				defer wg.Done()
				ticker := time.NewTicker(interval)
				defer ticker.Stop()
				for {
					select {
					case <-stop:
						return
					case <-ticker.C:
						if flushFn != nil {
							flushFn(id)
						} else {
							flushCleanerPools(id, workers, flushLimit)
						}
					}
				}
			}()
		}
		wg.Wait()
		close(done)
	}()
	return nil
}

// Stop halts the background flush workers.
func (c *PageCleaner) Stop() error {
	if c == nil {
		return ErrNotRunning
	}
	c.mu.Lock()
	if !c.running {
		c.mu.Unlock()
		return ErrNotRunning
	}
	stop := c.stopCh
	done := c.doneCh
	c.running = false
	c.stopCh = nil
	c.doneCh = nil
	c.mu.Unlock()

	close(stop)
	if done != nil {
		<-done
	}
	return nil
}

// Running reports whether the cleaner is active.
func (c *PageCleaner) Running() bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.running
}

func flushCleanerPools(workerID, workerCount, limit int) int {
	pools := buf.DefaultPools()
	if len(pools) == 0 {
		return 0
	}
	if workerCount < 1 {
		workerCount = 1
	}
	total := 0
	for idx, pool := range pools {
		if pool == nil {
			continue
		}
		if workerCount > 1 && idx%workerCount != workerID {
			continue
		}
		if limit <= 0 {
			total += pool.FlushList(0)
		} else {
			total += pool.FlushList(limit)
		}
	}
	return total
}

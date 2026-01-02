package srv

import (
	"sync"
	"time"
)

// MasterConfig configures the master scheduler intervals and hooks.
type MasterConfig struct {
	PurgeInterval time.Duration
	FlushInterval time.Duration
	StatsInterval time.Duration
	PurgeFn       func()
	FlushFn       func()
	StatsFn       func()
}

// MasterScheduler runs periodic background tasks.
type MasterScheduler struct {
	mu            sync.Mutex
	running       bool
	stopCh        chan struct{}
	doneCh        chan struct{}
	purgeInterval time.Duration
	flushInterval time.Duration
	statsInterval time.Duration
	purgeFn       func()
	flushFn       func()
	statsFn       func()
}

// DefaultMaster is the global master scheduler.
var DefaultMaster = NewMasterScheduler(MasterConfig{
	PurgeInterval: 100 * time.Millisecond,
	FlushInterval: time.Second,
	StatsInterval: time.Second,
	FlushFn:       func() { AdaptiveFlush(0) },
	StatsFn:       ExportInnoDBStatus,
})

// NewMasterScheduler constructs a scheduler from config.
func NewMasterScheduler(cfg MasterConfig) *MasterScheduler {
	return &MasterScheduler{
		purgeInterval: cfg.PurgeInterval,
		flushInterval: cfg.FlushInterval,
		statsInterval: cfg.StatsInterval,
		purgeFn:       cfg.PurgeFn,
		flushFn:       cfg.FlushFn,
		statsFn:       cfg.StatsFn,
	}
}

// SetIntervals updates task intervals and restarts if running.
func (m *MasterScheduler) SetIntervals(purge, flush, stats time.Duration) {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.purgeInterval = purge
	m.flushInterval = flush
	m.statsInterval = stats
	wasRunning := m.running
	m.mu.Unlock()
	if wasRunning {
		_ = m.Stop()
		_ = m.Start()
	}
}

// SetHooks updates the task hooks and restarts if running.
func (m *MasterScheduler) SetHooks(purgeFn, flushFn, statsFn func()) {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.purgeFn = purgeFn
	m.flushFn = flushFn
	m.statsFn = statsFn
	m.mu.Unlock()
}

// SetPurgeHook updates only the purge task hook.
func (m *MasterScheduler) SetPurgeHook(purgeFn func()) {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.purgeFn = purgeFn
	m.mu.Unlock()
}

// Start begins scheduling.
func (m *MasterScheduler) Start() error {
	if m == nil {
		return ErrNotRunning
	}
	m.mu.Lock()
	if m.running {
		m.mu.Unlock()
		return ErrAlreadyRunning
	}
	m.running = true
	m.stopCh = make(chan struct{})
	m.doneCh = make(chan struct{})
	stop := m.stopCh
	done := m.doneCh
	purgeInterval := m.purgeInterval
	flushInterval := m.flushInterval
	statsInterval := m.statsInterval
	purgeFn := m.purgeFn
	flushFn := m.flushFn
	statsFn := m.statsFn
	m.mu.Unlock()

	go func() {
		defer close(done)
		var purgeTicker *time.Ticker
		var flushTicker *time.Ticker
		var statsTicker *time.Ticker
		var purgeC <-chan time.Time
		var flushC <-chan time.Time
		var statsC <-chan time.Time
		if purgeInterval > 0 {
			purgeTicker = time.NewTicker(purgeInterval)
			purgeC = purgeTicker.C
		}
		if flushInterval > 0 {
			flushTicker = time.NewTicker(flushInterval)
			flushC = flushTicker.C
		}
		if statsInterval > 0 {
			statsTicker = time.NewTicker(statsInterval)
			statsC = statsTicker.C
		}
		defer func() {
			if purgeTicker != nil {
				purgeTicker.Stop()
			}
			if flushTicker != nil {
				flushTicker.Stop()
			}
			if statsTicker != nil {
				statsTicker.Stop()
			}
		}()
		for {
			select {
			case <-stop:
				return
			case <-purgeC:
				if purgeFn != nil {
					go purgeFn()
				}
			case <-flushC:
				if flushFn != nil {
					go flushFn()
				}
			case <-statsC:
				if statsFn != nil {
					go statsFn()
				}
			}
		}
	}()
	return nil
}

// Stop halts scheduling.
func (m *MasterScheduler) Stop() error {
	if m == nil {
		return ErrNotRunning
	}
	m.mu.Lock()
	if !m.running {
		m.mu.Unlock()
		return ErrNotRunning
	}
	stop := m.stopCh
	done := m.doneCh
	m.running = false
	m.stopCh = nil
	m.doneCh = nil
	m.mu.Unlock()

	close(stop)
	if done != nil {
		<-done
	}
	return nil
}

// Running reports whether the scheduler is active.
func (m *MasterScheduler) Running() bool {
	if m == nil {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.running
}

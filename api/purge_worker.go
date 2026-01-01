package api

import (
	"sync"
	"time"
)

const purgeWorkerInterval = 50 * time.Millisecond

var (
	purgeWorkerMu      sync.Mutex
	purgeWorkerStop    chan struct{}
	purgeWorkerDone    chan struct{}
	purgeWorkerRunning bool
)

func startPurgeWorker() {
	purgeWorkerMu.Lock()
	if purgeWorkerRunning {
		purgeWorkerMu.Unlock()
		return
	}
	stop := make(chan struct{})
	done := make(chan struct{})
	purgeWorkerStop = stop
	purgeWorkerDone = done
	purgeWorkerRunning = true
	purgeWorkerMu.Unlock()

	go func() {
		ticker := time.NewTicker(purgeWorkerInterval)
		defer ticker.Stop()
		defer close(done)
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				purgeIfNeeded()
			}
		}
	}()
}

func stopPurgeWorker() {
	purgeWorkerMu.Lock()
	if !purgeWorkerRunning {
		purgeWorkerMu.Unlock()
		return
	}
	stop := purgeWorkerStop
	done := purgeWorkerDone
	purgeWorkerStop = nil
	purgeWorkerDone = nil
	purgeWorkerRunning = false
	purgeWorkerMu.Unlock()

	close(stop)
	<-done
}

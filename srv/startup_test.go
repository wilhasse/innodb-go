package srv

import "testing"

func TestStartupShutdown(t *testing.T) {
	DefaultServer = NewServer()
	if IsStarted() {
		t.Fatalf("expected stopped")
	}
	if err := Startup(); err != nil {
		t.Fatalf("startup: %v", err)
	}
	if !IsStarted() {
		t.Fatalf("expected running")
	}
	if err := Startup(); err != ErrAlreadyRunning {
		t.Fatalf("expected already running, got %v", err)
	}
	if err := Shutdown(); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	if IsStarted() {
		t.Fatalf("expected stopped")
	}
	if err := Shutdown(); err != ErrNotRunning {
		t.Fatalf("expected not running, got %v", err)
	}
}

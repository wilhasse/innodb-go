package srv

import "testing"

func TestServerLifecycle(t *testing.T) {
	srv := NewServer()
	if srv.IsRunning() {
		t.Fatalf("expected stopped")
	}
	if err := srv.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	if !srv.IsRunning() {
		t.Fatalf("expected running")
	}
	if err := srv.Start(); err != ErrAlreadyRunning {
		t.Fatalf("expected already running, got %v", err)
	}
	if err := srv.Stop(); err != nil {
		t.Fatalf("stop: %v", err)
	}
	if srv.IsRunning() {
		t.Fatalf("expected stopped")
	}
	if err := srv.Stop(); err != ErrNotRunning {
		t.Fatalf("expected not running, got %v", err)
	}
	if srv.StartCount != 1 || srv.StopCount != 1 {
		t.Fatalf("counts start=%d stop=%d", srv.StartCount, srv.StopCount)
	}
}

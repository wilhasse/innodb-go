package tests

import (
	"testing"

	"github.com/wilhasse/innodb-go/api"
)

func TestLoggerHarness(t *testing.T) {
	resetAPI(t)
	if err := api.Init(); err != api.DB_SUCCESS {
		t.Fatalf("Init: %v", err)
	}
	calls := 0
	api.LoggerSet(func(_ api.Stream, _ string, _ ...any) int {
		calls++
		return 0
	}, nil)

	if err := api.Startup("barracuda"); err != api.DB_SUCCESS {
		t.Fatalf("Startup: %v", err)
	}
	if err := api.Shutdown(api.ShutdownNormal); err != api.DB_SUCCESS {
		t.Fatalf("Shutdown: %v", err)
	}
	if calls != 0 {
		t.Fatalf("expected no logger calls, got %d", calls)
	}
}

package api

import (
	"bytes"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/wilhasse/innodb-go/srv"
)

var (
	testDataMu   sync.Mutex
	testDataDirs []string
)

func resetAPIState() {
	stopPurgeWorker()
	if srv.DefaultMaster != nil && srv.DefaultMaster.Running() {
		_ = srv.DefaultMaster.Stop()
	}
	if srv.DefaultPageCleaner != nil && srv.DefaultPageCleaner.Running() {
		_ = srv.DefaultPageCleaner.Stop()
	}
	initialized = false
	started = false
	activeDBFormat = ""
	clientComparator = DefaultCompare
	Logger = DefaultLogger
	LogStream = os.Stderr
	setTestDataHomeDir()
}

func setTestDataHomeDir() {
	dir, err := os.MkdirTemp("", "innodb-go-api-")
	if err != nil {
		return
	}
	_ = os.Setenv("INNODB_DATA_HOME_DIR", dir)
	testDataMu.Lock()
	testDataDirs = append(testDataDirs, dir)
	testDataMu.Unlock()
}

func TestMain(m *testing.M) {
	code := m.Run()
	testDataMu.Lock()
	dirs := append([]string(nil), testDataDirs...)
	testDataMu.Unlock()
	for _, dir := range dirs {
		_ = os.RemoveAll(dir)
	}
	os.Exit(code)
}

func TestAPIVersion(t *testing.T) {
	want := (uint64(apiVersionCurrent) << 32) |
		(uint64(apiVersionRevision) << 16) |
		uint64(apiVersionAge)
	if got := APIVersion(); got != want {
		t.Fatalf("APIVersion()=%d, want %d", got, want)
	}
}

func TestInitStartupShutdown(t *testing.T) {
	resetAPIState()

	if got := Shutdown(ShutdownNormal); got != DB_SUCCESS {
		t.Fatalf("Shutdown before Init got %v, want %v", got, DB_SUCCESS)
	}
	if got := Init(); got != DB_SUCCESS {
		t.Fatalf("Init got %v, want %v", got, DB_SUCCESS)
	}
	if got := Startup(""); got != DB_SUCCESS {
		t.Fatalf("Startup got %v, want %v", got, DB_SUCCESS)
	}
	if got := Shutdown(ShutdownNormal); got != DB_SUCCESS {
		t.Fatalf("Shutdown got %v, want %v", got, DB_SUCCESS)
	}
}

func TestStartupUnknownFormat(t *testing.T) {
	resetAPIState()
	if got := Init(); got != DB_SUCCESS {
		t.Fatalf("Init got %v, want %v", got, DB_SUCCESS)
	}

	var buf bytes.Buffer
	LoggerSet(func(_ Stream, format string, args ...any) int {
		n, _ := fmt.Fprintf(&buf, format, args...)
		return n
	}, &buf)

	if got := Startup("unknown-format"); got != DB_UNSUPPORTED {
		t.Fatalf("Startup got %v, want %v", got, DB_UNSUPPORTED)
	}
	if !strings.Contains(buf.String(), "unknown-format") {
		t.Fatalf("expected log to mention unknown format, got %q", buf.String())
	}
}

func TestSetClientCompare(t *testing.T) {
	resetAPIState()

	SetClientCompare(nil)
	if ClientCompareFunc() == nil {
		t.Fatal("expected DefaultCompare when setting nil")
	}
}

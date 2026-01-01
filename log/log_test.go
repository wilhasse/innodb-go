package log

import "testing"

func TestReserveAndWriteFast(t *testing.T) {
	Init()
	end, start := ReserveAndWriteFast([]byte("abc"))
	if start != 0 {
		t.Fatalf("expected start 0, got %d", start)
	}
	if end != 3 {
		t.Fatalf("expected end 3, got %d", end)
	}
	entries := Entries()
	if len(entries) != 1 || string(entries[0].Data) != "abc" {
		t.Fatalf("expected entry to be stored")
	}
}

func TestReserveOpenWriteClose(t *testing.T) {
	Init()
	start := ReserveAndOpen(8)
	if start != 0 {
		t.Fatalf("expected start 0, got %d", start)
	}
	WriteLow([]byte("hello"))
	WriteLow([]byte("!"))
	end := Close()
	if end != 6 {
		t.Fatalf("expected end 6, got %d", end)
	}
	entries := Entries()
	if len(entries) != 1 || string(entries[0].Data) != "hello!" {
		t.Fatalf("expected combined entry")
	}
}

func TestFlushUpTo(t *testing.T) {
	Init()
	ReserveAndWriteFast([]byte("abcd"))
	ReserveAndWriteFast([]byte("ef"))
	if flushed := FlushUpTo(3); flushed < 3 {
		t.Fatalf("expected flushed >= 3, got %d", flushed)
	}
	if flushed := FlushUpTo(10); flushed != 6 {
		t.Fatalf("expected flushed capped at 6, got %d", flushed)
	}
}

func TestLogFlushMetrics(t *testing.T) {
	Init()
	ReserveAndWriteFast([]byte("xyz"))
	if flushed := FlushUpTo(3); flushed != 3 {
		t.Fatalf("expected flushed 3, got %d", flushed)
	}
	if NLogFlushes == 0 {
		t.Fatalf("expected log flushes to increment")
	}
	if NPendingLogFlushes != 0 {
		t.Fatalf("expected pending flushes to clear")
	}
}

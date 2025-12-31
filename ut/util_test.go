package ut

import (
	"math/bits"
	"strings"
	"testing"
)

func TestGetHigh32(t *testing.T) {
	val := Ulint(uint64(0x11223344)<<32 | 0x55667788)
	got := GetHigh32(val)
	if bits.UintSize == 32 {
		if got != 0 {
			t.Fatalf("expected 0 on 32-bit, got %d", got)
		}
	} else if got != Ulint(0x11223344) {
		t.Fatalf("high=%x", got)
	}
}

func TestTimeHelpers(t *testing.T) {
	now := Time()
	if now == 0 {
		t.Fatalf("expected time")
	}
	sec, usec := UsecTime()
	if sec == 0 || usec >= 1_000_000 {
		t.Fatalf("sec=%d usec=%d", sec, usec)
	}
	var stored uint64
	us := TimeUs(&stored)
	if us == 0 || stored != us {
		t.Fatalf("us=%d stored=%d", us, stored)
	}
	if TimeMs() == 0 {
		t.Fatalf("expected ms")
	}
	if DiffTime(now+2, now) != 2 {
		t.Fatalf("diff mismatch")
	}
}

func TestTimestampAndPower(t *testing.T) {
	ts := TimestampString()
	if len(ts) < 13 || !strings.Contains(ts, " ") {
		t.Fatalf("timestamp=%q", ts)
	}
	if PowerUp(1) != 1 || PowerUp(3) != 4 {
		t.Fatalf("power up mismatch")
	}
}

func TestPrintBufAndQuote(t *testing.T) {
	formatted := PrintBuf([]byte{0x41, 0x42})
	if !strings.Contains(formatted, "hex 4142") || !strings.Contains(formatted, "asc AB") {
		t.Fatalf("formatted=%q", formatted)
	}
	if QuoteFilename("ab'c") != "'ab''c'" {
		t.Fatalf("quote=%q", QuoteFilename("ab'c"))
	}
}

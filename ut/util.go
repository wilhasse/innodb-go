package ut

import (
	"fmt"
	"strings"
	"time"
)

// GetHigh32 returns the high 32 bits of a ulint.
func GetHigh32(a Ulint) Ulint {
	return Ulint(uint64(a) >> 32)
}

// Time returns the current Unix time in seconds.
func Time() int64 {
	return time.Now().Unix()
}

// UsecTime returns seconds and microseconds since the epoch.
func UsecTime() (sec Ulint, usec Ulint) {
	now := time.Now()
	return Ulint(now.Unix()), Ulint(now.Nanosecond() / 1000)
}

// TimeUs returns microseconds since epoch and optionally stores it in tloc.
func TimeUs(tloc *uint64) uint64 {
	now := time.Now()
	us := uint64(now.Unix())*1_000_000 + uint64(now.Nanosecond()/1000)
	if tloc != nil {
		*tloc = us
	}
	return us
}

// TimeMs returns milliseconds since epoch.
func TimeMs() Ulint {
	return Ulint(time.Now().UnixNano() / 1_000_000)
}

// DiffTime returns the difference in seconds.
func DiffTime(time2, time1 int64) float64 {
	return float64(time2 - time1)
}

// TimestampString formats the current time as YYMMDD HH:MM:SS.
func TimestampString() string {
	return time.Now().Format("060102 15:04:05")
}

// Delay sleeps for the given microseconds and returns the delay.
func Delay(delay Ulint) Ulint {
	if delay == 0 {
		return 0
	}
	time.Sleep(time.Duration(delay) * time.Microsecond)
	return delay
}

// PrintBuf formats a byte buffer in hex and ASCII.
func PrintBuf(buf []byte) string {
	var b strings.Builder
	fmt.Fprintf(&b, "len %d; hex ", len(buf))
	for _, v := range buf {
		fmt.Fprintf(&b, "%02x", v)
	}
	b.WriteString("; asc ")
	for _, v := range buf {
		if v >= 32 && v <= 126 {
			b.WriteByte(v)
		} else {
			b.WriteByte(' ')
		}
	}
	b.WriteString(";")
	return b.String()
}

// PowerUp rounds n up to the nearest power of two.
func PowerUp(n Ulint) Ulint {
	if n == 0 {
		return 1
	}
	res := Ulint(1)
	for res < n {
		res *= 2
	}
	return res
}

// QuoteFilename quotes a filename, doubling single quotes.
func QuoteFilename(name string) string {
	var b strings.Builder
	b.WriteByte('\'')
	for _, r := range name {
		if r == '\'' {
			b.WriteByte('\'')
		}
		b.WriteRune(r)
	}
	b.WriteByte('\'')
	return b.String()
}

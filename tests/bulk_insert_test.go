package tests

import (
	"encoding/binary"
	"math"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wilhasse/innodb-go/data"
	"github.com/wilhasse/innodb-go/row"
)

const (
	defaultRows   = 1000
	defaultBatch  = 100
	defaultThread = 4
)

func TestBulkInsert(t *testing.T) {
	rows := envInt("IBGO_BULK_ROWS", defaultRows)
	batch := envInt("IBGO_BULK_BATCH", defaultBatch)
	threads := envInt("IBGO_BULK_THREADS", defaultThread)
	if rows < 1 {
		rows = defaultRows
	}
	if batch < 1 {
		batch = defaultBatch
	}
	if threads < 1 {
		threads = defaultThread
	}

	store := row.NewStore(0)
	var mu sync.Mutex
	var inserted uint64

	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < threads; i++ {
		startRow := i * rows / threads
		endRow := (i + 1) * rows / threads
		wg.Add(1)
		go func(seed int64, start, end int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			for batchStart := start; batchStart < end; batchStart += batch {
				batchEnd := batchStart + batch
				if batchEnd > end {
					batchEnd = end
				}
				for j := batchStart; j < batchEnd; j++ {
					tuple := makeBulkTuple(uint64(j+1), rng)
					mu.Lock()
					err := store.Insert(tuple)
					mu.Unlock()
					if err != nil {
						t.Errorf("insert %d: %v", j+1, err)
						return
					}
					atomic.AddUint64(&inserted, 1)
				}
			}
		}(int64(i+1), startRow, endRow)
	}
	wg.Wait()

	if got := atomic.LoadUint64(&inserted); got != uint64(rows) {
		t.Fatalf("inserted=%d expected=%d", got, rows)
	}
	if len(store.Rows) != rows {
		t.Fatalf("rows=%d expected=%d", len(store.Rows), rows)
	}
	t.Logf("bulk insert rows=%d batch=%d threads=%d took=%s", rows, batch, threads, time.Since(start))
}

func makeBulkTuple(id uint64, rng *rand.Rand) *data.Tuple {
	fields := make([]data.Field, 7)
	fields[0] = makeUint64Field(id)
	fields[1] = makeUint32Field(uint32(rng.Intn(1_000_000)))
	fields[2] = makeStringField(randomString(rng, 5, 20))
	fields[3] = makeStringField(randomEmail(rng, 8, 12))
	fields[4] = makeFloat64Field(rng.Float64() * 100)
	fields[5] = makeUint32Field(uint32(time.Now().Unix()))
	fields[6] = makeStringField(randomString(rng, 20, 80))
	return &data.Tuple{
		NFields:    len(fields),
		NFieldsCmp: len(fields),
		Fields:     fields,
		Magic:      data.DataTupleMagic,
	}
}

func makeUint64Field(val uint64) data.Field {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], val)
	return data.Field{Data: buf[:], Len: 8}
}

func makeUint32Field(val uint32) data.Field {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], val)
	return data.Field{Data: buf[:], Len: 4}
}

func makeFloat64Field(val float64) data.Field {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], math.Float64bits(val))
	return data.Field{Data: buf[:], Len: 8}
}

func makeStringField(val string) data.Field {
	return data.Field{Data: []byte(val), Len: uint32(len(val))}
}

func randomString(rng *rand.Rand, minLen, maxLen int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 "
	n := minLen + rng.Intn(maxLen-minLen+1)
	b := make([]byte, n)
	for i := range b {
		b[i] = charset[rng.Intn(len(charset))]
	}
	return string(b)
}

func randomEmail(rng *rand.Rand, minLen, maxLen int) string {
	domains := []string{"gmail.com", "yahoo.com", "hotmail.com", "company.com", "test.org"}
	user := randomString(rng, minLen, maxLen)
	return user + "@" + domains[rng.Intn(len(domains))]
}

func envInt(name string, fallback int) int {
	val := os.Getenv(name)
	if val == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(val)
	if err != nil {
		return fallback
	}
	return parsed
}

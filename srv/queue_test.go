package srv

import (
	"testing"

	"github.com/wilhasse/innodb-go/que"
)

func TestQueryQueue(t *testing.T) {
	q := &QueryQueue{}
	t1 := &que.Thr{}
	t2 := &que.Thr{}
	q.Enqueue(t1)
	q.Enqueue(t2)

	if q.Len() != 2 {
		t.Fatalf("len=%d", q.Len())
	}
	if got := q.Dequeue(); got != t1 {
		t.Fatalf("expected t1")
	}
	if got := q.Dequeue(); got != t2 {
		t.Fatalf("expected t2")
	}
	if got := q.Dequeue(); got != nil {
		t.Fatalf("expected empty queue")
	}
}

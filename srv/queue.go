package srv

import "github.com/wilhasse/innodb-go/que"

// QueryQueue is a simple FIFO queue for query threads.
type QueryQueue struct {
	queue []*que.Thr
}

// Enqueue adds a thread to the queue.
func (q *QueryQueue) Enqueue(thr *que.Thr) {
	if q == nil || thr == nil {
		return
	}
	q.queue = append(q.queue, thr)
}

// Dequeue removes and returns the next thread.
func (q *QueryQueue) Dequeue() *que.Thr {
	if q == nil || len(q.queue) == 0 {
		return nil
	}
	thr := q.queue[0]
	q.queue = q.queue[1:]
	return thr
}

// Len returns the queue length.
func (q *QueryQueue) Len() int {
	if q == nil {
		return 0
	}
	return len(q.queue)
}

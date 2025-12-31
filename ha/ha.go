package ha

// Node is a hash chain node.
type Node struct {
	Next *Node
	Fold uint64
	Data any
}

// HashTable stores chained hash buckets.
type HashTable struct {
	buckets []*Node
}

// Create returns a hash table with at least n buckets.
func Create(n int) *HashTable {
	if n < 1 {
		n = 1
	}
	buckets := make([]*Node, nextPrime(n))
	return &HashTable{buckets: buckets}
}

// Clear removes all nodes from the hash table.
func (t *HashTable) Clear() {
	if t == nil {
		return
	}
	for i := range t.buckets {
		t.buckets[i] = nil
	}
}

// SearchAndGetData returns the data for a fold value.
func SearchAndGetData(t *HashTable, fold uint64) any {
	if t == nil || len(t.buckets) == 0 {
		return nil
	}
	for node := t.buckets[t.bucketIndex(fold)]; node != nil; node = node.Next {
		if node.Fold == fold {
			return node.Data
		}
	}
	return nil
}

// SearchAndUpdateIfFound updates data when a matching entry exists.
func SearchAndUpdateIfFound(t *HashTable, fold uint64, data any, newData any) {
	if t == nil || len(t.buckets) == 0 {
		return
	}
	for node := t.buckets[t.bucketIndex(fold)]; node != nil; node = node.Next {
		if node.Fold == fold && node.Data == data {
			node.Data = newData
			return
		}
	}
}

// InsertForFold inserts or updates a fold entry.
func InsertForFold(t *HashTable, fold uint64, data any) bool {
	if t == nil || len(t.buckets) == 0 {
		return false
	}
	idx := t.bucketIndex(fold)
	for node := t.buckets[idx]; node != nil; node = node.Next {
		if node.Fold == fold {
			node.Data = data
			return true
		}
	}
	node := &Node{Fold: fold, Data: data, Next: t.buckets[idx]}
	t.buckets[idx] = node
	return true
}

// SearchAndDeleteIfFound removes a matching entry.
func SearchAndDeleteIfFound(t *HashTable, fold uint64, data any) bool {
	if t == nil || len(t.buckets) == 0 {
		return false
	}
	idx := t.bucketIndex(fold)
	var prev *Node
	for node := t.buckets[idx]; node != nil; node = node.Next {
		if node.Fold == fold && node.Data == data {
			if prev == nil {
				t.buckets[idx] = node.Next
			} else {
				prev.Next = node.Next
			}
			return true
		}
		prev = node
	}
	return false
}

func (t *HashTable) bucketIndex(fold uint64) int {
	return int(fold % uint64(len(t.buckets)))
}

func nextPrime(n int) int {
	for {
		if isPrime(n) {
			return n
		}
		n++
	}
}

func isPrime(n int) bool {
	if n < 2 {
		return false
	}
	if n%2 == 0 {
		return n == 2
	}
	for i := 3; i*i <= n; i += 2 {
		if n%i == 0 {
			return false
		}
	}
	return true
}

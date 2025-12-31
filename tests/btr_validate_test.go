package tests

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/wilhasse/innodb-go/btr"
)

func TestBtrValidateRandomOps(t *testing.T) {
	tree := btr.NewTree(4, nil)
	rng := rand.New(rand.NewSource(1))
	store := make(map[string]string)

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("%03d", rng.Intn(200))
		val := fmt.Sprintf("v%s", key)
		tree.Insert([]byte(key), []byte(val))
		store[key] = val
		if err := btr.ValidateIndex(tree); err != nil {
			t.Fatalf("validate after insert: %v", err)
		}
	}

	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("%03d", rng.Intn(200))
		if _, ok := store[key]; ok {
			tree.Delete([]byte(key))
			delete(store, key)
			if err := btr.ValidateIndex(tree); err != nil {
				t.Fatalf("validate after delete: %v", err)
			}
		}
	}

	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("%03d", rng.Intn(200))
		if _, ok := store[key]; ok {
			val := fmt.Sprintf("u%s", key)
			tree.Insert([]byte(key), []byte(val))
			store[key] = val
			if err := btr.ValidateIndex(tree); err != nil {
				t.Fatalf("validate after update: %v", err)
			}
		}
	}

	var got []string
	cur := tree.First()
	if cur != nil {
		for {
			got = append(got, string(cur.Key()))
			if !cur.Next() {
				break
			}
		}
	}

	want := make([]string, 0, len(store))
	for key := range store {
		want = append(want, key)
	}
	sort.Strings(want)

	if len(got) != len(want) {
		t.Fatalf("range scan count mismatch: got %d want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("range scan mismatch: got %v want %v", got, want)
		}
	}
}

package btr

import (
	"fmt"
	"strings"
)

// TraceOperations runs a deterministic B-tree trace and returns the log.
func TraceOperations() string {
	var b strings.Builder
	tree := NewTree(4, nil)

	logf := func(format string, args ...interface{}) {
		fmt.Fprintf(&b, format, args...)
		b.WriteByte('\n')
	}
	insert := func(key, value string) {
		tree.Insert([]byte(key), []byte(value))
		logf("insert %s=%s", key, value)
	}
	search := func(key string) {
		val, ok := tree.Search([]byte(key))
		if ok {
			logf("search %s => %s", key, val)
		} else {
			logf("search %s => <miss>", key)
		}
	}
	del := func(key string) {
		ok := tree.Delete([]byte(key))
		logf("delete %s => %t", key, ok)
	}
	update := func(key, value string) {
		tree.Insert([]byte(key), []byte(value))
		logf("update %s=%s", key, value)
	}

	insert("a", "va")
	insert("c", "vc")
	insert("b", "vb")
	search("b")
	del("c")
	insert("d", "vd")
	update("b", "vb2")
	search("c")

	keys := finalKeys(tree)
	logf("final keys: %s", strings.Join(keys, " "))
	return b.String()
}

func finalKeys(tree *Tree) []string {
	var keys []string
	if tree == nil {
		return keys
	}
	cur := tree.First()
	for cur != nil && cur.Valid() {
		keys = append(keys, string(cur.Key()))
		if !cur.Next() {
			break
		}
	}
	return keys
}

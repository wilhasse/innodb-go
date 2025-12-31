package row

import (
	"testing"

	"github.com/wilhasse/innodb-go/data"
)

func TestPurgeList(t *testing.T) {
	list := &PurgeList{}
	list.Add(&data.Tuple{})
	list.Add(&data.Tuple{})
	list.Add(nil)

	list.MarkDeleted(1)

	purged := list.Run()
	if purged != 2 {
		t.Fatalf("purged=%d", purged)
	}
	if len(list.Items) != 1 {
		t.Fatalf("items=%d", len(list.Items))
	}
	if list.Items[0].Tuple == nil {
		t.Fatalf("expected remaining tuple")
	}
}

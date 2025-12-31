package row

import (
	"testing"

	"github.com/wilhasse/innodb-go/data"
)

func TestBuildIndexEntry(t *testing.T) {
	row := &data.Tuple{
		Fields: []data.Field{
			{Data: []byte("a"), Len: 1},
			{Data: []byte("longdata"), Len: 8, Ext: true},
		},
	}
	ext := NewExtCacheWithLimit([]int{1}, row, 3)
	entry, err := BuildIndexEntry(row, []int{0, 1}, ext)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if string(entry.Fields[0].Data) != "a" {
		t.Fatalf("field0=%s", entry.Fields[0].Data)
	}
	if entry.Fields[1].Len != 3 || string(entry.Fields[1].Data) != "lon" {
		t.Fatalf("field1=%s len=%d", entry.Fields[1].Data, entry.Fields[1].Len)
	}
}

func TestCopyRowModes(t *testing.T) {
	row := &data.Tuple{
		Fields: []data.Field{{Data: []byte("abc"), Len: 3}},
	}
	copyData := CopyRow(row, CopyData)
	copyPointers := CopyRow(row, CopyPointers)

	row.Fields[0].Data[0] = 'z'
	if copyData.Fields[0].Data[0] != 'a' {
		t.Fatalf("expected copy data to be independent")
	}
	if copyPointers.Fields[0].Data[0] != 'z' {
		t.Fatalf("expected copy pointers to share data")
	}
}

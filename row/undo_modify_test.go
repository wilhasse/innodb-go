package row

import (
	"testing"

	"github.com/wilhasse/innodb-go/data"
)

func TestUndoModify(t *testing.T) {
	tuple := &data.Tuple{
		Fields: []data.Field{{Data: []byte("a"), Len: 1}},
	}
	log := &UndoModifyLog{}
	log.RecordModify(tuple)

	tuple.Fields[0].Data[0] = 'b'

	if err := log.UndoLast(); err != nil {
		t.Fatalf("undo: %v", err)
	}
	if string(tuple.Fields[0].Data) != "a" {
		t.Fatalf("expected restored value, got %s", tuple.Fields[0].Data)
	}
	if err := log.UndoLast(); err != ErrUndoEmpty {
		t.Fatalf("expected empty undo, got %v", err)
	}
}

package rem

import (
	"bytes"
	"testing"

	"github.com/wilhasse/innodb-go/data"
)

func TestPackUnpackTuple(t *testing.T) {
	tuple := &data.Tuple{
		Fields: []data.Field{
			{Data: []byte("a"), Len: 1},
			{Len: data.UnivSQLNull},
			{Data: []byte("bb"), Len: 2},
		},
	}
	buf := PackTuple(tuple)
	if len(buf) == 0 {
		t.Fatalf("expected packed data")
	}
	got, err := UnpackTuple(buf)
	if err != nil {
		t.Fatalf("unpack: %v", err)
	}
	if got.NFields != 3 {
		t.Fatalf("nfields=%d", got.NFields)
	}
	if got.Fields[1].Len != data.UnivSQLNull {
		t.Fatalf("expected null field")
	}
	if !bytes.Equal(got.Fields[0].Data, []byte("a")) {
		t.Fatalf("field0=%s", got.Fields[0].Data)
	}
	if !bytes.Equal(got.Fields[2].Data, []byte("bb")) {
		t.Fatalf("field2=%s", got.Fields[2].Data)
	}
}

func TestUnpackTupleTruncated(t *testing.T) {
	if _, err := UnpackTuple([]byte{0x00}); err == nil {
		t.Fatalf("expected error")
	}
}

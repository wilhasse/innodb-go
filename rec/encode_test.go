package rec

import (
	"bytes"
	"testing"

	"github.com/wilhasse/innodb-go/data"
)

func TestEncodeFixedIntChar(t *testing.T) {
	tpl := &data.Tuple{Fields: []data.Field{
		{Data: []byte{0x00, 0x00, 0x00, 0x2a}, Len: 4},
		{Data: []byte("ab"), Len: 2},
	}}
	got, err := EncodeFixed(tpl, []int{4, 3}, RecNNewExtraBytes)
	if err != nil {
		t.Fatalf("EncodeFixed: %v", err)
	}
	want := make([]byte, RecNNewExtraBytes+7)
	copy(want[RecNNewExtraBytes:], []byte{0x00, 0x00, 0x00, 0x2a, 'a', 'b', 0x00})
	if !bytes.Equal(got, want) {
		t.Fatalf("got=%v want=%v", got, want)
	}
}

func TestEncodeFixedNull(t *testing.T) {
	tpl := &data.Tuple{Fields: []data.Field{
		{Len: data.UnivSQLNull},
		{Data: []byte("x"), Len: 1},
	}}
	got, err := EncodeFixed(tpl, []int{2, 1}, 0)
	if err != nil {
		t.Fatalf("EncodeFixed: %v", err)
	}
	want := []byte{0x00, 0x00, 'x'}
	if !bytes.Equal(got, want) {
		t.Fatalf("got=%v want=%v", got, want)
	}
}

package rec

import (
	"bytes"
	"testing"

	"github.com/wilhasse/innodb-go/data"
)

func TestDecodeFixedRoundTrip(t *testing.T) {
	tpl := &data.Tuple{Fields: []data.Field{
		{Data: []byte{0x00, 0x00, 0x00, 0x2a}, Len: 4},
		{Data: []byte("hi"), Len: 2},
	}}
	enc, err := EncodeFixed(tpl, []int{4, 2}, RecNNewExtraBytes)
	if err != nil {
		t.Fatalf("EncodeFixed: %v", err)
	}
	dec, err := DecodeFixed(enc, []int{4, 2}, RecNNewExtraBytes)
	if err != nil {
		t.Fatalf("DecodeFixed: %v", err)
	}
	if dec.Magic != data.DataTupleMagic {
		t.Fatalf("magic=%d", dec.Magic)
	}
	if dec.NFields != len(tpl.Fields) || dec.NFieldsCmp != len(tpl.Fields) {
		t.Fatalf("nfields=%d nfieldsCmp=%d", dec.NFields, dec.NFieldsCmp)
	}
	for i := range tpl.Fields {
		if dec.Fields[i].Len != tpl.Fields[i].Len {
			t.Fatalf("field %d len=%d want=%d", i, dec.Fields[i].Len, tpl.Fields[i].Len)
		}
		if !bytes.Equal(dec.Fields[i].Data, tpl.Fields[i].Data) {
			t.Fatalf("field %d data=%v want=%v", i, dec.Fields[i].Data, tpl.Fields[i].Data)
		}
	}
}

func TestDecodeVarRoundTrip(t *testing.T) {
	tpl := &data.Tuple{Fields: []data.Field{
		{Data: []byte("alpha"), Len: 5},
		{Len: data.UnivSQLNull},
		{Data: []byte("beta"), Len: 4},
	}}
	enc, err := EncodeVar(tpl, nil, 2)
	if err != nil {
		t.Fatalf("EncodeVar: %v", err)
	}
	dec, err := DecodeVar(enc, len(tpl.Fields), 2)
	if err != nil {
		t.Fatalf("DecodeVar: %v", err)
	}
	if dec.NFields != len(tpl.Fields) || dec.NFieldsCmp != len(tpl.Fields) {
		t.Fatalf("nfields=%d nfieldsCmp=%d", dec.NFields, dec.NFieldsCmp)
	}
	if !bytes.Equal(dec.Fields[0].Data, tpl.Fields[0].Data) || dec.Fields[0].Len != tpl.Fields[0].Len {
		t.Fatalf("field 0 data=%v len=%d", dec.Fields[0].Data, dec.Fields[0].Len)
	}
	if !data.FieldIsNull(&dec.Fields[1]) {
		t.Fatalf("field 1 expected null")
	}
	if !bytes.Equal(dec.Fields[2].Data, tpl.Fields[2].Data) || dec.Fields[2].Len != tpl.Fields[2].Len {
		t.Fatalf("field 2 data=%v len=%d", dec.Fields[2].Data, dec.Fields[2].Len)
	}
}

func TestDecodeVarPrefix(t *testing.T) {
	tpl := &data.Tuple{Fields: []data.Field{
		{Data: []byte("prefix"), Len: 6},
	}}
	enc, err := EncodeVar(tpl, []int{3}, 0)
	if err != nil {
		t.Fatalf("EncodeVar: %v", err)
	}
	dec, err := DecodeVar(enc, 1, 0)
	if err != nil {
		t.Fatalf("DecodeVar: %v", err)
	}
	if dec.Fields[0].Len != 3 {
		t.Fatalf("field 0 len=%d", dec.Fields[0].Len)
	}
	if !bytes.Equal(dec.Fields[0].Data, []byte("pre")) {
		t.Fatalf("field 0 data=%q", dec.Fields[0].Data)
	}
}

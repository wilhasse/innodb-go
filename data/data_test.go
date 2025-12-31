package data

import "testing"

func TestFieldNullAndEquality(t *testing.T) {
	var field Field
	FieldSetNull(&field)
	if !FieldIsNull(&field) {
		t.Fatalf("expected field to be null")
	}
	if !FieldDataIsBinaryEqual(&field, UnivSQLNull, nil) {
		t.Fatalf("expected null comparison to succeed")
	}
}

func TestFieldDup(t *testing.T) {
	field := Field{
		Data: []byte("abc"),
		Len:  3,
	}
	FieldDup(&field)
	field.Data[0] = 'z'
	if string(field.Data) != "zbc" {
		t.Fatalf("unexpected data mutation")
	}
}

func TestTupleCollCmp(t *testing.T) {
	tuple1 := NewTuple(2)
	FieldSetData(&tuple1.Fields[0], []byte("a"), 1)
	FieldSetData(&tuple1.Fields[1], []byte("b"), 1)

	tuple2 := NewTuple(2)
	FieldSetData(&tuple2.Fields[0], []byte("a"), 1)
	FieldSetData(&tuple2.Fields[1], []byte("c"), 1)

	if cmp := TupleCollCmp(tuple1, tuple2); cmp >= 0 {
		t.Fatalf("expected tuple1 < tuple2")
	}
}

func TestTupleCheckTyped(t *testing.T) {
	tuple := NewTuple(1)
	tuple.Fields[0].Type.MType = DataVarchar
	if !TupleCheckTyped(tuple) {
		t.Fatalf("expected tuple to be typed")
	}
	tuple.Fields[0].Type.MType = DataError
	if TupleCheckTyped(tuple) {
		t.Fatalf("expected tuple to be invalid")
	}
}

func TestTupleSetNFields(t *testing.T) {
	tuple := NewTuple(2)
	TupleSetNFields(tuple, 1)
	if tuple.NFields != 1 || tuple.NFieldsCmp != 1 {
		t.Fatalf("expected fields to shrink")
	}
}

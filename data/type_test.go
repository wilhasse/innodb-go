package data

import "testing"

func TestDataTypeStringChecks(t *testing.T) {
	if !DataTypeIsStringType(DataVarchar) {
		t.Fatalf("expected varchar to be string type")
	}
	if DataTypeIsStringType(DataInt) {
		t.Fatalf("expected int to be non-string type")
	}
	if !DataTypeIsBinaryStringType(DataBinary, 0) {
		t.Fatalf("expected binary type to be binary string")
	}
	if !DataTypeIsNonBinaryStringType(DataBlob, 0) {
		t.Fatalf("expected blob without binary flag to be non-binary string")
	}
	if DataTypeIsNonBinaryStringType(DataBlob, DataBinaryType) {
		t.Fatalf("expected blob with binary flag to be binary")
	}
}

func TestDataTypeFormPrtype(t *testing.T) {
	if got := DataTypeFormPrtype(1, 2); got != (1 + (2 << 16)) {
		t.Fatalf("unexpected prtype formation: %d", got)
	}
}

func TestDataTypeValidate(t *testing.T) {
	typ := &DataType{MType: DataVarchar, PrType: 0, MbMinLen: 1, MbMaxLen: 1}
	if !DataTypeValidate(typ) {
		t.Fatalf("expected type to validate")
	}
	typ.MType = DataError
	if DataTypeValidate(typ) {
		t.Fatalf("expected invalid mtype to fail")
	}
	typ.MType = DataSys
	typ.PrType = DataClientTypeMask
	if DataTypeValidate(typ) {
		t.Fatalf("expected invalid sys prtype to fail")
	}
	typ.MType = DataVarchar
	typ.PrType = 0
	typ.MbMinLen = 2
	typ.MbMaxLen = 1
	if DataTypeValidate(typ) {
		t.Fatalf("expected invalid mb lengths to fail")
	}
}

func TestDataTypeGetAtMostNMbchars(t *testing.T) {
	got := DataTypeGetAtMostNMbchars(0, 1, 1, 3, 5, []byte("hello"))
	if got != 3 {
		t.Fatalf("expected prefix len 3, got %d", got)
	}
	got = DataTypeGetAtMostNMbchars(0, 1, 1, 10, 5, []byte("hello"))
	if got != 5 {
		t.Fatalf("expected data len 5, got %d", got)
	}
}

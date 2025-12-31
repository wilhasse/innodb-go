package rec

import (
	"reflect"
	"testing"
)

func TestOffsetsFixedIntChar(t *testing.T) {
	got := OffsetsFixed([]int{4, 10}, RecNNewExtraBytes)
	want := []int{RecNNewExtraBytes, 4, 14}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("OffsetsFixed=%v want %v", got, want)
	}
}

func TestOffsetsFixedOldExtra(t *testing.T) {
	got := OffsetsFixed([]int{4, 4, 4}, RecNOldExtraBytes)
	want := []int{RecNOldExtraBytes, 4, 8, 12}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("OffsetsFixed=%v want %v", got, want)
	}
}

func TestOffsetsVarWithNull(t *testing.T) {
	got := OffsetsVar([]int{3, 5, 2}, []bool{false, true, false}, RecNNewExtraBytes)
	want := []uint32{uint32(RecNNewExtraBytes), 3, 3 | RecOffsSQLNull, 5}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("OffsetsVar=%v want %v", got, want)
	}
}

func TestOffsetsVarNoNulls(t *testing.T) {
	got := OffsetsVar([]int{1, 0, 4}, nil, RecNNewExtraBytes)
	want := []uint32{uint32(RecNNewExtraBytes), 1, 1, 5}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("OffsetsVar=%v want %v", got, want)
	}
}

package rec

import (
	"reflect"
	"testing"
)

func TestOffsetsFixedIntChar(t *testing.T) {
	got := OffsetsFixed([]int{4, 10}, RecNNewExtraBytes)
	want := []int{RecNNewExtraBytes, RecNNewExtraBytes + 4, RecNNewExtraBytes + 14}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("OffsetsFixed=%v want %v", got, want)
	}
}

func TestOffsetsFixedOldExtra(t *testing.T) {
	got := OffsetsFixed([]int{4, 4, 4}, RecNOldExtraBytes)
	want := []int{RecNOldExtraBytes, RecNOldExtraBytes + 4, RecNOldExtraBytes + 8, RecNOldExtraBytes + 12}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("OffsetsFixed=%v want %v", got, want)
	}
}

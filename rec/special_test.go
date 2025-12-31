package rec

import (
	"bytes"
	"testing"
)

func TestSpecialRecordTemplates(t *testing.T) {
	if len(InfimumExtra) != RecNNewExtraBytes {
		t.Fatalf("infimum extra len=%d", len(InfimumExtra))
	}
	if len(SupremumExtra) != RecNNewExtraBytes {
		t.Fatalf("supremum extra len=%d", len(SupremumExtra))
	}
	if !bytes.Equal(InfimumExtra, []byte{0x01, 0x00, 0x02, 0x00, 0x00}) {
		t.Fatalf("infimum extra=%v", InfimumExtra)
	}
	if !bytes.Equal(InfimumData, []byte{'i', 'n', 'f', 'i', 'm', 'u', 'm', 0x00}) {
		t.Fatalf("infimum data=%v", InfimumData)
	}
	if !bytes.Equal(SupremumExtra, []byte{0x01, 0x00, 0x0b, 0x00, 0x00}) {
		t.Fatalf("supremum extra=%v", SupremumExtra)
	}
	if !bytes.Equal(SupremumData, []byte{'s', 'u', 'p', 'r', 'e', 'm', 'u', 'm'}) {
		t.Fatalf("supremum data=%v", SupremumData)
	}
}

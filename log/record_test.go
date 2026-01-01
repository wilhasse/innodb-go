package log

import "testing"

func TestRecordCodecRoundtrip(t *testing.T) {
	in := Record{
		Type:    1,
		SpaceID: 42,
		PageNo:  7,
		Payload: []byte("hello"),
	}
	encoded := EncodeRecord(in)
	out, n, err := DecodeRecord(encoded)
	if err != nil {
		t.Fatalf("DecodeRecord: %v", err)
	}
	if n != len(encoded) {
		t.Fatalf("DecodeRecord size=%d, want %d", n, len(encoded))
	}
	if out.Type != in.Type || out.SpaceID != in.SpaceID || out.PageNo != in.PageNo {
		t.Fatalf("unexpected record header")
	}
	if string(out.Payload) != string(in.Payload) {
		t.Fatalf("payload=%q, want %q", out.Payload, in.Payload)
	}
}

func TestRecordCodecShortBuffer(t *testing.T) {
	_, _, err := DecodeRecord([]byte{1, 2, 3})
	if err == nil {
		t.Fatalf("expected short record error")
	}
}

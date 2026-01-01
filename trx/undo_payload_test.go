package trx

import "testing"

func TestUndoPayloadRoundtrip(t *testing.T) {
	in := &UndoPayload{
		TrxID:       42,
		PrimaryKey:  []byte("pk"),
		BeforeImage: []byte("before"),
	}
	buf := EncodeUndoPayload(in)
	out, err := DecodeUndoPayload(buf)
	if err != nil {
		t.Fatalf("DecodeUndoPayload: %v", err)
	}
	if out.TrxID != in.TrxID {
		t.Fatalf("trxID=%d, want %d", out.TrxID, in.TrxID)
	}
	if string(out.PrimaryKey) != string(in.PrimaryKey) {
		t.Fatalf("pk=%q, want %q", out.PrimaryKey, in.PrimaryKey)
	}
	if string(out.BeforeImage) != string(in.BeforeImage) {
		t.Fatalf("before=%q, want %q", out.BeforeImage, in.BeforeImage)
	}
}

func TestUndoPayloadShortBuffer(t *testing.T) {
	if _, err := DecodeUndoPayload([]byte{1, 2}); err == nil {
		t.Fatalf("expected error on short buffer")
	}
}

package trx

import (
	"bytes"
	"testing"
)

func TestEncodeDecodeUndoRecord(t *testing.T) {
	rec := &UndoRecord{
		Type:          UndoInsertRec,
		CmplInfo:      2,
		UpdatedExtern: true,
		UndoNo:        42,
		TableID:       99,
		Data:          []byte("payload"),
	}
	buf := EncodeUndoRecord(rec)
	decoded, err := DecodeUndoRecord(buf)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if decoded.Type != rec.Type || decoded.CmplInfo != rec.CmplInfo ||
		decoded.UpdatedExtern != rec.UpdatedExtern ||
		decoded.UndoNo != rec.UndoNo || decoded.TableID != rec.TableID {
		t.Fatalf("decoded header mismatch")
	}
	if !bytes.Equal(decoded.Data, rec.Data) {
		t.Fatalf("decoded data mismatch")
	}
}

func TestUndoRecordAccessors(t *testing.T) {
	rec := &UndoRecord{
		Type:          UndoUpdExistRec,
		CmplInfo:      3,
		UpdatedExtern: false,
		UndoNo:        7,
		TableID:       55,
	}
	buf := EncodeUndoRecord(rec)
	typ, err := UndoRecordType(buf)
	if err != nil || typ != rec.Type {
		t.Fatalf("type=%d err=%v", typ, err)
	}
	cmpl, err := UndoRecordCmplInfo(buf)
	if err != nil || cmpl != rec.CmplInfo {
		t.Fatalf("cmpl=%d err=%v", cmpl, err)
	}
	updated, err := UndoRecordUpdatedExtern(buf)
	if err != nil || updated != rec.UpdatedExtern {
		t.Fatalf("extern=%v err=%v", updated, err)
	}
	undoNo, err := UndoRecordUndoNo(buf)
	if err != nil || undoNo != rec.UndoNo {
		t.Fatalf("undo=%d err=%v", undoNo, err)
	}
	tableID, err := UndoRecordTableID(buf)
	if err != nil || tableID != rec.TableID {
		t.Fatalf("table=%d err=%v", tableID, err)
	}
}

func TestUndoRecordShortBuffer(t *testing.T) {
	if _, err := DecodeUndoRecord([]byte{1, 2}); err == nil {
		t.Fatalf("expected error")
	}
	if _, err := UndoRecordType(nil); err == nil {
		t.Fatalf("expected error")
	}
	if _, err := UndoRecordTableID(make([]byte, 10)); err == nil {
		t.Fatalf("expected error")
	}
}

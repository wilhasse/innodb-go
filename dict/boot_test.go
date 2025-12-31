package dict

import "testing"

func TestDictCreateBoot(t *testing.T) {
	DictCreate()

	if DictSys == nil {
		t.Fatalf("expected DictSys to be initialized")
	}
	if DictSys.SysTables == nil || DictSys.SysColumns == nil || DictSys.SysIndexes == nil || DictSys.SysFields == nil {
		t.Fatalf("expected system tables to be initialized")
	}
	if DictSys.SysTables.Indexes["CLUST_IND"] == nil {
		t.Fatalf("expected SYS_TABLES clustered index")
	}
	if DictSys.SysTables.Indexes["ID_IND"] == nil {
		t.Fatalf("expected SYS_TABLES ID index")
	}
	if DictSys.Header.TablesRoot == 0 || DictSys.Header.IndexesRoot == 0 {
		t.Fatalf("expected dictionary roots to be set")
	}
}

func TestDictHdrGetNewID(t *testing.T) {
	DictCreate()

	start := DictSys.Header.TableID
	id, err := DictHdrGetNewID(DictHdrTableID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if dulintToUint64(id) != dulintToUint64(start)+1 {
		t.Fatalf("expected table id increment")
	}
}

func TestDictSysGetNewRowID(t *testing.T) {
	DictCreate()

	DictSys.RowID = newDulint(0, DictHdrRowIDWriteMargin-1)
	DictSys.Header.RowID = newDulint(0, DictHdrRowIDWriteMargin-1)

	id, err := DictSysGetNewRowID()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if dulintToUint64(id) != DictHdrRowIDWriteMargin {
		t.Fatalf("expected row id to advance to margin")
	}
	if dulintToUint64(DictSys.Header.RowID) != DictHdrRowIDWriteMargin {
		t.Fatalf("expected header row id to flush at margin")
	}
}

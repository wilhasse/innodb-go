package api

import "testing"

func TestExecVSQLStringAndID(t *testing.T) {
	info, err := execVSQL("select * from $t where a = :a", []SQLArg{
		SQLArgString("a", "hello"),
		SQLArgID("t", "my_table"),
	})
	if err != DB_SUCCESS {
		t.Fatalf("execVSQL got %v, want %v", err, DB_SUCCESS)
	}
	if lit, ok := info.Literals["a"]; !ok || string(lit.Value) != "hello" {
		t.Fatalf("expected literal a=hello, got %#v", lit)
	}
	if id, ok := info.IDs["t"]; !ok || id.ID != "my_table" {
		t.Fatalf("expected id t=my_table, got %#v", id)
	}
}

func TestExecVSQLIntEncoding(t *testing.T) {
	info, err := execVSQL("select :n", []SQLArg{
		SQLArgIntSigned(":n", 2, -1),
		SQLArgIntUnsigned(":u", 4, 0xAABBCCDD),
	})
	if err != DB_SUCCESS {
		t.Fatalf("execVSQL got %v, want %v", err, DB_SUCCESS)
	}
	n := info.Literals["n"]
	if got := n.Value; len(got) != 2 || got[0] != 0xFF || got[1] != 0xFF {
		t.Fatalf("signed int encoding mismatch: %#v", got)
	}
	u := info.Literals["u"]
	if got := u.Value; len(got) != 4 || got[0] != 0xAA || got[1] != 0xBB || got[2] != 0xCC || got[3] != 0xDD {
		t.Fatalf("unsigned int encoding mismatch: %#v", got)
	}
}

func TestExecVSQLInvalidPrefix(t *testing.T) {
	_, err := execVSQL("select :a", []SQLArg{
		{Type: IB_VARCHAR, Name: "a", String: "bad"},
	})
	if err != DB_INVALID_INPUT {
		t.Fatalf("execVSQL got %v, want %v", err, DB_INVALID_INPUT)
	}
}

func TestExecSQLRequiresStartup(t *testing.T) {
	resetAPIState()
	if got := ExecSQL("select 1"); got != DB_ERROR {
		t.Fatalf("ExecSQL without Startup got %v, want %v", got, DB_ERROR)
	}
	if err := Init(); err != DB_SUCCESS {
		t.Fatalf("Init got %v, want %v", err, DB_SUCCESS)
	}
	if got := ExecSQL("select 1"); got != DB_ERROR {
		t.Fatalf("ExecSQL before Startup got %v, want %v", got, DB_ERROR)
	}
	if err := Startup(""); err != DB_SUCCESS {
		t.Fatalf("Startup got %v, want %v", err, DB_SUCCESS)
	}
	if got := ExecSQL("select 1"); got != DB_UNSUPPORTED {
		t.Fatalf("ExecSQL got %v, want %v", got, DB_UNSUPPORTED)
	}
	_ = Shutdown(ShutdownNormal)
}

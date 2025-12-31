package api

import "testing"

func TestCfgSetGet(t *testing.T) {
	resetAPIState()
	if err := CfgInit(); err != DB_SUCCESS {
		t.Fatalf("CfgInit got %v, want %v", err, DB_SUCCESS)
	}

	if err := CfgSet("adaptive_hash_index", false); err != DB_SUCCESS {
		t.Fatalf("CfgSet got %v, want %v", err, DB_SUCCESS)
	}
	var val bool
	if err := CfgGet("adaptive_hash_index", &val); err != DB_SUCCESS {
		t.Fatalf("CfgGet got %v, want %v", err, DB_SUCCESS)
	}
	if val {
		t.Fatal("expected adaptive_hash_index to be false")
	}
}

func TestCfgSetReadOnlyAfterStartup(t *testing.T) {
	resetAPIState()
	if err := Init(); err != DB_SUCCESS {
		t.Fatalf("Init got %v, want %v", err, DB_SUCCESS)
	}
	if err := Startup(""); err != DB_SUCCESS {
		t.Fatalf("Startup got %v, want %v", err, DB_SUCCESS)
	}
	if err := CfgSet("adaptive_hash_index", true); err != DB_READONLY {
		t.Fatalf("CfgSet after Startup got %v, want %v", err, DB_READONLY)
	}
	_ = Shutdown(ShutdownNormal)
}

func TestCfgGetAll(t *testing.T) {
	resetAPIState()
	if err := CfgInit(); err != DB_SUCCESS {
		t.Fatalf("CfgInit got %v, want %v", err, DB_SUCCESS)
	}
	names, err := CfgGetAll()
	if err != DB_SUCCESS {
		t.Fatalf("CfgGetAll got %v, want %v", err, DB_SUCCESS)
	}
	found := false
	for _, name := range names {
		if name == "adaptive_hash_index" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected adaptive_hash_index in CfgGetAll, got %v", names)
	}
}

func TestCfgNumericRange(t *testing.T) {
	resetAPIState()
	if err := CfgInit(); err != DB_SUCCESS {
		t.Fatalf("CfgInit got %v, want %v", err, DB_SUCCESS)
	}

	registerVar(&ConfigVar{
		Name:     "test_numeric",
		Type:     CfgTypeUlint,
		Flag:     CfgFlagNone,
		MinValue: 1,
		MaxValue: 3,
		Value:    Ulint(1),
	})

	if err := CfgSet("test_numeric", 4); err != DB_INVALID_INPUT {
		t.Fatalf("CfgSet out of range got %v, want %v", err, DB_INVALID_INPUT)
	}
	if err := CfgSet("test_numeric", 2); err != DB_SUCCESS {
		t.Fatalf("CfgSet in range got %v, want %v", err, DB_SUCCESS)
	}
}

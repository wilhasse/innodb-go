package api

import "testing"

func TestUtf8Strcasecmp(t *testing.T) {
	if got := Utf8Strcasecmp("Abc", "aBC"); got != 0 {
		t.Fatalf("Utf8Strcasecmp got %d, want 0", got)
	}
	if got := Utf8Strcasecmp("abc", "abd"); got >= 0 {
		t.Fatalf("Utf8Strcasecmp expected negative, got %d", got)
	}
}

func TestUtf8Strncasecmp(t *testing.T) {
	if got := Utf8Strncasecmp("AbCd", "aBcE", 3); got != 0 {
		t.Fatalf("Utf8Strncasecmp got %d, want 0", got)
	}
	if got := Utf8Strncasecmp("ab", "abc", 5); got >= 0 {
		t.Fatalf("Utf8Strncasecmp expected negative, got %d", got)
	}
}

func TestUtf8Casedown(t *testing.T) {
	buf := []byte("AbC!")
	Utf8Casedown(buf)
	if got := string(buf); got != "abc!" {
		t.Fatalf("Utf8Casedown got %q, want %q", got, "abc!")
	}
}

func TestUtf8ConvertFromID(t *testing.T) {
	dst := make([]byte, 5)
	Utf8ConvertFromID(nil, dst, "hi", 5)
	if dst[0] != 'h' || dst[1] != 'i' || dst[2] != 0 {
		t.Fatalf("Utf8ConvertFromID unexpected buffer: %#v", dst)
	}
}

func TestUtf8Isspace(t *testing.T) {
	if got := Utf8Isspace(nil, ' '); got != IBTrue {
		t.Fatalf("Utf8Isspace got %v, want %v", got, IBTrue)
	}
	if got := Utf8Isspace(nil, 'a'); got != IBFalse {
		t.Fatalf("Utf8Isspace got %v, want %v", got, IBFalse)
	}
}

func TestUcodeGetStorageSize(t *testing.T) {
	if got := UcodeGetStorageSize(nil, 4, 10, "abcdef"); got != 4 {
		t.Fatalf("UcodeGetStorageSize got %d, want 4", got)
	}
	if got := UcodeGetStorageSize(nil, 10, 4, "abcd"); got != 4 {
		t.Fatalf("UcodeGetStorageSize got %d, want 4", got)
	}
}

package lock

import "testing"

func TestModeCompatibilityMatrix(t *testing.T) {
	cases := []struct {
		a       Mode
		b       Mode
		compat  bool
	}{
		{ModeIS, ModeIS, true},
		{ModeIS, ModeIX, true},
		{ModeIS, ModeS, true},
		{ModeIS, ModeX, false},
		{ModeIX, ModeIS, true},
		{ModeIX, ModeIX, true},
		{ModeIX, ModeS, false},
		{ModeIX, ModeX, false},
		{ModeS, ModeIS, true},
		{ModeS, ModeIX, false},
		{ModeS, ModeS, true},
		{ModeS, ModeX, false},
		{ModeX, ModeIS, false},
		{ModeX, ModeIX, false},
		{ModeX, ModeS, false},
		{ModeX, ModeX, false},
	}
	for _, tc := range cases {
		if got := ModeCompatible(tc.a, tc.b); got != tc.compat {
			t.Fatalf("compat %s/%s got %v want %v", ModeName(tc.a), ModeName(tc.b), got, tc.compat)
		}
	}
}

func TestModeStrongerOrEq(t *testing.T) {
	cases := []struct {
		a    Mode
		b    Mode
		want bool
	}{
		{ModeIS, ModeIS, true},
		{ModeIX, ModeIS, true},
		{ModeS, ModeIX, true},
		{ModeX, ModeS, true},
		{ModeIS, ModeIX, false},
		{ModeIX, ModeS, false},
		{ModeS, ModeX, false},
	}
	for _, tc := range cases {
		if got := ModeStrongerOrEq(tc.a, tc.b); got != tc.want {
			t.Fatalf("stronger %s>= %s got %v want %v", ModeName(tc.a), ModeName(tc.b), got, tc.want)
		}
	}
}

func TestModeName(t *testing.T) {
	if ModeName(ModeIS) == "UNKNOWN" {
		t.Fatalf("expected named mode")
	}
	if ModeName(Mode(-1)) != "UNKNOWN" {
		t.Fatalf("expected unknown name")
	}
}

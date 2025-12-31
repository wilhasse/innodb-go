package tests

import (
	"testing"

	"github.com/wilhasse/innodb-go/api"
)

func TestShutdownHarness(t *testing.T) {
	resetAPI(t)
	for i := 0; i < 10; i++ {
		if err := api.Init(); err != api.DB_SUCCESS {
			t.Fatalf("Init: %v", err)
		}

		var val bool
		if err := api.CfgGet("doublewrite", &val); err != api.DB_SUCCESS {
			t.Fatalf("CfgGet doublewrite: %v", err)
		}
		if !val {
			t.Fatalf("doublewrite default=false, want true")
		}

		want := i%2 == 0
		if err := api.CfgSet("doublewrite", want); err != api.DB_SUCCESS {
			t.Fatalf("CfgSet doublewrite: %v", err)
		}
		if err := api.CfgGet("doublewrite", &val); err != api.DB_SUCCESS {
			t.Fatalf("CfgGet doublewrite: %v", err)
		}
		if val != want {
			t.Fatalf("doublewrite=%v, want %v", val, want)
		}

		if err := api.Shutdown(api.ShutdownNormal); err != api.DB_SUCCESS {
			t.Fatalf("Shutdown: %v", err)
		}
	}
}

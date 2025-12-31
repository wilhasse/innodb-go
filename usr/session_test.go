package usr

import (
	"testing"

	"github.com/wilhasse/innodb-go/que"
	"github.com/wilhasse/innodb-go/trx"
)

func TestSessionLifecycle(t *testing.T) {
	trx.TrxVarInit()

	sess := Open()
	if sess == nil || sess.State != SessionActive {
		t.Fatalf("expected active session")
	}
	if sess.Trx == nil {
		t.Fatalf("expected session trx")
	}

	sess.Graphs = append(sess.Graphs, &que.Fork{})
	if err := Close(sess); err != ErrSessionBusy {
		t.Fatalf("expected busy error, got %v", err)
	}
	sess.Graphs = nil

	if err := Close(sess); err != nil {
		t.Fatalf("close: %v", err)
	}
	if sess.State != SessionClosed {
		t.Fatalf("state=%d", sess.State)
	}
	if sess.Trx != nil {
		t.Fatalf("expected trx released")
	}
	if trx.TrxCount != 0 {
		t.Fatalf("trx count=%d", trx.TrxCount)
	}

	if err := Close(sess); err != ErrSessionClosed {
		t.Fatalf("expected closed error, got %v", err)
	}
}

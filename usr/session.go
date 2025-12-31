package usr

import (
	"errors"

	"github.com/wilhasse/innodb-go/que"
	"github.com/wilhasse/innodb-go/trx"
)

// SessionState tracks session lifecycle.
type SessionState int

const (
	SessionActive SessionState = iota + 1
	SessionClosed
)

// ErrSessionBusy is returned when graphs are still attached.
var ErrSessionBusy = errors.New("usr: session has active graphs")

// ErrSessionClosed is returned when closing an already closed session.
var ErrSessionClosed = errors.New("usr: session already closed")

// Session holds user session state.
type Session struct {
	State  SessionState
	Trx    *trx.Trx
	Graphs []*que.Fork
}

// Open creates a new session with an idle transaction.
func Open() *Session {
	return &Session{
		State: SessionActive,
		Trx:   trx.TrxCreate(),
	}
}

// Close releases a session and its transaction.
func Close(sess *Session) error {
	if sess == nil {
		return nil
	}
	if sess.State == SessionClosed {
		return ErrSessionClosed
	}
	if len(sess.Graphs) != 0 {
		return ErrSessionBusy
	}
	if sess.Trx != nil {
		trx.TrxRelease(sess.Trx)
		sess.Trx = nil
	}
	sess.State = SessionClosed
	return nil
}

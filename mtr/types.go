package mtr

import "github.com/wilhasse/innodb-go/dyn"

// LogMode controls what the mini-transaction writes to the log.
type LogMode int

const (
	LogAll         LogMode = 21
	LogNone        LogMode = 22
	LogShortInserts LogMode = 24
)

// Mtr holds mini-transaction state and log buffer.
type Mtr struct {
	LogMode       LogMode
	Log           *dyn.Array
	Modifications bool
	NLogRecs      int
}

// New creates a mini-transaction with an empty log buffer.
func New() *Mtr {
	return &Mtr{LogMode: LogAll, Log: dyn.New()}
}

// Reset clears the log contents and counters.
func (m *Mtr) Reset() {
	if m == nil {
		return
	}
	m.Modifications = false
	m.NLogRecs = 0
	if m.Log != nil {
		m.Log.Free()
	}
	m.Log = dyn.New()
}

// LogBytes returns a flattened copy of the log buffer.
func (m *Mtr) LogBytes() []byte {
	if m == nil || m.Log == nil {
		return nil
	}
	total := m.Log.DataSize()
	out := make([]byte, 0, total)
	for block := m.Log.FirstBlock(); block != nil; block = m.Log.NextBlock(block) {
		data := block.Data()
		used := block.Used()
		if used > len(data) {
			used = len(data)
		}
		out = append(out, data[:used]...)
	}
	return out
}

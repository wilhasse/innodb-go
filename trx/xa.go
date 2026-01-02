package trx

// XAState represents the XA/2PC lifecycle.
type XAState int

const (
	XANotStarted XAState = iota
	XAActive
	XAPrepared
	XACommitted
	XARolledBack
)

// XID identifies a distributed transaction.
type XID struct {
	FormatID int
	GTRID    []byte
	BQUAL    []byte
}

// IsZero reports whether the XID is empty.
func (x XID) IsZero() bool {
	return len(x.GTRID) == 0 && len(x.BQUAL) == 0
}

// TrxXAStart assigns an XA id and starts the transaction if needed.
func TrxXAStart(trx *Trx, xid XID) bool {
	if trx == nil || xid.IsZero() {
		return false
	}
	if trx.State == TrxNotStarted {
		TrxBegin(trx)
	}
	if trx.State != TrxActive {
		return false
	}
	trx.XID = cloneXID(xid)
	trx.XAState = XAActive
	return true
}

// TrxXAPrepare transitions an XA transaction to prepared.
func TrxXAPrepare(trx *Trx) bool {
	if trx == nil || trx.State != TrxActive || trx.XAState != XAActive || trx.XID == nil {
		return false
	}
	trx.XAState = XAPrepared
	return true
}

// TrxXACommit commits a prepared XA transaction.
func TrxXACommit(trx *Trx) bool {
	if trx == nil || trx.State != TrxActive || trx.XAState != XAPrepared {
		return false
	}
	TrxCommit(trx)
	trx.XID = nil
	trx.XAState = XACommitted
	return true
}

// TrxXARollback rolls back a prepared or active XA transaction.
func TrxXARollback(trx *Trx) bool {
	if trx == nil || trx.State != TrxActive {
		return false
	}
	if trx.XAState != XAPrepared && trx.XAState != XAActive {
		return false
	}
	TrxRollback(trx)
	trx.XID = nil
	trx.XAState = XARolledBack
	return true
}

// TrxXAClear resets XA state on a transaction.
func TrxXAClear(trx *Trx) {
	if trx == nil {
		return
	}
	trx.XID = nil
	trx.XAState = XANotStarted
}

func cloneXID(xid XID) *XID {
	return &XID{
		FormatID: xid.FormatID,
		GTRID:    append([]byte(nil), xid.GTRID...),
		BQUAL:    append([]byte(nil), xid.BQUAL...),
	}
}

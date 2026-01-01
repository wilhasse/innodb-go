package dict

// SysPersister provides SYS_* row persistence outside the dictionary package.
type SysPersister interface {
	LoadSysRows() (SysRows, error)
	PersistSysRows(rows SysRows) error
}

var sysPersister SysPersister

// SetSysPersister registers a SYS_* table persister.
func SetSysPersister(p SysPersister) {
	sysPersister = p
}

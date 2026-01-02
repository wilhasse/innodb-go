package srv

// DefaultServer is the global server instance.
var DefaultServer = NewServer()

// Startup starts the default server.
func Startup() error {
	if DefaultServer == nil {
		DefaultServer = NewServer()
	}
	if err := DefaultServer.Start(); err != nil {
		return err
	}
	if DefaultMaster != nil {
		_ = DefaultMaster.Start()
	}
	return nil
}

// Shutdown stops the default server.
func Shutdown() error {
	if DefaultServer == nil {
		return ErrNotRunning
	}
	if DefaultMaster != nil && DefaultMaster.Running() {
		_ = DefaultMaster.Stop()
	}
	return DefaultServer.Stop()
}

// IsStarted reports whether the default server is running.
func IsStarted() bool {
	return DefaultServer != nil && DefaultServer.IsRunning()
}

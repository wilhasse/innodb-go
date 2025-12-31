package srv

import "errors"

// ErrAlreadyRunning indicates the server is already running.
var ErrAlreadyRunning = errors.New("srv: already running")

// ErrNotRunning indicates the server is not running.
var ErrNotRunning = errors.New("srv: not running")

// ServerState represents the server lifecycle state.
type ServerState int

const (
	ServerStopped ServerState = iota
	ServerRunning
)

// Server tracks core server lifecycle state.
type Server struct {
	State      ServerState
	StartCount int
	StopCount  int
}

// NewServer creates a stopped server instance.
func NewServer() *Server {
	return &Server{State: ServerStopped}
}

// Start transitions the server to running.
func (s *Server) Start() error {
	if s == nil {
		return ErrNotRunning
	}
	if s.State == ServerRunning {
		return ErrAlreadyRunning
	}
	s.State = ServerRunning
	s.StartCount++
	return nil
}

// Stop transitions the server to stopped.
func (s *Server) Stop() error {
	if s == nil {
		return ErrNotRunning
	}
	if s.State != ServerRunning {
		return ErrNotRunning
	}
	s.State = ServerStopped
	s.StopCount++
	return nil
}

// IsRunning reports whether the server is running.
func (s *Server) IsRunning() bool {
	return s != nil && s.State == ServerRunning
}

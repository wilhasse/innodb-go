package api

import (
	"fmt"
	"io"
	"os"
)

// Stream matches the C ib_stream_t (typically a FILE*).
type Stream = io.Writer

// LoggerFunc mirrors ib_logger_t and behaves like fprintf.
type LoggerFunc func(stream Stream, format string, args ...any) int

var (
	// Logger is the active logging hook.
	Logger LoggerFunc = DefaultLogger
	// LogStream is passed as the first argument to Logger.
	LogStream Stream = os.Stderr
)

// DefaultLogger writes formatted output to the provided stream.
func DefaultLogger(stream Stream, format string, args ...any) int {
	if stream == nil {
		stream = os.Stderr
	}
	n, _ := fmt.Fprintf(stream, format, args...)
	return n
}

// LoggerSet updates the logging function and its default stream.
func LoggerSet(logger LoggerFunc, stream Stream) {
	if logger == nil {
		logger = DefaultLogger
	}
	if stream == nil {
		stream = os.Stderr
	}
	Logger = logger
	LogStream = stream
}

// Log writes via Logger using LogStream when stream is nil.
func Log(stream Stream, format string, args ...any) int {
	if Logger == nil {
		return 0
	}
	if stream == nil {
		stream = LogStream
	}
	if stream == nil {
		stream = os.Stderr
	}
	return Logger(stream, format, args...)
}

package os

import (
	"io"
	stdos "os"
)

// File mirrors the subset of *os.File used by the engine.
type File interface {
	io.ReaderAt
	io.WriterAt
	Sync() error
	Close() error
	Name() string
}

// FileMode mirrors os.FileMode.
type FileMode = stdos.FileMode

// FileInfo mirrors os.FileInfo.
type FileInfo = stdos.FileInfo

// FileSystem defines filesystem operations used by the port.
type FileSystem interface {
	Open(name string) (File, error)
	Create(name string) (File, error)
	OpenFile(name string, flag int, perm FileMode) (File, error)
	Remove(name string) error
	Rename(oldpath, newpath string) error
	Stat(name string) (FileInfo, error)
	MkdirAll(path string, perm FileMode) error
}

// OSFileSystem uses the host os package.
type OSFileSystem struct{}

func (OSFileSystem) Open(name string) (File, error) {
	return stdos.Open(name)
}

func (OSFileSystem) Create(name string) (File, error) {
	return stdos.Create(name)
}

func (OSFileSystem) OpenFile(name string, flag int, perm FileMode) (File, error) {
	return stdos.OpenFile(name, flag, perm)
}

func (OSFileSystem) Remove(name string) error {
	return stdos.Remove(name)
}

func (OSFileSystem) Rename(oldpath, newpath string) error {
	return stdos.Rename(oldpath, newpath)
}

func (OSFileSystem) Stat(name string) (FileInfo, error) {
	return stdos.Stat(name)
}

func (OSFileSystem) MkdirAll(path string, perm FileMode) error {
	return stdos.MkdirAll(path, perm)
}

// DefaultFS is the active filesystem implementation.
var DefaultFS FileSystem = OSFileSystem{}

// Open delegates to DefaultFS.
func Open(name string) (File, error) {
	return DefaultFS.Open(name)
}

// Create delegates to DefaultFS.
func Create(name string) (File, error) {
	return DefaultFS.Create(name)
}

// OpenFile delegates to DefaultFS.
func OpenFile(name string, flag int, perm FileMode) (File, error) {
	return DefaultFS.OpenFile(name, flag, perm)
}

// Remove delegates to DefaultFS.
func Remove(name string) error {
	return DefaultFS.Remove(name)
}

// Rename delegates to DefaultFS.
func Rename(oldpath, newpath string) error {
	return DefaultFS.Rename(oldpath, newpath)
}

// Stat delegates to DefaultFS.
func Stat(name string) (FileInfo, error) {
	return DefaultFS.Stat(name)
}

// MkdirAll delegates to DefaultFS.
func MkdirAll(path string, perm FileMode) error {
	return DefaultFS.MkdirAll(path, perm)
}

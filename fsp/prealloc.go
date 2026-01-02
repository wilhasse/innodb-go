package fsp

var preallocateFiles bool

// SetPreallocateFiles toggles full file preallocation.
func SetPreallocateFiles(enabled bool) {
	preallocateFiles = enabled
}

func preallocateFilesEnabled() bool {
	return preallocateFiles
}

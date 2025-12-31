package ha

// DefaultHeapBytes mirrors HA_STORAGE_DEFAULT_HEAP_BYTES.
const DefaultHeapBytes uint64 = 1024

// DefaultHashCells mirrors HA_STORAGE_DEFAULT_HASH_CELLS.
const DefaultHashCells uint64 = 4096

// Storage keeps unique data blobs.
type Storage struct {
	entries   map[string][]byte
	size      uint64
	HeapBytes uint64
	HashCells uint64
}

// CreateStorage allocates a new storage with optional sizing hints.
func CreateStorage(initialHeapBytes, initialHashCells uint64) *Storage {
	if initialHeapBytes == 0 {
		initialHeapBytes = DefaultHeapBytes
	}
	if initialHashCells == 0 {
		initialHashCells = DefaultHashCells
	}
	return &Storage{
		entries:   make(map[string][]byte, initialHashCells),
		HeapBytes: initialHeapBytes,
		HashCells: initialHashCells,
	}
}

// PutMemLimit stores data with an optional memory limit.
func (s *Storage) PutMemLimit(data []byte, memLimit uint64) []byte {
	if s == nil {
		return nil
	}
	if s.entries == nil {
		s.entries = make(map[string][]byte)
	}
	key := string(data)
	if existing, ok := s.entries[key]; ok {
		return existing
	}
	if memLimit > 0 && s.size+uint64(len(data)) > memLimit {
		return nil
	}
	copyData := append([]byte(nil), data...)
	s.entries[key] = copyData
	s.size += uint64(len(copyData))
	return copyData
}

// Put stores data without a memory limit.
func (s *Storage) Put(data []byte) []byte {
	return s.PutMemLimit(data, 0)
}

// PutString stores a string without a memory limit.
func (s *Storage) PutString(str string) []byte {
	return s.PutMemLimit([]byte(str), 0)
}

// PutStringMemLimit stores a string with a memory limit.
func (s *Storage) PutStringMemLimit(str string, memLimit uint64) []byte {
	return s.PutMemLimit([]byte(str), memLimit)
}

// Empty clears the storage contents.
func (s *Storage) Empty() {
	if s == nil {
		return
	}
	s.entries = make(map[string][]byte, s.HashCells)
	s.size = 0
}

// Size returns the number of bytes stored.
func (s *Storage) Size() uint64 {
	if s == nil {
		return 0
	}
	return s.size
}

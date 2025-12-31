package btr

import (
	"encoding/binary"

	"github.com/wilhasse/innodb-go/fil"
)

const (
	externMarker         = 0xEE
	externHeaderSize     = 1 + 8 + 2
	externDefaultPrefix  = 16
	ExternFieldThreshold = 32
)

// RecGetExternallyStoredLen returns the external length when stored out of page.
func RecGetExternallyStoredLen(value []byte) int {
	if id, _, ok := decodeExternRef(value); ok {
		return len(fil.ExternGet(id))
	}
	return len(value)
}

// StoreBigRecExternFields stores a large field externally and returns a reference.
func StoreBigRecExternFields(value []byte, prefixLen int) []byte {
	if len(value) <= ExternFieldThreshold {
		return cloneBytes(value)
	}
	if prefixLen <= 0 || prefixLen > len(value) {
		prefixLen = externDefaultPrefix
		if prefixLen > len(value) {
			prefixLen = len(value)
		}
	}
	id := fil.ExternStore(value)
	return encodeExternRef(id, value[:prefixLen])
}

// FreeExternallyStoredField frees external storage for a field reference.
func FreeExternallyStoredField(value []byte) {
	if id, _, ok := decodeExternRef(value); ok {
		fil.ExternFree(id)
	}
}

// CopyExternallyStoredFieldPrefix returns the stored prefix or a slice of the value.
func CopyExternallyStoredFieldPrefix(value []byte, length int) []byte {
	if length < 0 {
		return nil
	}
	if _, prefix, ok := decodeExternRef(value); ok {
		if length == 0 || length > len(prefix) {
			length = len(prefix)
		}
		return cloneBytes(prefix[:length])
	}
	if length == 0 || length > len(value) {
		length = len(value)
	}
	return cloneBytes(value[:length])
}

// RecFreeExternallyStoredFields frees external fields referenced by a record.
func RecFreeExternallyStoredFields(values ...[]byte) {
	for _, value := range values {
		FreeExternallyStoredField(value)
	}
}

// GetExternallyStoredField resolves an external field reference.
func GetExternallyStoredField(value []byte) []byte {
	if id, _, ok := decodeExternRef(value); ok {
		return fil.ExternGet(id)
	}
	return cloneBytes(value)
}

func encodeExternRef(id uint64, prefix []byte) []byte {
	if len(prefix) > 0xFFFF {
		prefix = prefix[:0xFFFF]
	}
	buf := make([]byte, externHeaderSize+len(prefix))
	buf[0] = externMarker
	binary.BigEndian.PutUint64(buf[1:], id)
	binary.BigEndian.PutUint16(buf[9:], uint16(len(prefix)))
	copy(buf[externHeaderSize:], prefix)
	return buf
}

func decodeExternRef(value []byte) (uint64, []byte, bool) {
	if len(value) < externHeaderSize || value[0] != externMarker {
		return 0, nil, false
	}
	id := binary.BigEndian.Uint64(value[1:])
	prefixLen := int(binary.BigEndian.Uint16(value[9:]))
	if externHeaderSize+prefixLen > len(value) {
		return 0, nil, false
	}
	prefix := value[externHeaderSize : externHeaderSize+prefixLen]
	return id, prefix, true
}

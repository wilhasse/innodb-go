package rec

import "encoding/binary"

// NodePtrEncode encodes a node pointer record (child page no + key bytes).
func NodePtrEncode(childPageNo uint32, key []byte) []byte {
	buf := make([]byte, 4+len(key))
	binary.BigEndian.PutUint32(buf[:4], childPageNo)
	copy(buf[4:], key)
	return buf
}

// NodePtrDecode decodes a node pointer record.
func NodePtrDecode(rec []byte) (uint32, []byte, bool) {
	if len(rec) < 4 {
		return 0, nil, false
	}
	child := binary.BigEndian.Uint32(rec[:4])
	key := make([]byte, len(rec[4:]))
	copy(key, rec[4:])
	return child, key, true
}

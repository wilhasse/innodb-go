package btr

import "github.com/wilhasse/innodb-go/rec"

// NodePtrBytes builds a node pointer record for a child page and key.
func NodePtrBytes(childPageNo uint32, key []byte) []byte {
	return rec.NodePtrEncode(childPageNo, key)
}

// NodePtrBytesDecode parses a node pointer record.
func NodePtrBytesDecode(recBytes []byte) (uint32, []byte, bool) {
	return rec.NodePtrDecode(recBytes)
}

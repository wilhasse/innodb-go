package fsp

import (
	"encoding/binary"
	"errors"
)

type nodeMeta struct {
	name      string
	sizePages uint64
}

func readNodeMetadata(page []byte) ([]nodeMeta, error) {
	if nodeMetaOffset+4 > len(page) {
		return nil, errors.New("fsp: node metadata out of bounds")
	}
	off := nodeMetaOffset
	count := binary.BigEndian.Uint32(page[off : off+4])
	off += 4
	nodes := make([]nodeMeta, 0, count)
	for i := uint32(0); i < count; i++ {
		if off+2+4 > len(page) {
			return nil, errors.New("fsp: node metadata truncated")
		}
		nameLen := binary.BigEndian.Uint16(page[off : off+2])
		off += 2
		sizePages := binary.BigEndian.Uint32(page[off : off+4])
		off += 4
		if off+int(nameLen) > len(page) {
			return nil, errors.New("fsp: node metadata truncated")
		}
		name := string(page[off : off+int(nameLen)])
		off += int(nameLen)
		nodes = append(nodes, nodeMeta{name: name, sizePages: uint64(sizePages)})
	}
	return nodes, nil
}

func writeNodeMetadata(page []byte, nodes []nodeMeta) error {
	if nodeMetaOffset+4 > len(page) {
		return errors.New("fsp: node metadata out of bounds")
	}
	off := nodeMetaOffset
	if len(nodes) > int(^uint32(0)) {
		return errors.New("fsp: node metadata count overflow")
	}
	binary.BigEndian.PutUint32(page[off:off+4], uint32(len(nodes)))
	off += 4
	for _, node := range nodes {
		nameBytes := []byte(node.name)
		if len(nameBytes) > int(^uint16(0)) {
			return errors.New("fsp: node metadata name too long")
		}
		if node.sizePages > uint64(^uint32(0)) {
			return errors.New("fsp: node metadata size too large")
		}
		if off+2+4+len(nameBytes) > len(page) {
			return errors.New("fsp: node metadata overflow")
		}
		binary.BigEndian.PutUint16(page[off:off+2], uint16(len(nameBytes)))
		off += 2
		binary.BigEndian.PutUint32(page[off:off+4], uint32(node.sizePages))
		off += 4
		copy(page[off:off+len(nameBytes)], nameBytes)
		off += len(nameBytes)
	}
	clear(page[off:])
	return nil
}

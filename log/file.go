package log

import (
	"encoding/binary"
	"errors"
	"fmt"
	"path/filepath"

	ibos "github.com/wilhasse/innodb-go/os"
)

const (
	logFileMagic   uint32 = 0x49424C47 // "IBLG"
	logFileVersion uint32 = 1
	logHeaderSize         = 64
	logFilePrefix         = "ib_logfile"
)

type logHeader struct {
	Magic         uint32
	Version       uint32
	StartLSN      uint64
	CheckpointLSN uint64
	FlushedLSN    uint64
	CurrentLSN    uint64
	FileSize      uint64
}

func logFilePath(dir string, index int) string {
	return filepath.Join(dir, fmt.Sprintf("%s%d", logFilePrefix, index))
}

func newLogHeader(cfg Config) logHeader {
	fileSize := cfg.FileSize
	if fileSize == 0 {
		fileSize = 4 << 20
	}
	return logHeader{
		Magic:         logFileMagic,
		Version:       logFileVersion,
		StartLSN:      0,
		CheckpointLSN: 0,
		FlushedLSN:    0,
		CurrentLSN:    0,
		FileSize:      fileSize,
	}
}

func encodeLogHeader(h logHeader) []byte {
	buf := make([]byte, logHeaderSize)
	binary.BigEndian.PutUint32(buf[0:], h.Magic)
	binary.BigEndian.PutUint32(buf[4:], h.Version)
	binary.BigEndian.PutUint64(buf[8:], h.StartLSN)
	binary.BigEndian.PutUint64(buf[16:], h.CheckpointLSN)
	binary.BigEndian.PutUint64(buf[24:], h.FlushedLSN)
	binary.BigEndian.PutUint64(buf[32:], h.CurrentLSN)
	binary.BigEndian.PutUint64(buf[40:], h.FileSize)
	return buf
}

func decodeLogHeader(buf []byte) (logHeader, error) {
	if len(buf) < logHeaderSize {
		return logHeader{}, errors.New("log: short header")
	}
	h := logHeader{
		Magic:         binary.BigEndian.Uint32(buf[0:]),
		Version:       binary.BigEndian.Uint32(buf[4:]),
		StartLSN:      binary.BigEndian.Uint64(buf[8:]),
		CheckpointLSN: binary.BigEndian.Uint64(buf[16:]),
		FlushedLSN:    binary.BigEndian.Uint64(buf[24:]),
		CurrentLSN:    binary.BigEndian.Uint64(buf[32:]),
		FileSize:      binary.BigEndian.Uint64(buf[40:]),
	}
	if h.Magic != logFileMagic || h.Version != logFileVersion {
		return logHeader{}, errors.New("log: invalid header")
	}
	return h, nil
}

func writeLogHeader(file ibos.File, hdr logHeader) error {
	buf := encodeLogHeader(hdr)
	_, err := ibos.FileWriteAt(file, buf, 0)
	return err
}

func readLogHeader(file ibos.File) (logHeader, error) {
	buf := make([]byte, logHeaderSize)
	if _, err := ibos.FileReadAt(file, buf, 0); err != nil {
		return logHeader{}, err
	}
	return decodeLogHeader(buf)
}

func openLogFile(cfg Config) (ibos.File, logHeader, error) {
	dir := resolveLogDir(cfg)
	path := logFilePath(dir, 0)
	exists, err := ibos.FileExists(path)
	if err != nil {
		return nil, logHeader{}, err
	}
	if !exists {
		if err := ibos.FileCreateSubdirsIfNeeded(path); err != nil {
			return nil, logHeader{}, err
		}
		file, err := ibos.FileCreateSimple(path, ibos.FileCreate, ibos.FileReadWrite)
		if err != nil {
			return nil, logHeader{}, err
		}
		hdr := newLogHeader(cfg)
		if err := writeLogHeader(file, hdr); err != nil {
			_ = ibos.FileClose(file)
			return nil, logHeader{}, err
		}
		return file, hdr, nil
	}
	file, err := ibos.FileCreateSimple(path, ibos.FileOpen, ibos.FileReadWrite)
	if err != nil {
		return nil, logHeader{}, err
	}
	hdr, err := readLogHeader(file)
	if err != nil {
		_ = ibos.FileClose(file)
		return nil, logHeader{}, err
	}
	return file, hdr, nil
}

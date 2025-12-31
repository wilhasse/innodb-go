package page

import (
	"bytes"
	"compress/zlib"
	"errors"
	"io"
)

// ZipPage stores a compressed page payload.
type ZipPage struct {
	Data         []byte
	OriginalSize int
}

// ZipCompress compresses data using zlib.
func ZipCompress(data []byte, level int) (*ZipPage, error) {
	if data == nil {
		return nil, errors.New("page: nil data")
	}
	if level == 0 {
		level = zlib.DefaultCompression
	}
	var buf bytes.Buffer
	writer, err := zlib.NewWriterLevel(&buf, level)
	if err != nil {
		return nil, err
	}
	if _, err := writer.Write(data); err != nil {
		writer.Close()
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return &ZipPage{Data: buf.Bytes(), OriginalSize: len(data)}, nil
}

// ZipDecompress inflates a compressed page payload.
func ZipDecompress(zip *ZipPage) ([]byte, error) {
	if zip == nil {
		return nil, errors.New("page: nil zip")
	}
	reader, err := zlib.NewReader(bytes.NewReader(zip.Data))
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	out, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	if zip.OriginalSize > 0 && len(out) != zip.OriginalSize {
		return nil, errors.New("page: decompressed size mismatch")
	}
	return out, nil
}

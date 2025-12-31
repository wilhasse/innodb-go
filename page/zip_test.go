package page

import (
	"bytes"
	"testing"
)

func TestZipCompressRoundTrip(t *testing.T) {
	data := bytes.Repeat([]byte("A"), 4096)
	zip, err := ZipCompress(data, 0)
	if err != nil {
		t.Fatalf("compress: %v", err)
	}
	out, err := ZipDecompress(zip)
	if err != nil {
		t.Fatalf("decompress: %v", err)
	}
	if !bytes.Equal(out, data) {
		t.Fatalf("round-trip mismatch")
	}
}

func TestZipCompressSize(t *testing.T) {
	data := bytes.Repeat([]byte("B"), 8192)
	zip, err := ZipCompress(data, 0)
	if err != nil {
		t.Fatalf("compress: %v", err)
	}
	if len(zip.Data) >= len(data) {
		t.Fatalf("expected compression to reduce size")
	}
}

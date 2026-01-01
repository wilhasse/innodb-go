package api

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/wilhasse/innodb-go/fsp"
)

type dataFileSpec struct {
	path                     string
	sizeBytes                uint64
	autoextend               bool
	autoextendIncrementBytes uint64
}

func openSystemTablespace() ErrCode {
	var spec string
	if err := CfgGet("data_file_path", &spec); err != DB_SUCCESS {
		return err
	}
	spec = strings.TrimSpace(spec)
	if spec == "" {
		return DB_SUCCESS
	}
	parsed, err := parseDataFilePathSpec(spec)
	if err != nil {
		Log(nil, "InnoDB: invalid data_file_path: %v\n", err)
		return DB_INVALID_INPUT
	}
	if parsed.path == "" || parsed.sizeBytes == 0 {
		return DB_SUCCESS
	}
	if !filepath.IsAbs(parsed.path) {
		parsed.path = filepath.Join(dataHomeDir(), parsed.path)
	}
	if parsed.autoextend && parsed.autoextendIncrementBytes == 0 {
		var inc Ulint
		if err := CfgGet("autoextend_increment", &inc); err == DB_SUCCESS {
			parsed.autoextendIncrementBytes = uint64(inc) << 20
		}
	}
	if err := fsp.OpenSystemTablespace(fsp.SystemTablespaceSpec{
		Path:                     parsed.path,
		SizeBytes:                parsed.sizeBytes,
		Autoextend:               parsed.autoextend,
		AutoextendIncrementBytes: parsed.autoextendIncrementBytes,
	}); err != nil {
		Log(nil, "InnoDB: failed to open system tablespace: %v\n", err)
		return DB_ERROR
	}
	return DB_SUCCESS
}

func closeSystemTablespace() {
	_ = fsp.CloseSystemTablespace()
}

func parseDataFilePathSpec(spec string) (dataFileSpec, error) {
	parts := strings.Split(spec, ";")
	head := strings.TrimSpace(parts[0])
	if head == "" {
		return dataFileSpec{}, nil
	}
	tokens := strings.Split(head, ":")
	if len(tokens) < 2 {
		return dataFileSpec{}, fmt.Errorf("missing size in %q", spec)
	}
	path := strings.TrimSpace(tokens[0])
	if path == "" {
		return dataFileSpec{}, fmt.Errorf("missing path in %q", spec)
	}
	sizeBytes, err := parseSizeBytes(tokens[1])
	if err != nil {
		return dataFileSpec{}, err
	}
	parsed := dataFileSpec{
		path:      path,
		sizeBytes: sizeBytes,
	}
	for i := 2; i < len(tokens); i++ {
		token := strings.TrimSpace(tokens[i])
		if token == "" {
			continue
		}
		if strings.EqualFold(token, "autoextend") {
			parsed.autoextend = true
			if i+1 < len(tokens) {
				if inc, err := parseSizeBytes(tokens[i+1]); err == nil && inc > 0 {
					parsed.autoextendIncrementBytes = inc
					i++
				}
			}
			continue
		}
		return dataFileSpec{}, fmt.Errorf("unsupported option %q", token)
	}
	return parsed, nil
}

func parseSizeBytes(raw string) (uint64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, fmt.Errorf("empty size")
	}
	suffix := raw[len(raw)-1]
	mult := uint64(1)
	switch suffix {
	case 'k', 'K':
		mult = 1 << 10
		raw = raw[:len(raw)-1]
	case 'm', 'M':
		mult = 1 << 20
		raw = raw[:len(raw)-1]
	case 'g', 'G':
		mult = 1 << 30
		raw = raw[:len(raw)-1]
	}
	val, err := strconv.ParseUint(strings.TrimSpace(raw), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid size %q", raw)
	}
	return val * mult, nil
}

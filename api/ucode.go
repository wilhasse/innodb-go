package api

import (
	"strings"
	"unicode"
)

// UcodeGetConnectionCharset returns the connection charset (stub).
func UcodeGetConnectionCharset() *Charset {
	return nil
}

// UcodeGetCharset returns a charset descriptor by ID (stub).
func UcodeGetCharset(_ Ulint) *Charset {
	return nil
}

// UcodeGetCharsetWidth reports min/max byte length for a charset.
func UcodeGetCharsetWidth(_ *Charset, mbminlen, mbmaxlen *Ulint) {
	if mbminlen != nil {
		*mbminlen = 0
	}
	if mbmaxlen != nil {
		*mbmaxlen = 0
	}
}

// Utf8Strcasecmp compares strings case-insensitively.
func Utf8Strcasecmp(a, b string) int {
	return strings.Compare(strings.ToLower(a), strings.ToLower(b))
}

// Utf8Strncasecmp compares up to n bytes of two strings case-insensitively.
func Utf8Strncasecmp(a, b string, n Ulint) int {
	if n == 0 {
		return 0
	}
	limit := int(n)
	la := strings.ToLower(a)
	lb := strings.ToLower(b)
	aCut := la
	if len(aCut) > limit {
		aCut = aCut[:limit]
	}
	bCut := lb
	if len(bCut) > limit {
		bCut = bCut[:limit]
	}
	if cmp := strings.Compare(aCut, bCut); cmp != 0 {
		return cmp
	}
	if len(la) >= limit || len(lb) >= limit {
		return 0
	}
	switch {
	case len(la) < len(lb):
		return -1
	case len(la) > len(lb):
		return 1
	default:
		return 0
	}
}

// Utf8Casedown lowercases ASCII bytes in-place.
func Utf8Casedown(buf []byte) {
	for i, b := range buf {
		r := rune(b)
		if r >= 'A' && r <= 'Z' {
			buf[i] = byte(unicode.ToLower(r))
		}
	}
}

// Utf8ConvertFromTableID copies an identifier into the destination buffer.
func Utf8ConvertFromTableID(_ *Charset, to []byte, from string, toLen Ulint) {
	copyWithLimit(to, from, toLen)
}

// Utf8ConvertFromID copies an identifier into the destination buffer.
func Utf8ConvertFromID(_ *Charset, to []byte, from string, toLen Ulint) {
	copyWithLimit(to, from, toLen)
}

// Utf8Isspace reports whether the byte is a space.
func Utf8Isspace(_ *Charset, c byte) Bool {
	if unicode.IsSpace(rune(c)) {
		return IBTrue
	}
	return IBFalse
}

// UcodeGetStorageSize returns the storage size for a prefix.
func UcodeGetStorageSize(_ *Charset, prefixLen, strLen Ulint, _ string) Ulint {
	return minUlint(prefixLen, strLen)
}

func copyWithLimit(dst []byte, src string, limit Ulint) {
	n := len(dst)
	if limit > 0 && int(limit) < n {
		n = int(limit)
	}
	if n <= 0 {
		return
	}
	copied := copy(dst[:n], src)
	for i := copied; i < n; i++ {
		dst[i] = 0
	}
}

func minUlint(a, b Ulint) Ulint {
	if a < b {
		return a
	}
	return b
}

package btr

import "errors"

// ErrDuplicateKey signals a duplicate key.
var ErrDuplicateKey = errors.New("btr: duplicate key")

// UniqueCheckBytes reports whether a duplicate key exists on the page.
func UniqueCheckBytes(pageBytes []byte, keyRec []byte, nFields int) bool {
	_, exact := SearchRecordBytes(pageBytes, keyRec, nFields)
	return exact
}

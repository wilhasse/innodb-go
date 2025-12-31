package api

import (
	"encoding/binary"
	"errors"
	"strings"

	"github.com/wilhasse/innodb-go/data"
	"github.com/wilhasse/innodb-go/row"
	"github.com/wilhasse/innodb-go/trx"
)

// LockMode mirrors cursor lock modes.
type LockMode int

const (
	LockIX LockMode = iota + 1
	LockIS
)

// CursorMode controls cursor movement.
type CursorMode int

const (
	CursorGE CursorMode = iota
	CursorG
)

// Cursor provides simple table iteration.
type Cursor struct {
	Table *Table
	pos   int
	Trx   *trx.Trx
}

// CursorOpenTable opens a cursor on a table.
func CursorOpenTable(name string, trx *trx.Trx, out **Cursor) ErrCode {
	if out == nil {
		return DB_ERROR
	}
	table := findTable(name)
	if table == nil {
		return DB_TABLE_NOT_FOUND
	}
	*out = &Cursor{Table: table, pos: 0, Trx: trx}
	return DB_SUCCESS
}

// CursorClose closes a cursor.
func CursorClose(_ *Cursor) ErrCode {
	return DB_SUCCESS
}

// CursorLock is a no-op for the in-memory cursor.
func CursorLock(_ *Cursor, _ LockMode) ErrCode {
	return DB_SUCCESS
}

// CursorAttachTrx binds a transaction to a cursor.
func CursorAttachTrx(crsr *Cursor, trx *trx.Trx) ErrCode {
	if crsr == nil {
		return DB_ERROR
	}
	crsr.Trx = trx
	return DB_SUCCESS
}

// CursorReset resets cursor position.
func CursorReset(crsr *Cursor) ErrCode {
	if crsr == nil {
		return DB_ERROR
	}
	crsr.pos = 0
	return DB_SUCCESS
}

// CursorInsertRow inserts a tuple via the cursor.
func CursorInsertRow(crsr *Cursor, tpl *data.Tuple) ErrCode {
	if crsr == nil || crsr.Table == nil || crsr.Table.Store == nil {
		return DB_ERROR
	}
	cloned := cloneTuple(tpl)
	if err := crsr.Table.Store.Insert(cloned); err != nil {
		if errors.Is(err, row.ErrDuplicateKey) {
			return DB_DUPLICATE_KEY
		}
		return DB_ERROR
	}
	return DB_SUCCESS
}

// CursorFirst positions the cursor at the first row.
func CursorFirst(crsr *Cursor) ErrCode {
	if crsr == nil || crsr.Table == nil {
		return DB_ERROR
	}
	crsr.pos = 0
	if len(crsr.Table.Store.Rows) == 0 {
		return DB_RECORD_NOT_FOUND
	}
	return DB_SUCCESS
}

// CursorNext advances the cursor.
func CursorNext(crsr *Cursor) ErrCode {
	if crsr == nil || crsr.Table == nil {
		return DB_ERROR
	}
	crsr.pos++
	if crsr.pos >= len(crsr.Table.Store.Rows) {
		return DB_END_OF_INDEX
	}
	return DB_SUCCESS
}

// CursorReadRow reads the current row into tpl.
func CursorReadRow(crsr *Cursor, tpl *data.Tuple) ErrCode {
	if crsr == nil || crsr.Table == nil || tpl == nil {
		return DB_ERROR
	}
	if crsr.pos < 0 || crsr.pos >= len(crsr.Table.Store.Rows) {
		return DB_RECORD_NOT_FOUND
	}
	row := crsr.Table.Store.Rows[crsr.pos]
	copyTuple(tpl, row)
	return DB_SUCCESS
}

// CursorMoveTo positions the cursor based on a search tuple.
func CursorMoveTo(crsr *Cursor, tpl *data.Tuple, mode CursorMode, ret *int) ErrCode {
	if crsr == nil || crsr.Table == nil || tpl == nil {
		return DB_ERROR
	}
	search, err := tupleReadI32(tpl, 0)
	if err != DB_SUCCESS {
		return err
	}
	for i, row := range crsr.Table.Store.Rows {
		val, err := tupleReadI32(row, 0)
		if err != DB_SUCCESS {
			continue
		}
		switch mode {
		case CursorGE:
			if val >= search {
				crsr.pos = i
				if ret != nil {
					if val == search {
						*ret = 0
					} else {
						*ret = -1
					}
				}
				return DB_SUCCESS
			}
		case CursorG:
			if val > search {
				crsr.pos = i
				if ret != nil {
					*ret = -1
				}
				return DB_SUCCESS
			}
		}
	}
	return DB_RECORD_NOT_FOUND
}

// ClustReadTupleCreate allocates a read tuple for the cursor.
func ClustReadTupleCreate(crsr *Cursor) *data.Tuple {
	return newTupleForCursor(crsr)
}

// ClustSearchTupleCreate allocates a search tuple for the cursor.
func ClustSearchTupleCreate(crsr *Cursor) *data.Tuple {
	return newTupleForCursor(crsr)
}

// TupleWriteI32 writes an int32 value into a tuple.
func TupleWriteI32(tpl *data.Tuple, col int, val int32) ErrCode {
	if tpl == nil || col < 0 || col >= len(tpl.Fields) {
		return DB_ERROR
	}
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], uint32(val))
	tpl.Fields[col].Data = append([]byte(nil), buf[:]...)
	tpl.Fields[col].Len = 4
	return DB_SUCCESS
}

// TupleReadI32 reads an int32 value from a tuple.
func TupleReadI32(tpl *data.Tuple, col int, out *int32) ErrCode {
	if tpl == nil || out == nil || col < 0 || col >= len(tpl.Fields) {
		return DB_ERROR
	}
	field := tpl.Fields[col]
	if len(field.Data) < 4 {
		return DB_ERROR
	}
	*out = int32(binary.BigEndian.Uint32(field.Data))
	return DB_SUCCESS
}

// TupleClear resets tuple fields.
func TupleClear(tpl *data.Tuple) *data.Tuple {
	if tpl == nil {
		return nil
	}
	for i := range tpl.Fields {
		tpl.Fields[i].Data = nil
		tpl.Fields[i].Len = 0
	}
	return tpl
}

// TupleDelete releases a tuple.
func TupleDelete(tpl *data.Tuple) {
	unregisterTupleMeta(tpl)
}

func newTupleForCursor(crsr *Cursor) *data.Tuple {
	n := 0
	if crsr != nil && crsr.Table != nil && crsr.Table.Schema != nil {
		n = len(crsr.Table.Schema.Columns)
	}
	if n == 0 {
		n = 1
	}
	fields := make([]data.Field, n)
	tpl := &data.Tuple{
		NFields:    n,
		NFieldsCmp: n,
		Fields:     fields,
		Magic:      data.DataTupleMagic,
	}
	if crsr != nil && crsr.Table != nil && crsr.Table.Schema != nil {
		registerTupleMeta(tpl, crsr.Table.Schema)
	}
	return tpl
}

func tupleReadI32(tpl *data.Tuple, col int) (int32, ErrCode) {
	var out int32
	if err := TupleReadI32(tpl, col, &out); err != DB_SUCCESS {
		return 0, err
	}
	return out, DB_SUCCESS
}

func copyTuple(dst, src *data.Tuple) {
	if dst == nil || src == nil {
		return
	}
	dst.InfoBits = src.InfoBits
	dst.Magic = src.Magic
	if len(dst.Fields) != len(src.Fields) {
		dst.Fields = make([]data.Field, len(src.Fields))
		dst.NFields = len(src.Fields)
		dst.NFieldsCmp = len(src.Fields)
	}
	for i := range src.Fields {
		srcField := src.Fields[i]
		dataBytes := append([]byte(nil), srcField.Data...)
		dst.Fields[i] = data.Field{
			Data: dataBytes,
			Len:  srcField.Len,
			Ext:  srcField.Ext,
			Type: srcField.Type,
		}
	}
}

func cloneTuple(src *data.Tuple) *data.Tuple {
	if src == nil {
		return nil
	}
	dst := &data.Tuple{
		InfoBits:   src.InfoBits,
		NFields:    len(src.Fields),
		NFieldsCmp: src.NFieldsCmp,
		Magic:      src.Magic,
		Fields:     make([]data.Field, len(src.Fields)),
	}
	copyTuple(dst, src)
	return dst
}

func findTable(name string) *Table {
	schemaMu.Lock()
	defer schemaMu.Unlock()
	for _, db := range databases {
		if table := db.Tables[strings.ToLower(name)]; table != nil {
			return table
		}
	}
	return nil
}

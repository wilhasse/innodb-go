package api

import (
	"bytes"
	"encoding/binary"
	"errors"
	"strings"

	"github.com/wilhasse/innodb-go/btr"
	"github.com/wilhasse/innodb-go/data"
	"github.com/wilhasse/innodb-go/rec"
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

// MatchMode mirrors ib_match_t.
type MatchMode int

const (
	IB_EXACT_MATCH MatchMode = iota
	IB_CLOSEST_MATCH
	IB_EXACT_PREFIX
)

// Cursor provides simple table iteration.
type Cursor struct {
	Table     *Table
	Tree      *btr.Tree
	treeCur   *btr.Cursor
	pcur      *btr.Pcur
	lastKey   []byte
	Trx       *trx.Trx
	MatchMode MatchMode
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
	var tree *btr.Tree
	if table.Store != nil {
		tree = table.Store.Tree
	}
	cursor := &Cursor{Table: table, Tree: tree, Trx: trx, MatchMode: IB_CLOSEST_MATCH}
	if tree != nil {
		cursor.pcur = btr.NewPcur(tree)
	}
	*out = cursor
	return DB_SUCCESS
}

// CursorClose closes a cursor.
func CursorClose(crsr *Cursor) ErrCode {
	if crsr != nil && crsr.pcur != nil {
		crsr.pcur.Free()
	}
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
	if crsr.pcur != nil {
		crsr.pcur.Init()
	}
	crsr.treeCur = nil
	crsr.lastKey = nil
	return DB_SUCCESS
}

// CursorInsertRow inserts a tuple via the cursor.
func CursorInsertRow(crsr *Cursor, tpl *data.Tuple) ErrCode {
	if crsr == nil || crsr.Table == nil || crsr.Table.Store == nil {
		return DB_ERROR
	}
	if err := validateNotNull(crsr, tpl); err != DB_SUCCESS {
		return err
	}
	encoded, err := encodeDecodeTuple(tpl)
	if err != DB_SUCCESS {
		return err
	}
	if err := crsr.Table.Store.Insert(encoded); err != nil {
		if errors.Is(err, row.ErrDuplicateKey) {
			return DB_DUPLICATE_KEY
		}
		return DB_ERROR
	}
	recordUndoInsert(crsr, encoded)
	return DB_SUCCESS
}

func validateNotNull(crsr *Cursor, tpl *data.Tuple) ErrCode {
	if crsr == nil || crsr.Table == nil || crsr.Table.Schema == nil || tpl == nil {
		return DB_ERROR
	}
	for i, col := range crsr.Table.Schema.Columns {
		if col.Attr&IB_COL_NOT_NULL == 0 {
			continue
		}
		if i >= len(tpl.Fields) {
			return DB_DATA_MISMATCH
		}
		field := tpl.Fields[i]
		if field.Len == data.UnivSQLNull || (field.Len == 0 && len(field.Data) == 0) {
			return DB_DATA_MISMATCH
		}
	}
	return DB_SUCCESS
}

// CursorFirst positions the cursor at the first row.
func CursorFirst(crsr *Cursor) ErrCode {
	if crsr == nil || crsr.Table == nil || crsr.Tree == nil {
		return DB_ERROR
	}
	pcur := ensurePcur(crsr)
	if pcur == nil {
		return DB_ERROR
	}
	if !pcur.OpenAtIndexSide(true) {
		return DB_RECORD_NOT_FOUND
	}
	crsr.treeCur = pcur.Cur.Cursor
	if crsr.treeCur == nil || !crsr.treeCur.Valid() {
		return DB_RECORD_NOT_FOUND
	}
	crsr.lastKey = crsr.treeCur.Key()
	return DB_SUCCESS
}

// CursorNext advances the cursor.
func CursorNext(crsr *Cursor) ErrCode {
	if crsr == nil || crsr.Table == nil || crsr.Tree == nil {
		return DB_ERROR
	}
	pcur := ensurePcur(crsr)
	if pcur == nil || pcur.Cur == nil {
		return DB_ERROR
	}
	if pcur.Cur.Valid() {
		crsr.lastKey = pcur.Cur.Key()
		if !pcur.Cur.Next() {
			crsr.treeCur = nil
			return DB_END_OF_INDEX
		}
		crsr.treeCur = pcur.Cur.Cursor
		crsr.lastKey = pcur.Cur.Key()
		return DB_SUCCESS
	}
	if len(crsr.lastKey) == 0 {
		return DB_END_OF_INDEX
	}
	if !pcur.OpenOnUserRec(crsr.lastKey, btr.SearchGE) {
		return DB_END_OF_INDEX
	}
	if pcur.Cur.Valid() && row.CompareKeys(pcur.Cur.Key(), crsr.lastKey) == 0 {
		if !pcur.Cur.Next() {
			crsr.treeCur = nil
			return DB_END_OF_INDEX
		}
	}
	if !pcur.Cur.Valid() {
		return DB_END_OF_INDEX
	}
	crsr.treeCur = pcur.Cur.Cursor
	crsr.lastKey = pcur.Cur.Key()
	return DB_SUCCESS
}

// CursorReadRow reads the current row into tpl.
func CursorReadRow(crsr *Cursor, tpl *data.Tuple) ErrCode {
	if crsr == nil || crsr.Table == nil || crsr.Tree == nil || tpl == nil {
		return DB_ERROR
	}
	if crsr.treeCur == nil || !crsr.treeCur.Valid() {
		return DB_RECORD_NOT_FOUND
	}
	value := crsr.treeCur.Value()
	if len(value) == 0 {
		return DB_RECORD_NOT_FOUND
	}
	recBytes := value
	if len(value) >= 8 {
		recBytes = value[8:]
	}
	if len(recBytes) == 0 {
		row, ok := cursorRow(crsr)
		if !ok {
			return DB_RECORD_NOT_FOUND
		}
		copyTuple(tpl, row)
		return DB_SUCCESS
	}
	nFields := len(tpl.Fields)
	if nFields == 0 && crsr.Table != nil && crsr.Table.Schema != nil {
		nFields = len(crsr.Table.Schema.Columns)
	}
	decoded, err := rec.DecodeVar(recBytes, nFields, 0)
	if err != nil {
		row, ok := cursorRow(crsr)
		if !ok {
			return DB_ERROR
		}
		copyTuple(tpl, row)
		return DB_SUCCESS
	}
	copyTuple(tpl, decoded)
	return DB_SUCCESS
}

func cursorRow(crsr *Cursor) (*data.Tuple, bool) {
	if crsr == nil || crsr.Table == nil || crsr.Table.Store == nil {
		return nil, false
	}
	if crsr.treeCur == nil && crsr.pcur != nil && crsr.pcur.Cur != nil && crsr.pcur.Cur.Valid() {
		crsr.treeCur = crsr.pcur.Cur.Cursor
	}
	if crsr.treeCur == nil || !crsr.treeCur.Valid() {
		return nil, false
	}
	rowID, ok := row.DecodeRowID(crsr.treeCur.Value())
	if !ok {
		return nil, false
	}
	rowTuple := crsr.Table.Store.RowByID(rowID)
	if rowTuple == nil {
		return nil, false
	}
	return rowTuple, true
}

// CursorMoveTo positions the cursor based on a search tuple.
func CursorMoveTo(crsr *Cursor, tpl *data.Tuple, mode CursorMode, ret *int) ErrCode {
	if crsr == nil || crsr.Table == nil || crsr.Tree == nil || tpl == nil {
		return DB_ERROR
	}
	keyFields := searchFieldCount(tpl)
	if keyFields == 0 {
		return DB_ERROR
	}
	pkFields := primaryKeyCols(crsr.Table)
	if pkFields > 0 && keyFields > pkFields {
		keyFields = pkFields
	}
	exactRequired := crsr.MatchMode == IB_EXACT_MATCH
	prefixRequired := crsr.MatchMode == IB_EXACT_PREFIX
	if crsr.Table.Store == nil {
		return DB_ERROR
	}
	searchKey := crsr.Table.Store.KeyForSearch(tpl, keyFields)
	if len(searchKey) == 0 {
		return DB_ERROR
	}
	pcur := ensurePcur(crsr)
	if pcur == nil || pcur.Cur == nil {
		return DB_ERROR
	}
	if !pcur.Cur.Search(searchKey, btr.SearchGE) {
		return DB_RECORD_NOT_FOUND
	}
	for pcur.Cur.Valid() {
		rowID, ok := row.DecodeRowID(pcur.Cur.Value())
		if !ok {
			if !pcur.Cur.Next() {
				break
			}
			continue
		}
		rowTuple := crsr.Table.Store.RowByID(rowID)
		if rowTuple == nil {
			if !pcur.Cur.Next() {
				break
			}
			continue
		}
		cmp := compareTuplePrefix(rowTuple, tpl, keyFields)
		switch mode {
		case CursorGE:
			if cmp < 0 {
				if !pcur.Cur.Next() {
					return DB_RECORD_NOT_FOUND
				}
				continue
			}
		case CursorG:
			if cmp <= 0 {
				if !pcur.Cur.Next() {
					return DB_RECORD_NOT_FOUND
				}
				continue
			}
		}
		if prefixRequired && !tupleHasPrefix(rowTuple, tpl, keyFields) {
			if !pcur.Cur.Next() {
				return DB_RECORD_NOT_FOUND
			}
			continue
		}
		if exactRequired {
			if cmp != 0 || (pkFields > 0 && keyFields != pkFields) {
				if !pcur.Cur.Next() {
					return DB_RECORD_NOT_FOUND
				}
				continue
			}
		}
		crsr.treeCur = pcur.Cur.Cursor
		crsr.lastKey = pcur.Cur.Key()
		if ret != nil {
			if cmp == 0 && (pkFields == 0 || keyFields == pkFields) {
				*ret = 0
			} else {
				*ret = -1
			}
		}
		return DB_SUCCESS
	}
	return DB_RECORD_NOT_FOUND
}

func ensurePcur(crsr *Cursor) *btr.Pcur {
	if crsr == nil || crsr.Tree == nil {
		return nil
	}
	if crsr.pcur == nil {
		crsr.pcur = btr.NewPcur(crsr.Tree)
	} else if crsr.pcur.Cur == nil {
		crsr.pcur.Cur = btr.NewCur(crsr.Tree)
	}
	return crsr.pcur
}

func searchFieldCount(tpl *data.Tuple) int {
	if tpl == nil {
		return 0
	}
	for i := 0; i < len(tpl.Fields); i++ {
		field := tpl.Fields[i]
		if field.Len == data.UnivSQLNull {
			return i + 1
		}
		if field.Len == 0 && len(field.Data) == 0 {
			return i
		}
	}
	return len(tpl.Fields)
}

func primaryKeyCols(table *Table) int {
	if table == nil || table.Schema == nil {
		return 0
	}
	for _, idx := range table.Schema.Indexes {
		if idx != nil && idx.Clustered {
			return len(idx.Columns)
		}
	}
	return 0
}

func compareTuplePrefix(row, search *data.Tuple, n int) int {
	if row == nil || search == nil {
		switch {
		case row == search:
			return 0
		case row == nil:
			return -1
		default:
			return 1
		}
	}
	if n > len(row.Fields) {
		n = len(row.Fields)
	}
	if n > len(search.Fields) {
		n = len(search.Fields)
	}
	for i := 0; i < n; i++ {
		cmp := data.CompareFields(&row.Fields[i], &search.Fields[i])
		if cmp != 0 {
			return cmp
		}
	}
	return 0
}

func tupleHasPrefix(row, search *data.Tuple, n int) bool {
	if row == nil || search == nil {
		return false
	}
	if n > len(row.Fields) {
		n = len(row.Fields)
	}
	if n > len(search.Fields) {
		n = len(search.Fields)
	}
	for i := 0; i < n; i++ {
		if !fieldHasPrefix(row.Fields[i], search.Fields[i]) {
			return false
		}
	}
	return true
}

func fieldHasPrefix(row, search data.Field) bool {
	if search.Len == data.UnivSQLNull {
		return row.Len == data.UnivSQLNull
	}
	if search.Len == 0 && len(search.Data) == 0 {
		return true
	}
	if row.Len == data.UnivSQLNull {
		return false
	}
	slen := int(search.Len)
	if slen > len(search.Data) {
		slen = len(search.Data)
	}
	if slen > len(row.Data) {
		return false
	}
	return bytes.Equal(row.Data[:slen], search.Data[:slen])
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

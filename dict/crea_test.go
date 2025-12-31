package dict

import (
	"encoding/binary"
	"testing"

	"github.com/wilhasse/innodb-go/data"
)

func TestCreateSysTablesTuple(t *testing.T) {
	table := &Table{
		Name:  "t1",
		ID:    newDulint(0, 42),
		Space: 7,
		Columns: []Column{
			{Name: "c1"},
			{Name: "c2"},
		},
	}
	tuple := CreateSysTablesTuple(table)
	if tuple == nil || len(tuple.Fields) != 8 {
		t.Fatalf("expected tuple with 8 fields")
	}
	if string(tuple.Fields[0].Data) != "t1" {
		t.Fatalf("unexpected table name")
	}
	if binary.BigEndian.Uint64(tuple.Fields[1].Data) != dulintToUint64(table.ID) {
		t.Fatalf("unexpected table id")
	}
	if binary.BigEndian.Uint32(tuple.Fields[2].Data) != 2 {
		t.Fatalf("unexpected column count")
	}
	if binary.BigEndian.Uint32(tuple.Fields[7].Data) != table.Space {
		t.Fatalf("unexpected space id")
	}
}

func TestCreateSysColumnsTuple(t *testing.T) {
	table := &Table{
		Name:  "t1",
		ID:    newDulint(0, 1),
		Space: 0,
		Columns: []Column{
			{Name: "c1", Type: dataType()},
		},
	}
	tuple := CreateSysColumnsTuple(table, 0)
	if tuple == nil || len(tuple.Fields) != 7 {
		t.Fatalf("expected tuple with 7 fields")
	}
	if string(tuple.Fields[2].Data) != "c1" {
		t.Fatalf("unexpected column name")
	}
	if binary.BigEndian.Uint32(tuple.Fields[3].Data) != table.Columns[0].Type.MType {
		t.Fatalf("unexpected mtype")
	}
}

func TestCreateSysIndexesTuple(t *testing.T) {
	table := &Table{Name: "t1", ID: newDulint(0, 1), Space: 1}
	index := &Index{
		Name:      "idx",
		ID:        newDulint(0, 2),
		Fields:    []string{"c1"},
		Unique:    true,
		Clustered: true,
		RootPage:  99,
	}
	tuple := CreateSysIndexesTuple(table, index)
	if tuple == nil || len(tuple.Fields) != 7 {
		t.Fatalf("expected tuple with 7 fields")
	}
	if string(tuple.Fields[2].Data) != "idx" {
		t.Fatalf("unexpected index name")
	}
	if binary.BigEndian.Uint32(tuple.Fields[6].Data) != 99 {
		t.Fatalf("unexpected root page")
	}
}

func TestCreateSysFieldsTuple(t *testing.T) {
	index := &Index{
		ID:     newDulint(0, 2),
		Fields: []string{"c1", "c2"},
	}
	tuple := CreateSysFieldsTuple(index, 1)
	if tuple == nil || len(tuple.Fields) != 3 {
		t.Fatalf("expected tuple with 3 fields")
	}
	if string(tuple.Fields[2].Data) != "c2" {
		t.Fatalf("unexpected field name")
	}
}

func TestDictCreateTableIndex(t *testing.T) {
	DictInit()
	table := &Table{Name: "t1", Indexes: make(map[string]*Index)}
	if err := DictCreateTable(table); err != nil {
		t.Fatalf("unexpected create table error: %v", err)
	}
	if err := DictCreateTable(table); err != ErrTableExists {
		t.Fatalf("expected ErrTableExists, got %v", err)
	}
	index := &Index{Name: "idx"}
	if err := DictCreateIndex("t1", index); err != nil {
		t.Fatalf("unexpected create index error: %v", err)
	}
	if err := DictCreateIndex("t1", index); err != ErrIndexExists {
		t.Fatalf("expected ErrIndexExists, got %v", err)
	}
}

func dataType() data.DataType {
	return data.DataType{MType: data.DataVarchar, PrType: 0, Len: 10}
}

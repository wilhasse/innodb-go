package que

import (
	"testing"

	"github.com/wilhasse/innodb-go/data"
	"github.com/wilhasse/innodb-go/pars"
	"github.com/wilhasse/innodb-go/row"
)

func TestBuildGraphInsert(t *testing.T) {
	ctx, _ := newTestContext()
	stmt, err := pars.ParseSQL("INSERT INTO t (id,name) VALUES (1,'a')")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	graph, err := BuildGraph(stmt, ctx)
	if err != nil {
		t.Fatalf("BuildGraph: %v", err)
	}
	thr := ForkGetFirstThr(graph)
	node, ok := thr.Child.(*InsertNode)
	if !ok {
		t.Fatalf("expected InsertNode")
	}
	if got := string(node.Tuple.Fields[0].Data); got != "1" {
		t.Fatalf("id=%s", got)
	}
	if got := string(node.Tuple.Fields[1].Data); got != "a" {
		t.Fatalf("name=%s", got)
	}
}

func TestBuildGraphUpdate(t *testing.T) {
	ctx, store := newTestContext()
	row1 := makeSQLTuple("1", "a")
	if err := store.Insert(row1); err != nil {
		t.Fatalf("insert: %v", err)
	}
	stmt, err := pars.ParseSQL("UPDATE t SET name='b' WHERE id=1")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	graph, err := BuildGraph(stmt, ctx)
	if err != nil {
		t.Fatalf("BuildGraph: %v", err)
	}
	thr := ForkGetFirstThr(graph)
	node, ok := thr.Child.(*UpdateNode)
	if !ok {
		t.Fatalf("expected UpdateNode")
	}
	if got := string(node.NewTuple.Fields[1].Data); got != "b" {
		t.Fatalf("updated name=%s", got)
	}
}

func TestBuildGraphDelete(t *testing.T) {
	ctx, store := newTestContext()
	row1 := makeSQLTuple("1", "a")
	if err := store.Insert(row1); err != nil {
		t.Fatalf("insert: %v", err)
	}
	stmt, err := pars.ParseSQL("DELETE FROM t WHERE id=1")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	graph, err := BuildGraph(stmt, ctx)
	if err != nil {
		t.Fatalf("BuildGraph: %v", err)
	}
	thr := ForkGetFirstThr(graph)
	node, ok := thr.Child.(*DeleteNode)
	if !ok {
		t.Fatalf("expected DeleteNode")
	}
	if node.Tuple != row1 {
		t.Fatalf("expected delete target to match inserted row")
	}
}

func TestBuildGraphSelect(t *testing.T) {
	ctx, store := newTestContext()
	row1 := makeSQLTuple("1", "a")
	if err := store.Insert(row1); err != nil {
		t.Fatalf("insert: %v", err)
	}
	stmt, err := pars.ParseSQL("SELECT id,name FROM t WHERE id=1")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	graph, err := BuildGraph(stmt, ctx)
	if err != nil {
		t.Fatalf("BuildGraph: %v", err)
	}
	thr := ForkGetFirstThr(graph)
	node, ok := thr.Child.(*SelectNode)
	if !ok {
		t.Fatalf("expected SelectNode")
	}
	if len(node.Columns) != 2 || node.Columns[0] != 0 || node.Columns[1] != 1 {
		t.Fatalf("columns=%v", node.Columns)
	}
	if err := ForkRun(graph); err != nil {
		t.Fatalf("ForkRun: %v", err)
	}
	if len(node.Rows) != 1 {
		t.Fatalf("rows=%d", len(node.Rows))
	}
}

func newTestContext() (*BuildContext, *row.Store) {
	store := row.NewStore(0)
	ctx := &BuildContext{
		Tables: map[string]*TableContext{
			"t": {
				Store:   store,
				Columns: []string{"id", "name"},
			},
		},
	}
	return ctx, store
}

func makeSQLTuple(values ...string) *data.Tuple {
	tuple := data.NewTuple(len(values))
	for i, value := range values {
		tuple.Fields[i] = data.Field{
			Data: []byte(value),
			Len:  uint32(len(value)),
		}
	}
	return tuple
}

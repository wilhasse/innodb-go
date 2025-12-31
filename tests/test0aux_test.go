package tests

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/wilhasse/innodb-go/api"
	"github.com/wilhasse/innodb-go/data"
)

const (
	test0auxLogGroupHomeDir = "log"
	test0auxDataFilePath    = "ibdata1:32M:autoextend"
)

type configVar struct {
	name     string
	value    string
	hasValue bool
}

type config struct {
	elems []configVar
}

func readIntFromTuple(tpl *data.Tuple, colMeta *api.ColMeta, col int) (uint64, api.ErrCode) {
	if tpl == nil || colMeta == nil {
		return 0, api.DB_ERROR
	}
	switch colMeta.TypeLen {
	case 1:
		if colMeta.Attr&api.IB_COL_UNSIGNED != 0 {
			var v uint8
			if err := api.TupleReadU8(tpl, col, &v); err != api.DB_SUCCESS {
				return 0, err
			}
			return uint64(v), api.DB_SUCCESS
		}
		var v int8
		if err := api.TupleReadI8(tpl, col, &v); err != api.DB_SUCCESS {
			return 0, err
		}
		return uint64(v), api.DB_SUCCESS
	case 2:
		if colMeta.Attr&api.IB_COL_UNSIGNED != 0 {
			var v uint16
			if err := api.TupleReadU16(tpl, col, &v); err != api.DB_SUCCESS {
				return 0, err
			}
			return uint64(v), api.DB_SUCCESS
		}
		var v int16
		if err := api.TupleReadI16(tpl, col, &v); err != api.DB_SUCCESS {
			return 0, err
		}
		return uint64(v), api.DB_SUCCESS
	case 4:
		if colMeta.Attr&api.IB_COL_UNSIGNED != 0 {
			var v uint32
			if err := api.TupleReadU32(tpl, col, &v); err != api.DB_SUCCESS {
				return 0, err
			}
			return uint64(v), api.DB_SUCCESS
		}
		var v int32
		if err := api.TupleReadI32(tpl, col, &v); err != api.DB_SUCCESS {
			return 0, err
		}
		return uint64(v), api.DB_SUCCESS
	case 8:
		if colMeta.Attr&api.IB_COL_UNSIGNED != 0 {
			var v uint64
			if err := api.TupleReadU64(tpl, col, &v); err != api.DB_SUCCESS {
				return 0, err
			}
			return v, api.DB_SUCCESS
		}
		var v int64
		if err := api.TupleReadI64(tpl, col, &v); err != api.DB_SUCCESS {
			return 0, err
		}
		return uint64(v), api.DB_SUCCESS
	default:
		return 0, api.DB_ERROR
	}
}

func formatIntCol(tpl *data.Tuple, colMeta *api.ColMeta, col int) (string, api.ErrCode) {
	val, err := readIntFromTuple(tpl, colMeta, col)
	if err != api.DB_SUCCESS {
		return "", err
	}
	if colMeta.Attr&api.IB_COL_UNSIGNED != 0 {
		return strconv.FormatUint(val, 10), api.DB_SUCCESS
	}
	switch colMeta.TypeLen {
	case 1:
		return strconv.FormatInt(int64(int8(val)), 10), api.DB_SUCCESS
	case 2:
		return strconv.FormatInt(int64(int16(val)), 10), api.DB_SUCCESS
	case 4:
		return strconv.FormatInt(int64(int32(val)), 10), api.DB_SUCCESS
	case 8:
		return strconv.FormatInt(int64(val), 10), api.DB_SUCCESS
	default:
		return "", api.DB_ERROR
	}
}

func formatTuple(tpl *data.Tuple) (string, api.ErrCode) {
	if tpl == nil {
		return "", api.DB_ERROR
	}
	var b strings.Builder
	for i := 0; i < len(tpl.Fields); i++ {
		var colMeta api.ColMeta
		dataLen := api.ColGetMeta(tpl, i, &colMeta)
		if colMeta.Type == api.IB_SYS {
			continue
		}
		if dataLen == api.Ulint(api.IBSQLNull) {
			b.WriteString("|")
			continue
		}
		switch colMeta.Type {
		case api.IB_INT:
			out, err := formatIntCol(tpl, &colMeta, i)
			if err != api.DB_SUCCESS {
				return "", err
			}
			b.WriteString(out)
		case api.IB_FLOAT:
			var v float32
			if err := api.TupleReadFloat(tpl, i, &v); err != api.DB_SUCCESS {
				return "", err
			}
			fmt.Fprintf(&b, "%f", v)
		case api.IB_DOUBLE:
			var v float64
			if err := api.TupleReadDouble(tpl, i, &v); err != api.DB_SUCCESS {
				return "", err
			}
			fmt.Fprintf(&b, "%f", v)
		case api.IB_CHAR, api.IB_BLOB, api.IB_DECIMAL, api.IB_VARCHAR:
			length := int(dataLen)
			b.WriteString(strconv.Itoa(length))
			b.WriteString(":")
			if length > 0 {
				val := api.ColGetValue(tpl, i)
				if len(val) < length {
					length = len(val)
				}
				if length > 0 {
					b.Write(val[:length])
				}
			}
		default:
			return "", api.DB_ERROR
		}
		b.WriteString("|")
	}
	b.WriteString("\n")
	return b.String(), api.DB_SUCCESS
}

func printTuple(w io.Writer, tpl *data.Tuple) api.ErrCode {
	out, err := formatTuple(tpl)
	if err != api.DB_SUCCESS {
		return err
	}
	_, _ = io.WriteString(w, out)
	return api.DB_SUCCESS
}

func testConfigure() api.ErrCode {
	if err := os.MkdirAll(test0auxLogGroupHomeDir, 0o755); err != nil {
		return api.DB_ERROR
	}
	flushMethod := "o_direct"
	if runtime.GOOS == "windows" {
		flushMethod = "async_unbuffered"
	}
	if err := api.CfgSet("flush_method", flushMethod); err != api.DB_SUCCESS {
		return err
	}
	if err := api.CfgSet("log_files_in_group", 2); err != api.DB_SUCCESS {
		return err
	}
	if err := api.CfgSet("log_file_size", 32*1024*1024); err != api.DB_SUCCESS {
		return err
	}
	if err := api.CfgSet("log_buffer_size", 24*16384); err != api.DB_SUCCESS {
		return err
	}
	if err := api.CfgSet("buffer_pool_size", 5*1024*1024); err != api.DB_SUCCESS {
		return err
	}
	if err := api.CfgSet("additional_mem_pool_size", 4*1024*1024); err != api.DB_SUCCESS {
		return err
	}
	if err := api.CfgSet("flush_log_at_trx_commit", 1); err != api.DB_SUCCESS {
		return err
	}
	if err := api.CfgSet("file_io_threads", 4); err != api.DB_SUCCESS {
		return err
	}
	if err := api.CfgSet("lock_wait_timeout", 60); err != api.DB_SUCCESS {
		return err
	}
	if err := api.CfgSet("open_files", 300); err != api.DB_SUCCESS {
		return err
	}
	if err := api.CfgSet("doublewrite", true); err != api.DB_SUCCESS {
		return err
	}
	if err := api.CfgSet("checksums", true); err != api.DB_SUCCESS {
		return err
	}
	if err := api.CfgSet("rollback_on_timeout", true); err != api.DB_SUCCESS {
		return err
	}
	if err := api.CfgSet("print_verbose_log", true); err != api.DB_SUCCESS {
		return err
	}
	if err := api.CfgSet("file_per_table", true); err != api.DB_SUCCESS {
		return err
	}
	if err := api.CfgSet("data_home_dir", "./"); err != api.DB_SUCCESS {
		return err
	}
	if err := api.CfgSet("log_group_home_dir", test0auxLogGroupHomeDir); err != api.DB_SUCCESS {
		return err
	}
	if err := api.CfgSet("data_file_path", test0auxDataFilePath); err != api.DB_SUCCESS {
		return err
	}
	return api.DB_SUCCESS
}

func genRandText(rng *rand.Rand, max int) []byte {
	if max <= 1 {
		return nil
	}
	const charset = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
	nextInt := func(n int) int {
		if rng != nil {
			return rng.Intn(n)
		}
		return rand.Intn(n)
	}
	n := nextInt(max)
	if n == 0 {
		n = 1
	}
	buf := make([]byte, n)
	for i := range buf {
		buf[i] = charset[nextInt(len(charset))]
	}
	return buf
}

func parseUintArg(arg string) (uint64, bool) {
	v, err := strconv.ParseInt(strings.TrimSpace(arg), 10, 64)
	if err != nil {
		return 0, false
	}
	if v < 0 {
		v = -v
	}
	return uint64(v), true
}

func setGlobalOption(opt int, arg string) api.ErrCode {
	switch opt {
	case 1:
		size, ok := parseUintArg(arg)
		if !ok {
			return api.DB_INVALID_INPUT
		}
		return api.CfgSet("buffer_pool_size", size*1024*1024)
	case 2:
		size, ok := parseUintArg(arg)
		if !ok {
			return api.DB_INVALID_INPUT
		}
		return api.CfgSet("log_file_size", size*1024*1024)
	case 3:
		return api.CfgSet("adaptive_hash_index", false)
	case 4:
		size, ok := parseUintArg(arg)
		if !ok {
			return api.DB_INVALID_INPUT
		}
		return api.CfgSet("io_capacity", size)
	case 5:
		return api.CfgSet("use_sys_malloc", true)
	case 6:
		pct, ok := parseUintArg(arg)
		if !ok {
			return api.DB_INVALID_INPUT
		}
		return api.CfgSet("lru_old_blocks_pct", pct)
	case 7:
		pct, ok := parseUintArg(arg)
		if !ok {
			return api.DB_INVALID_INPUT
		}
		return api.CfgSet("lru_block_access_recency", pct)
	case 8:
		level, ok := parseUintArg(arg)
		if !ok {
			return api.DB_INVALID_INPUT
		}
		return api.CfgSet("force_recovery", level)
	case 9:
		return api.CfgSet("log_group_home_dir", arg)
	case 10:
		return api.CfgSet("data_home_dir", arg)
	case 11:
		return api.CfgSet("data_file_path", arg)
	case 12:
		return api.CfgSet("doublewrite", false)
	case 13:
		return api.CfgSet("checksums", false)
	case 14:
		return api.CfgSet("file_per_table", false)
	case 15:
		level, ok := parseUintArg(arg)
		if !ok {
			return api.DB_INVALID_INPUT
		}
		return api.CfgSet("flush_log_at_trx_commit", level)
	case 16:
		return api.CfgSet("flush_method", arg)
	case 17:
		threads, ok := parseUintArg(arg)
		if !ok {
			return api.DB_INVALID_INPUT
		}
		return api.CfgSet("file_io_threads", threads)
	case 18:
		threads, ok := parseUintArg(arg)
		if !ok {
			return api.DB_INVALID_INPUT
		}
		return api.CfgSet("file_io_threads", threads)
	case 19:
		count, ok := parseUintArg(arg)
		if !ok {
			return api.DB_INVALID_INPUT
		}
		return api.CfgSet("open_files", count)
	case 20:
		secs, ok := parseUintArg(arg)
		if !ok {
			return api.DB_INVALID_INPUT
		}
		return api.CfgSet("lock_wait_timeout", secs)
	default:
		return api.DB_ERROR
	}
}

func printUsage(w io.Writer, progName string) {
	fmt.Fprintf(w,
		"usage: %s "+
			"[--ib-buffer-pool-size size in mb]\n"+
			"[--ib-log-file-size size in mb]\n"+
			"[--ib-disable-ahi]\n"+
			"[--ib-io-capacity number of records]\n"+
			"[--ib-use-sys-malloc]\n"+
			"[--ib-lru-old-ratio as %% e.g. 38]\n"+
			"[--ib-lru-access-threshold in ms]\n"+
			"[--ib-force-recovery 1-6]\n"+
			"[--ib-log-dir path]\n"+
			"[--ib-data-dir path]\n"+
			"[--ib-data-file-path string]\n"+
			"[--ib-disble-dblwr]\n"+
			"[--ib-disble-checksum]\n"+
			"[--ib-disble-file-per-table]\n"+
			"[--ib-flush-log-at-trx-commit 1-3]\n"+
			"[--ib-flush-method method]\n"+
			"[--ib-read-threads count]\n"+
			"[--ib-write-threads count]\n"+
			"[--ib-max-open-files count]\n"+
			"[--ib-lock-wait-timeout seconds]\n",
		progName)
}

func printVersion(w io.Writer) {
	version := api.APIVersion()
	fmt.Fprintf(w, "API: %d.%d.%d\n",
		version>>32,
		(version>>16)&0xffff,
		version&0xffff)
}

func configAddElem(cfg *config, key, val string) {
	cfg.elems = append(cfg.elems, configVar{
		name:     key,
		value:    val,
		hasValue: val != "",
	})
}

func configParseFile(filename string, cfg *config) int {
	data, err := os.ReadFile(filename)
	if err != nil {
		return -1
	}
	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		line = strings.TrimSuffix(line, "\r")
		line = strings.TrimLeftFunc(line, func(r rune) bool {
			return r == ' ' || r == '\t' || r == '\n' || r == '\r' || r == '\v' || r == '\f'
		})
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "#") {
			continue
		}
		key := line
		val := ""
		if idx := strings.IndexByte(line, '='); idx >= 0 {
			key = line[:idx]
			val = line[idx+1:]
		}
		if key == "" {
			continue
		}
		configAddElem(cfg, key, val)
	}
	return 0
}

func configPrint(cfg *config, w io.Writer) {
	for _, elem := range cfg.elems {
		if elem.hasValue {
			fmt.Fprintf(w, "%s=%s\n", elem.name, elem.value)
		} else {
			fmt.Fprintf(w, "%s\n", elem.name)
		}
	}
}

func configFree(cfg *config) {
	cfg.elems = nil
}

func dropTable(dbname, name string) api.ErrCode {
	tableName := fmt.Sprintf("%s/%s", dbname, name)
	trx := api.TrxBegin(api.IB_TRX_REPEATABLE_READ)
	if trx == nil {
		return api.DB_ERROR
	}
	if err := api.SchemaLockExclusive(trx); err != api.DB_SUCCESS {
		_ = api.TrxRollback(trx)
		return err
	}
	if err := api.TableDrop(trx, tableName); err != api.DB_SUCCESS {
		_ = api.TrxRollback(trx)
		return err
	}
	return api.TrxCommit(trx)
}

func TestTest0auxConfigParse(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "innodb.conf")
	content := "# comment\n  alpha=1\nbeta=two\ngamma\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	var cfg config
	if got := configParseFile(path, &cfg); got != 0 {
		t.Fatalf("configParseFile=%d, want 0", got)
	}
	if len(cfg.elems) != 3 {
		t.Fatalf("config elems=%d, want 3", len(cfg.elems))
	}
	if cfg.elems[0].name != "alpha" || cfg.elems[0].value != "1" || !cfg.elems[0].hasValue {
		t.Fatalf("alpha parsed=%+v", cfg.elems[0])
	}
	if cfg.elems[1].name != "beta" || cfg.elems[1].value != "two" || !cfg.elems[1].hasValue {
		t.Fatalf("beta parsed=%+v", cfg.elems[1])
	}
	if cfg.elems[2].name != "gamma" || cfg.elems[2].hasValue {
		t.Fatalf("gamma parsed=%+v", cfg.elems[2])
	}
	var buf bytes.Buffer
	configPrint(&cfg, &buf)
	want := "alpha=1\nbeta=two\ngamma\n"
	if got := buf.String(); got != want {
		t.Fatalf("configPrint=%q, want %q", got, want)
	}
	configFree(&cfg)
	if len(cfg.elems) != 0 {
		t.Fatalf("configFree elems=%d, want 0", len(cfg.elems))
	}
}

func TestTest0auxGenRandText(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	text := genRandText(rng, 10)
	if len(text) == 0 || len(text) >= 10 {
		t.Fatalf("genRandText len=%d, want 1..9", len(text))
	}
	for _, b := range text {
		if !(b >= 'A' && b <= 'Z') &&
			!(b >= 'a' && b <= 'z') &&
			!(b >= '0' && b <= '9') {
			t.Fatalf("genRandText invalid byte=%q", b)
		}
	}
}

func TestTest0auxTupleHelpers(t *testing.T) {
	resetAPI(t)
	if err := api.Init(); err != api.DB_SUCCESS {
		t.Fatalf("Init: %v", err)
	}
	defer func() {
		_ = api.Shutdown(api.ShutdownNormal)
	}()
	if err := testConfigure(); err != api.DB_SUCCESS {
		t.Fatalf("testConfigure: %v", err)
	}
	if err := api.Startup("barracuda"); err != api.DB_SUCCESS {
		t.Fatalf("Startup: %v", err)
	}
	const auxDB = "aux_db"
	const auxTable = "t0"
	tableName := fmt.Sprintf("%s/%s", auxDB, auxTable)
	if err := api.DatabaseCreate(auxDB); err != api.DB_SUCCESS {
		t.Fatalf("DatabaseCreate: %v", err)
	}
	if err := createAuxTable(tableName); err != api.DB_SUCCESS {
		t.Fatalf("create table: %v", err)
	}
	trx := api.TrxBegin(api.IB_TRX_REPEATABLE_READ)
	if trx == nil {
		t.Fatalf("TrxBegin returned nil")
	}
	var crsr *api.Cursor
	if err := api.CursorOpenTable(tableName, trx, &crsr); err != api.DB_SUCCESS {
		t.Fatalf("CursorOpenTable: %v", err)
	}
	tpl := api.ClustReadTupleCreate(crsr)
	if tpl == nil {
		t.Fatalf("ClustReadTupleCreate returned nil")
	}
	if err := api.TupleWriteI32(tpl, 0, -42); err != api.DB_SUCCESS {
		t.Fatalf("TupleWriteI32: %v", err)
	}
	if err := api.TupleWriteU16(tpl, 1, 42); err != api.DB_SUCCESS {
		t.Fatalf("TupleWriteU16: %v", err)
	}
	if err := api.TupleWriteFloat(tpl, 2, 1.5); err != api.DB_SUCCESS {
		t.Fatalf("TupleWriteFloat: %v", err)
	}
	if err := api.TupleWriteDouble(tpl, 3, 2.25); err != api.DB_SUCCESS {
		t.Fatalf("TupleWriteDouble: %v", err)
	}
	if err := api.ColSetValue(tpl, 4, []byte("hi"), 2); err != api.DB_SUCCESS {
		t.Fatalf("ColSetValue: %v", err)
	}
	if err := api.CursorInsertRow(crsr, tpl); err != api.DB_SUCCESS {
		t.Fatalf("CursorInsertRow: %v", err)
	}
	if err := api.CursorFirst(crsr); err != api.DB_SUCCESS {
		t.Fatalf("CursorFirst: %v", err)
	}
	readTpl := api.ClustReadTupleCreate(crsr)
	if readTpl == nil {
		t.Fatalf("ClustReadTupleCreate returned nil")
	}
	if err := api.CursorReadRow(crsr, readTpl); err != api.DB_SUCCESS {
		t.Fatalf("CursorReadRow: %v", err)
	}
	var meta api.ColMeta
	api.ColGetMeta(readTpl, 0, &meta)
	intVal, err := readIntFromTuple(readTpl, &meta, 0)
	if err != api.DB_SUCCESS {
		t.Fatalf("readIntFromTuple c1: %v", err)
	}
	if got := int32(intVal); got != -42 {
		t.Fatalf("readIntFromTuple c1=%d, want -42", got)
	}
	api.ColGetMeta(readTpl, 1, &meta)
	intVal, err = readIntFromTuple(readTpl, &meta, 1)
	if err != api.DB_SUCCESS {
		t.Fatalf("readIntFromTuple c2: %v", err)
	}
	if intVal != 42 {
		t.Fatalf("readIntFromTuple c2=%d, want 42", intVal)
	}
	var buf bytes.Buffer
	if err := printTuple(&buf, readTpl); err != api.DB_SUCCESS {
		t.Fatalf("printTuple: %v", err)
	}
	want := "-42|42|1.500000|2.250000|2:hi|\n"
	if got := buf.String(); got != want {
		t.Fatalf("printTuple=%q, want %q", got, want)
	}
	api.TupleDelete(readTpl)
	api.TupleDelete(tpl)
	if err := api.CursorClose(crsr); err != api.DB_SUCCESS {
		t.Fatalf("CursorClose: %v", err)
	}
	if err := api.TrxCommit(trx); err != api.DB_SUCCESS {
		t.Fatalf("TrxCommit: %v", err)
	}
	if err := api.TableDrop(nil, tableName); err != api.DB_SUCCESS {
		t.Fatalf("TableDrop: %v", err)
	}
	if err := api.DatabaseDrop(auxDB); err != api.DB_SUCCESS {
		t.Fatalf("DatabaseDrop: %v", err)
	}
}

func createAuxTable(name string) api.ErrCode {
	var schema *api.TableSchema
	if err := api.TableSchemaCreate(name, &schema, api.IB_TBL_COMPACT, 0); err != api.DB_SUCCESS {
		return err
	}
	if err := api.TableSchemaAddCol(schema, "c1", api.IB_INT, api.IB_COL_NONE, 0, 4); err != api.DB_SUCCESS {
		return err
	}
	if err := api.TableSchemaAddCol(schema, "c2", api.IB_INT, api.IB_COL_UNSIGNED, 0, 2); err != api.DB_SUCCESS {
		return err
	}
	if err := api.TableSchemaAddCol(schema, "c3", api.IB_FLOAT, api.IB_COL_NONE, 0, 4); err != api.DB_SUCCESS {
		return err
	}
	if err := api.TableSchemaAddCol(schema, "c4", api.IB_DOUBLE, api.IB_COL_NONE, 0, 8); err != api.DB_SUCCESS {
		return err
	}
	if err := api.TableSchemaAddCol(schema, "c5", api.IB_VARCHAR, api.IB_COL_NONE, 0, 10); err != api.DB_SUCCESS {
		return err
	}
	var idx *api.IndexSchema
	if err := api.TableSchemaAddIndex(schema, "c1", &idx); err != api.DB_SUCCESS {
		return err
	}
	if err := api.IndexSchemaAddCol(idx, "c1", 0); err != api.DB_SUCCESS {
		return err
	}
	if err := api.IndexSchemaSetClustered(idx); err != api.DB_SUCCESS {
		return err
	}
	err := api.TableCreate(nil, schema, nil)
	api.TableSchemaDelete(schema)
	return err
}

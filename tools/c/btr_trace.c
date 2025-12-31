#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "test0aux.h"

#define DATABASE "btrtrace"
#define TABLE "btr_trace"

typedef struct row_t {
	char key[8];
	char val[8];
} row_t;

#define COL_LEN(n) (sizeof(((row_t*)0)->n))

static ib_err_t create_database(const char* name)
{
	ib_err_t drop_err = ib_database_drop(name);
	(void) drop_err;
	if (ib_database_create(name) != IB_TRUE) {
		return(DB_ERROR);
	}
	return(DB_SUCCESS);
}

static ib_err_t create_table(const char* dbname, const char* name)
{
	ib_trx_t	ib_trx;
	ib_id_t		 table_id = 0;
	ib_err_t	err = DB_SUCCESS;
	ib_tbl_sch_t	ib_tbl_sch = NULL;
	ib_idx_sch_t	ib_idx_sch = NULL;
	char		 table_name[IB_MAX_TABLE_NAME_LEN];

#ifdef __WIN__
	sprintf(table_name, "%s/%s", dbname, name);
#else
	snprintf(table_name, sizeof(table_name), "%s/%s", dbname, name);
#endif

	err = ib_table_schema_create(table_name, &ib_tbl_sch, IB_TBL_COMPACT, 0);
	if (err != DB_SUCCESS) {
		return(err);
	}

	err = ib_table_schema_add_col(
		ib_tbl_sch, "key",
		IB_VARCHAR, IB_COL_NONE, 0, COL_LEN(key) - 1);
	if (err != DB_SUCCESS) {
		return(err);
	}

	err = ib_table_schema_add_col(
		ib_tbl_sch, "val",
		IB_VARCHAR, IB_COL_NONE, 0, COL_LEN(val) - 1);
	if (err != DB_SUCCESS) {
		return(err);
	}

	err = ib_table_schema_add_index(ib_tbl_sch, "PRIMARY", &ib_idx_sch);
	if (err != DB_SUCCESS) {
		return(err);
	}

	err = ib_index_schema_add_col(ib_idx_sch, "key", 0);
	if (err != DB_SUCCESS) {
		return(err);
	}

	err = ib_index_schema_set_clustered(ib_idx_sch);
	if (err != DB_SUCCESS) {
		return(err);
	}

	ib_trx = ib_trx_begin(IB_TRX_REPEATABLE_READ);
	if (ib_trx == NULL) {
		return(DB_ERROR);
	}

	err = ib_schema_lock_exclusive(ib_trx);
	if (err != DB_SUCCESS) {
		return(err);
	}

	err = ib_table_create(ib_trx, ib_tbl_sch, &table_id);
	if (err != DB_SUCCESS) {
		return(err);
	}

	err = ib_trx_commit(ib_trx);
	if (err != DB_SUCCESS) {
		return(err);
	}

	if (ib_tbl_sch != NULL) {
		ib_table_schema_delete(ib_tbl_sch);
	}

	return(err);
}

static ib_err_t open_table(
	const char* dbname,
	const char* name,
	ib_trx_t ib_trx,
	ib_crsr_t* crsr)
{
	ib_err_t	 err = DB_SUCCESS;
	char		 table_name[IB_MAX_TABLE_NAME_LEN];

#ifdef __WIN__
	sprintf(table_name, "%s/%s", dbname, name);
#else
	snprintf(table_name, sizeof(table_name), "%s/%s", dbname, name);
#endif

	err = ib_cursor_open_table(table_name, ib_trx, crsr);
	return(err);
}

static void read_varchar(const ib_tpl_t tpl, int col, char* buf, size_t buf_size)
{
	const char* ptr = ib_col_get_value(tpl, col);
	ib_ulint_t len = ib_col_get_len(tpl, col);
	size_t copy_len = 0;

	if (buf_size == 0) {
		return;
	}

	if (ptr == NULL || len == 0) {
		buf[0] = '\0';
		return;
	}

	copy_len = len;
	if (copy_len >= buf_size) {
		copy_len = buf_size - 1;
	}

	memcpy(buf, ptr, copy_len);
	buf[copy_len] = '\0';
}

static ib_err_t insert_row(ib_crsr_t crsr, const char* key, const char* val)
{
	ib_err_t err;
	ib_tpl_t tpl = ib_clust_read_tuple_create(crsr);

	if (tpl == NULL) {
		return(DB_ERROR);
	}

	err = ib_col_set_value(tpl, 0, key, strlen(key));
	if (err != DB_SUCCESS) {
		ib_tuple_delete(tpl);
		return(err);
	}

	err = ib_col_set_value(tpl, 1, val, strlen(val));
	if (err != DB_SUCCESS) {
		ib_tuple_delete(tpl);
		return(err);
	}

	err = ib_cursor_insert_row(crsr, tpl);
	ib_tuple_delete(tpl);

	return(err);
}

static ib_err_t search_value(
	ib_crsr_t crsr,
	const char* key,
	char* out,
	size_t out_len,
	ib_bool_t* found)
{
	ib_err_t err;
	ib_tpl_t key_tpl = NULL;
	ib_tpl_t row_tpl = NULL;
	int res = ~0;

	*found = IB_FALSE;

	key_tpl = ib_clust_search_tuple_create(crsr);
	if (key_tpl == NULL) {
		return(DB_ERROR);
	}

	err = ib_col_set_value(key_tpl, 0, key, strlen(key));
	if (err != DB_SUCCESS) {
		ib_tuple_delete(key_tpl);
		return(err);
	}

	err = ib_cursor_moveto(crsr, key_tpl, IB_CUR_GE, &res);
	if (err == DB_SUCCESS && res == 0) {
		row_tpl = ib_clust_read_tuple_create(crsr);
		if (row_tpl == NULL) {
			ib_tuple_delete(key_tpl);
			return(DB_ERROR);
		}

		err = ib_cursor_read_row(crsr, row_tpl);
		if (err == DB_SUCCESS) {
			read_varchar(row_tpl, 1, out, out_len);
			*found = IB_TRUE;
		}
		ib_tuple_delete(row_tpl);
	}

	ib_tuple_delete(key_tpl);

	if (err == DB_RECORD_NOT_FOUND || err == DB_END_OF_INDEX) {
		return(DB_SUCCESS);
	}

	return(err);
}

static ib_err_t delete_key(ib_crsr_t crsr, const char* key, ib_bool_t* deleted)
{
	ib_err_t err;
	ib_tpl_t key_tpl = NULL;
	int res = ~0;

	*deleted = IB_FALSE;

	key_tpl = ib_clust_search_tuple_create(crsr);
	if (key_tpl == NULL) {
		return(DB_ERROR);
	}

	err = ib_col_set_value(key_tpl, 0, key, strlen(key));
	if (err != DB_SUCCESS) {
		ib_tuple_delete(key_tpl);
		return(err);
	}

	err = ib_cursor_moveto(crsr, key_tpl, IB_CUR_GE, &res);
	if (err == DB_SUCCESS && res == 0) {
		err = ib_cursor_delete_row(crsr);
		if (err == DB_SUCCESS) {
			*deleted = IB_TRUE;
		}
	}

	ib_tuple_delete(key_tpl);

	if (err == DB_RECORD_NOT_FOUND || err == DB_END_OF_INDEX) {
		return(DB_SUCCESS);
	}

	return(err);
}

static ib_err_t update_key(
	ib_crsr_t crsr,
	const char* key,
	const char* val,
	ib_bool_t* updated)
{
	ib_err_t err;
	ib_tpl_t key_tpl = NULL;
	ib_tpl_t old_tpl = NULL;
	ib_tpl_t new_tpl = NULL;
	int res = ~0;

	*updated = IB_FALSE;

	key_tpl = ib_clust_search_tuple_create(crsr);
	if (key_tpl == NULL) {
		return(DB_ERROR);
	}

	err = ib_col_set_value(key_tpl, 0, key, strlen(key));
	if (err != DB_SUCCESS) {
		ib_tuple_delete(key_tpl);
		return(err);
	}

	err = ib_cursor_moveto(crsr, key_tpl, IB_CUR_GE, &res);
	if (err == DB_SUCCESS && res == 0) {
		old_tpl = ib_clust_read_tuple_create(crsr);
		new_tpl = ib_clust_read_tuple_create(crsr);
		if (old_tpl == NULL || new_tpl == NULL) {
			ib_tuple_delete(key_tpl);
			if (old_tpl != NULL) {
				ib_tuple_delete(old_tpl);
			}
			if (new_tpl != NULL) {
				ib_tuple_delete(new_tpl);
			}
			return(DB_ERROR);
		}

		err = ib_cursor_read_row(crsr, old_tpl);
		if (err == DB_SUCCESS) {
			err = ib_tuple_copy(new_tpl, old_tpl);
		}
		if (err == DB_SUCCESS) {
			err = ib_col_set_value(new_tpl, 1, val, strlen(val));
		}
		if (err == DB_SUCCESS) {
			err = ib_cursor_update_row(crsr, old_tpl, new_tpl);
			if (err == DB_SUCCESS) {
				*updated = IB_TRUE;
			}
		}
	}

	if (key_tpl != NULL) {
		ib_tuple_delete(key_tpl);
	}
	if (old_tpl != NULL) {
		ib_tuple_delete(old_tpl);
	}
	if (new_tpl != NULL) {
		ib_tuple_delete(new_tpl);
	}

	if (err == DB_RECORD_NOT_FOUND || err == DB_END_OF_INDEX) {
		return(DB_SUCCESS);
	}

	return(err);
}

static ib_err_t print_final_keys(ib_crsr_t crsr)
{
	ib_err_t err;
	ib_tpl_t tpl = NULL;
	ib_bool_t first = IB_TRUE;

	tpl = ib_clust_read_tuple_create(crsr);
	if (tpl == NULL) {
		return(DB_ERROR);
	}

	printf("final keys: ");

	err = ib_cursor_first(crsr);
	while (err == DB_SUCCESS) {
		char key[32];

		err = ib_cursor_read_row(crsr, tpl);
		if (err != DB_SUCCESS) {
			break;
		}

		read_varchar(tpl, 0, key, sizeof(key));
		if (!first) {
			printf(" ");
		}
		printf("%s", key);
		first = IB_FALSE;

		err = ib_cursor_next(crsr);
	}

	printf("\n");

	ib_tuple_delete(tpl);

	if (err == DB_END_OF_INDEX || err == DB_RECORD_NOT_FOUND) {
		return(DB_SUCCESS);
	}

	return(err);
}

int main(int argc, char* argv[])
{
	ib_err_t err;
	ib_crsr_t crsr = NULL;
	ib_trx_t ib_trx;
	char value[32];
	ib_bool_t ok = IB_FALSE;

	(void) argc;
	(void) argv;

	err = ib_init();
	assert(err == DB_SUCCESS);

	test_configure();

	err = ib_startup("barracuda");
	assert(err == DB_SUCCESS);

	err = create_database(DATABASE);
	assert(err == DB_SUCCESS);

	err = create_table(DATABASE, TABLE);
	assert(err == DB_SUCCESS);

	ib_trx = ib_trx_begin(IB_TRX_REPEATABLE_READ);
	assert(ib_trx != NULL);

	err = open_table(DATABASE, TABLE, ib_trx, &crsr);
	assert(err == DB_SUCCESS);

	err = ib_cursor_lock(crsr, IB_LOCK_IX);
	assert(err == DB_SUCCESS);

	err = insert_row(crsr, "a", "va");
	assert(err == DB_SUCCESS);
	printf("insert a=va\n");

	err = insert_row(crsr, "c", "vc");
	assert(err == DB_SUCCESS);
	printf("insert c=vc\n");

	err = insert_row(crsr, "b", "vb");
	assert(err == DB_SUCCESS);
	printf("insert b=vb\n");

	err = search_value(crsr, "b", value, sizeof(value), &ok);
	assert(err == DB_SUCCESS);
	if (ok == IB_TRUE) {
		printf("search b => %s\n", value);
	} else {
		printf("search b => <miss>\n");
	}

	err = ib_cursor_set_lock_mode(crsr, IB_LOCK_X);
	assert(err == DB_SUCCESS);

	err = delete_key(crsr, "c", &ok);
	assert(err == DB_SUCCESS);
	printf("delete c => %s\n", ok == IB_TRUE ? "true" : "false");

	err = insert_row(crsr, "d", "vd");
	assert(err == DB_SUCCESS);
	printf("insert d=vd\n");

	err = update_key(crsr, "b", "vb2", &ok);
	assert(err == DB_SUCCESS);
	printf("update b=vb2\n");

	err = search_value(crsr, "c", value, sizeof(value), &ok);
	assert(err == DB_SUCCESS);
	if (ok == IB_TRUE) {
		printf("search c => %s\n", value);
	} else {
		printf("search c => <miss>\n");
	}

	err = print_final_keys(crsr);
	assert(err == DB_SUCCESS);

	err = ib_cursor_close(crsr);
	assert(err == DB_SUCCESS);
	crsr = NULL;

	err = ib_trx_commit(ib_trx);
	assert(err == DB_SUCCESS);

	err = drop_table(DATABASE, TABLE);
	assert(err == DB_SUCCESS);

	err = ib_database_drop(DATABASE);
	if (err != DB_SUCCESS) {
		/* Ignore drop failures; database may not exist. */
	}

	err = ib_shutdown(IB_SHUTDOWN_NORMAL);
	assert(err == DB_SUCCESS);

	return(EXIT_SUCCESS);
}

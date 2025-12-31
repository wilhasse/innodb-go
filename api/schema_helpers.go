package api

// TblSchAddVarcharCol adds a VARCHAR column to a table schema.
func TblSchAddVarcharCol(schema *TableSchema, name string, length uint32) ErrCode {
	return TableSchemaAddCol(schema, name, IB_VARCHAR, IB_COL_NONE, 0, length)
}

// TblSchAddBlobCol adds a BLOB column to a table schema.
func TblSchAddBlobCol(schema *TableSchema, name string) ErrCode {
	return TableSchemaAddCol(schema, name, IB_BLOB, IB_COL_NONE, 0, 0)
}

// TblSchAddU32Col adds a UINT32 column to a table schema.
func TblSchAddU32Col(schema *TableSchema, name string) ErrCode {
	return TableSchemaAddCol(schema, name, IB_INT, IB_COL_UNSIGNED, 0, 4)
}

// TblSchAddTextCol adds a TEXT column to a table schema.
func TblSchAddTextCol(schema *TableSchema, name string) ErrCode {
	return TableSchemaAddCol(schema, name, IB_VARCHAR, IB_COL_NONE, 0, 0)
}

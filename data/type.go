package data

// DataClientDefaultCharsetColl mirrors data_client_default_charset_coll.
var DataClientDefaultCharsetColl uint32

// Precise type flags.
const (
	DataEnglish        = 4
	DataErrorType      = 111
	DataClientTypeMask = 255
	DataNotNull        = 256
	DataUnsigned       = 512
	DataBinaryType     = 1024
	DataCustomType     = 2048
	DataSysPrtypeMask  = 0xF
	DataRowID          = 0
	DataTrxID          = 1
	DataRollPtr        = 2
	DataNSysCols       = 3
)

// DataTypeVarInit resets type globals.
func DataTypeVarInit() {
	DataClientDefaultCharsetColl = 0
}

// DataTypeGetAtMostNMbchars returns bytes occupied by at most n characters.
func DataTypeGetAtMostNMbchars(prtype, mbminlen, mbmaxlen, prefixLen, dataLen uint32, str []byte) uint32 {
	if dataLen == UnivSQLNull {
		return 0
	}
	if mbmaxlen == 0 {
		if prefixLen < dataLen {
			return prefixLen
		}
		return dataLen
	}
	if mbminlen != mbmaxlen {
		if prefixLen < dataLen {
			return prefixLen
		}
		return dataLen
	}
	if prefixLen < dataLen {
		return prefixLen
	}
	return dataLen
}

// DataTypeIsStringType reports whether the main type is a string type.
func DataTypeIsStringType(mtype uint32) bool {
	return mtype <= DataBlob || mtype == DataClient || mtype == DataVarClient
}

// DataTypeIsBinaryStringType reports whether the type is binary string.
func DataTypeIsBinaryStringType(mtype, prtype uint32) bool {
	return mtype == DataFixBinary || mtype == DataBinary || (mtype == DataBlob && (prtype&DataBinaryType) != 0)
}

// DataTypeIsNonBinaryStringType reports whether the type is non-binary string.
func DataTypeIsNonBinaryStringType(mtype, prtype uint32) bool {
	return DataTypeIsStringType(mtype) && !DataTypeIsBinaryStringType(mtype, prtype)
}

// DataTypeFormPrtype combines old prtype with charset-collation.
func DataTypeFormPrtype(oldPrtype, charsetColl uint32) uint32 {
	return oldPrtype + (charsetColl << 16)
}

// DataTypeValidate validates a data type descriptor.
func DataTypeValidate(typ *DataType) bool {
	if typ == nil {
		return false
	}
	if typ.MType < DataVarchar || typ.MType > DataClient {
		return false
	}
	if typ.MType == DataSys {
		if (typ.PrType & DataClientTypeMask) >= DataNSysCols {
			return false
		}
	}
	if typ.MbMinLen > typ.MbMaxLen {
		return false
	}
	return true
}

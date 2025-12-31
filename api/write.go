package api

import (
	"github.com/wilhasse/innodb-go/data"
	"github.com/wilhasse/innodb-go/rec"
)

func encodeDecodeTuple(tpl *data.Tuple) (*data.Tuple, ErrCode) {
	if tpl == nil {
		return nil, DB_ERROR
	}
	recBytes, err := rec.EncodeVar(tpl, nil, 0)
	if err != nil {
		return nil, DB_ERROR
	}
	decoded, err := rec.DecodeVar(recBytes, len(tpl.Fields), 0)
	if err != nil {
		return nil, DB_ERROR
	}
	return decoded, DB_SUCCESS
}

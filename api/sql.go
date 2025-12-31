package api

import (
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/wilhasse/innodb-go/pars"
)

// SQLArg represents a parameter for ExecSQL/ExecDDLSQL.
type SQLArg struct {
	Type        ColType
	Name        string
	String      string
	IntLen      int
	Signed      bool
	IntValue    int64
	UintValue   uint64
	UserFunc    pars.UserFunc
	UserFuncArg any
}

// SQLArgString binds a string literal (":name").
func SQLArgString(name, value string) SQLArg {
	return SQLArg{Type: IB_VARCHAR, Name: ":" + name, String: value}
}

// SQLArgChar binds a fixed-length string literal (":name").
func SQLArgChar(name, value string) SQLArg {
	return SQLArg{Type: IB_CHAR, Name: ":" + name, String: value}
}

// SQLArgID binds an identifier ("$name").
func SQLArgID(name, value string) SQLArg {
	return SQLArg{Type: IB_VARCHAR, Name: "$" + name, String: value}
}

// SQLArgIntSigned binds a signed integer literal.
func SQLArgIntSigned(name string, length int, value int64) SQLArg {
	return SQLArg{Type: IB_INT, Name: name, IntLen: length, Signed: true, IntValue: value}
}

// SQLArgIntUnsigned binds an unsigned integer literal.
func SQLArgIntUnsigned(name string, length int, value uint64) SQLArg {
	return SQLArg{Type: IB_INT, Name: name, IntLen: length, Signed: false, UintValue: value}
}

// SQLArgFunc binds a user function.
func SQLArgFunc(name string, fn pars.UserFunc, arg any) SQLArg {
	return SQLArg{Type: IB_SYS, Name: name, UserFunc: fn, UserFuncArg: arg}
}

// ExecSQL parses arguments and returns DB_UNSUPPORTED until SQL execution is ported.
func ExecSQL(sql string, args ...SQLArg) ErrCode {
	if !started {
		return DB_ERROR
	}
	_, err := execVSQL(sql, args)
	if err != DB_SUCCESS {
		return err
	}
	return DB_UNSUPPORTED
}

// ExecDDLSQL parses arguments and returns DB_UNSUPPORTED until SQL execution is ported.
func ExecDDLSQL(sql string, args ...SQLArg) ErrCode {
	if !started {
		return DB_ERROR
	}
	_, err := execVSQL(sql, args)
	if err != DB_SUCCESS {
		return err
	}
	return DB_UNSUPPORTED
}

func execVSQL(sql string, args []SQLArg) (*pars.Info, ErrCode) {
	if strings.TrimSpace(sql) == "" {
		return nil, DB_INVALID_INPUT
	}
	info := pars.NewInfo()
	for _, arg := range args {
		switch arg.Type {
		case IB_CHAR, IB_VARCHAR:
			prefix, name, err := parseNamePrefix(arg.Name)
			if err != nil {
				return nil, DB_INVALID_INPUT
			}
			if prefix == '$' {
				info.AddID(name, arg.String)
			} else {
				info.AddStrLiteral(name, arg.String)
			}
		case IB_INT:
			name := trimNamePrefix(arg.Name)
			buf, unsigned, err := encodeInt(arg)
			if err != DB_SUCCESS {
				return nil, err
			}
			info.AddLiteral(name, buf, pars.LiteralInt, unsigned)
		case IB_SYS:
			name := trimNamePrefix(arg.Name)
			if arg.UserFunc == nil {
				return nil, DB_INVALID_INPUT
			}
			info.AddFunction(name, arg.UserFunc, arg.UserFuncArg)
		default:
			return nil, DB_UNSUPPORTED
		}
	}
	return info, DB_SUCCESS
}

func parseNamePrefix(name string) (rune, string, error) {
	if name == "" {
		return 0, "", errors.New("empty name")
	}
	r := rune(name[0])
	if r != ':' && r != '$' {
		return 0, "", fmt.Errorf("missing prefix: %s", name)
	}
	if len(name) == 1 {
		return 0, "", fmt.Errorf("empty name after prefix: %s", name)
	}
	return r, name[1:], nil
}

func trimNamePrefix(name string) string {
	if name == "" {
		return name
	}
	switch name[0] {
	case ':', '$':
		if len(name) > 1 {
			return name[1:]
		}
	}
	return name
}

func encodeInt(arg SQLArg) ([]byte, bool, ErrCode) {
	if arg.IntLen != 1 && arg.IntLen != 2 && arg.IntLen != 4 && arg.IntLen != 8 {
		return nil, false, DB_INVALID_INPUT
	}
	bits := uint(arg.IntLen * 8)
	if arg.Signed {
		min, max := signedRange(bits)
		if arg.IntValue < min || arg.IntValue > max {
			return nil, false, DB_INVALID_INPUT
		}
		return encodeUint(uint64(arg.IntValue), arg.IntLen), false, DB_SUCCESS
	}
	if arg.UintValue > unsignedMax(bits) {
		return nil, true, DB_INVALID_INPUT
	}
	return encodeUint(arg.UintValue, arg.IntLen), true, DB_SUCCESS
}

func signedRange(bits uint) (int64, int64) {
	if bits == 64 {
		return math.MinInt64, math.MaxInt64
	}
	max := int64(1<<(bits-1)) - 1
	min := -int64(1 << (bits - 1))
	return min, max
}

func unsignedMax(bits uint) uint64 {
	if bits == 64 {
		return math.MaxUint64
	}
	return (1 << bits) - 1
}

func encodeUint(value uint64, length int) []byte {
	buf := make([]byte, length)
	for i := length - 1; i >= 0; i-- {
		buf[i] = byte(value)
		value >>= 8
	}
	return buf
}

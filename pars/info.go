package pars

// UserFunc mirrors pars_user_func_cb_t.
type UserFunc func(arg any, userArg any) any

// LiteralType describes a bound literal kind.
type LiteralType string

const (
	LiteralInt    LiteralType = "int"
	LiteralString LiteralType = "string"
)

// BoundLiteral stores a bound literal value.
type BoundLiteral struct {
	Name     string
	Value    []byte
	Type     LiteralType
	Unsigned bool
}

// BoundID stores a bound identifier.
type BoundID struct {
	Name string
	ID   string
}

// UserFuncBinding stores a bound user function.
type UserFuncBinding struct {
	Name string
	Func UserFunc
	Arg  any
}

// Info mirrors pars_info_t with minimal Go storage.
type Info struct {
	Literals map[string]BoundLiteral
	IDs      map[string]BoundID
	Funcs    map[string]UserFuncBinding
}

// NewInfo creates an empty Info struct.
func NewInfo() *Info {
	return &Info{
		Literals: map[string]BoundLiteral{},
		IDs:      map[string]BoundID{},
		Funcs:    map[string]UserFuncBinding{},
	}
}

// AddLiteral stores a literal value.
func (info *Info) AddLiteral(name string, value []byte, typ LiteralType, unsigned bool) {
	info.Literals[name] = BoundLiteral{
		Name:     name,
		Value:    value,
		Type:     typ,
		Unsigned: unsigned,
	}
}

// AddStrLiteral stores a string literal.
func (info *Info) AddStrLiteral(name, value string) {
	info.AddLiteral(name, []byte(value), LiteralString, false)
}

// AddID stores an identifier binding.
func (info *Info) AddID(name, id string) {
	info.IDs[name] = BoundID{
		Name: name,
		ID:   id,
	}
}

// AddFunction stores a user function binding.
func (info *Info) AddFunction(name string, fn UserFunc, arg any) {
	info.Funcs[name] = UserFuncBinding{
		Name: name,
		Func: fn,
		Arg:  arg,
	}
}

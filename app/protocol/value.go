package protocol

import "strconv"

func (v *Value) Array() []*Value {
	return v.array
}

func (v *Value) Bulk() string {
	return v.bulk
}

func (v *Value) BulkToInteger() (int, error) {
	num, err := strconv.ParseInt(v.bulk, 10, 64)
	if err != nil {
		return 0, err
	}
	return int(num), nil
}

func (v *Value) Str() string {
	return v.str
}

func (v *Value) Integer() int {
	return v.integer
}

func (v *Value) SetArray(array []*Value) *Value {
	v.typ = TARRAY
	v.array = array
	return v
}

func (v *Value) SetBulk(bulk string) *Value {
	v.typ = TBULK
	v.bulk = bulk
	return v
}

func (v *Value) SetStr(str string) *Value {
	v.typ = TSTRING
	v.str = str
	return v
}

func (v *Value) SetInteger(integer int) *Value {
	v.typ = TINTEGER
	v.integer = integer
	return v
}

func (v *Value) SetError(err string) *Value {
	v.typ = TERROR
	v.str = err
	return v
}

// SetNull: set null bulk string
func (v *Value) SetNull() *Value {
	v.typ = TNULL
	return v
}

func (v *Value) SetEmptyArray() *Value {
	v.typ = TARRAY
	v.array = nil
	return v
}

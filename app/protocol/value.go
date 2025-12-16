package protocol

func (v *Value) Array() []*Value {
	return v.array
}

func (v *Value) Bulk() string {
	return v.bulk
}

func (v *Value) Str() string {
	return v.str
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

func (v *Value) SetError(err string) *Value {
	v.typ = TERROR
	v.str = err
	return v
}

func (v *Value) SetNull() *Value {
	v.typ = TNULL
	return v
}

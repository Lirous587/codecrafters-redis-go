package protocol

import (
	"strconv"

	"github.com/pkg/errors"
)

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

func (v *Value) BulkToDouble() (float64, error) {
	num, err := strconv.ParseFloat(v.bulk, 64)
	if err != nil {
		return 0, err
	}
	return float64(num), nil
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

func (v *Value) Append(val ...*Value) *Value {
	if v.typ != TARRAY {
		v.typ = TARRAY
		if v.array == nil {
			v.array = make([]*Value, 0)
		}
	}
	v.array = append(v.array, val...)
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

func (v *Value) SetDouble(double float64) *Value {
	v.typ = TDOUBLE
	v.double = double
	return v
}

func (v *Value) SetError(err string) *Value {
	v.typ = TERROR
	v.str = err
	return v
}

func (v *Value) Error() error {
	if v == nil {
		return nil
	}
	if v.typ != TERROR {
		return nil
	}
	return errors.New(v.str)
}

func (v *Value) SetNullBulk() *Value {
	v.typ = TNULL
	return v
}

// SetNullArray 设置为 Null Array (空/无效数组)
//
// 协议格式: *-1\r\n
// 底层状态: v.array = nil
//
// 使用场景:
// 1. 阻塞命令超时: 如 BLPOP/BRPOP 设置超时时间后，未获取到数据。
// 2. 某些错误或特殊状态: 表示"完全不存在"或"操作无效"，区别于空集合。
//
// 注意: 在 RESP2 中，这与 SetEmptyArray (*0) 是截然不同的。
func (v *Value) SetNullArray() *Value {
	v.typ = TARRAY
	v.array = nil // 关键：nil 切片会在 Marshal 时被编码为 *-1
	return v
}

// SetEmptyArray 设置为 Empty Array (空数组/空集合)
//
// 协议格式: *0\r\n
// 底层状态: v.array = make([]*Value, 0) (非 nil，但长度为 0)
//
// 使用场景:
// 1. 容器不存在: 查询一个不存在的 Key (LRANGE, XRANGE)，Redis 视为空容器处理。
// 2. 范围查询为空: 查询范围越界，导致结果集为空。
// 3. 正常的空结果: 如 KEYS * 没匹配到任何键。
//
// redis哲学: "针对容器类型的读操作，Key不存在等同于容器为空"
func (v *Value) SetEmptyArray() *Value {
	v.typ = TARRAY
	v.array = make([]*Value, 0) // 关键：已初始化但长度为0，Marshal 时编码为 *0
	return v
}

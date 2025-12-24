package protocol

import (
	"bufio"
	"fmt"
	"io"
	"math"
	"strconv"

	"github.com/pkg/errors"
)

// 1.解析指令 判断操作方法
// 2.对于不同的方法去提取参数
// 3.

// 定义操作常量
const (
	STRING  = '+'
	ERROR   = '-'
	INTEGER = ':'
	BULK    = '$'
	ARRAY   = '*'
	DOUBLE  = ','
)

// 定义类型常量
const (
	TARRAY   = "array"
	TBULK    = "bulk"
	TSTRING  = "string"
	TINTEGER = "integer"
	TNULL    = "null"
	TERROR   = "error"
	TDOUBLE  = "double"
)

type Value struct {
	typ     string
	array   []*Value
	bulk    string
	str     string
	integer int
	double  float64
}

type Resp struct {
	reader *bufio.Reader
}

func NewResp(rd io.Reader) *Resp {
	return &Resp{reader: bufio.NewReader(rd)}
}

// readLine 从缓冲区读取数据
// 返回读取到的有效数据以及字节数和可能发送的错误
// 其中有效数据和字节数均不包含 \r\n
func (r *Resp) readLine() ([]byte, int, error) {
	var line []byte
	n := 0
	for {
		b, err := r.reader.ReadByte()
		if err != nil {
			return nil, 0, errors.WithStack(err)
		}
		n += 1
		line = append(line, b)
		if n >= 2 && line[len(line)-2] == '\r' {
			break
		}
	}

	// 返回的行和字节数不携带 \r\n
	return line[:len(line)-2], len(line) - 2, nil
}

// readLength 读取resp中的<length>
// 参考redis的resp协议可知
// <操作符><length>\r\n....
// 整数后面必定是 \r\n
// 所有读取整数时有必要读完一整行(即解决掉尾随的\r\n) 这样可以便于后续解析
func (r *Resp) readLength() (int, error) {
	line, _, err := r.readLine()
	if err != nil {
		return 0, errors.WithStack(err)
	}
	i64, err := strconv.ParseInt(string(line), 10, 64)
	if err != nil {
		return 0, errors.WithStack(err)
	}

	return int(i64), nil
}

func (r *Resp) Read() (*Value, error) {
	typ, err := r.reader.ReadByte()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	switch typ {
	case STRING:
		return r.readString()
	case BULK:
		return r.readBulk()
	case ARRAY:
		return r.readArray()
	case DOUBLE:
		return r.readDouble()
	default:
		return nil, errors.New(fmt.Sprintf("Unknown type: %v", string(typ)))
	}
}

func (r *Resp) readString() (*Value, error) {
	v := new(Value)
	v.typ = TSTRING
	line, _, err := r.readLine()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	v.str = string(line)
	return v, nil
}

// $<length>\r\n<data>\r\n
func (r *Resp) readBulk() (*Value, error) {
	v := new(Value)
	v.typ = TBULK

	len, err := r.readLength()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// 处理 Null Bulk String: $-1\r\n
	if len == -1 {
		v.typ = TNULL
		return v, nil
	}

	// 手动分配一个大小等于bulk的缓冲区
	bulk := make([]byte, len)
	r.reader.Read(bulk)

	v.bulk = string(bulk)

	// 读完剩下的 CRLF
	r.readLine()

	return v, nil
}

// *2\r\n$5\r\nhello\r\n$5\r\nworld\r\n
func (r *Resp) readArray() (*Value, error) {
	v := new(Value)
	v.typ = TARRAY

	// 获取array长度
	len, err := r.readLength()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	v.array = make([]*Value, len)
	for i := 0; i < len; i++ {
		val, err := r.Read() // 递归调用 Read，不只是 readBulk
		if err != nil {
			return nil, errors.WithStack(err)
		}
		v.array[i] = val
	}

	return v, nil
}

// ,1.23\r\n
func (r *Resp) readDouble() (*Value, error) {
	v := new(Value)
	v.typ = TDOUBLE

	data, _, err := r.readLine()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	val, err := strconv.ParseFloat(string(data), 64)
	if err != nil {
		return nil, errors.New("ERR invalid float format")
	}

	v.double = val

	return v, nil
}

// Marshal 将Value结构体转为resp格式 用于客户端响应
func (v *Value) Marshal() []byte {
	switch v.typ {
	case TARRAY:
		return v.marshalArray()
	case TBULK:
		return v.marshalBulk()
	case TSTRING:
		return v.marshalString()
	case TINTEGER:
		return v.marshalInteger()
	case TNULL:
		return v.marshalNull()
	case TERROR:
		return v.marshalError()
	case TDOUBLE:
		return v.marshalDouble()
	default:
		return nil
	}
}

// *<number-of-elements>\r\n<element-1>...<element-n>
func (v *Value) marshalArray() []byte {
	res := make([]byte, 0, 16)
	res = append(res, ARRAY)
	res = append(res, strconv.Itoa(len(v.array))...)
	res = append(res, '\r', '\n')

	for i := 0; i < len(v.array); i++ {
		res = append(res, v.array[i].Marshal()...)
	}
	return res
}

// $<length>\r\n<data>\r\n
func (v *Value) marshalBulk() []byte {
	res := make([]byte, 0, 16)
	res = append(res, BULK)
	res = append(res, strconv.Itoa(len(v.bulk))...)
	res = append(res, '\r', '\n')
	res = append(res, v.bulk...)
	res = append(res, '\r', '\n')
	return res
}

// +OK\r\n
func (v *Value) marshalString() []byte {
	res := make([]byte, 0, 16)
	res = append(res, STRING)
	res = append(res, v.str...)
	res = append(res, '\r', '\n')
	return res
}

// :[<+|->]<value>\r\n
func (v *Value) marshalInteger() []byte {
	res := make([]byte, 0, 16)
	res = append(res, INTEGER)

	// 只需处理正号即可
	if v.integer >= 0 {
		res = append(res, '+')
	}

	// AppendInt 会自动设置负号
	res = strconv.AppendInt(res, int64(v.integer), 10)
	res = append(res, '\r', '\n')

	return res
}

func (v *Value) marshalNull() []byte {
	return []byte("$-1\r\n")
}

// -Error message\r\n
func (v *Value) marshalError() []byte {
	res := make([]byte, 0, 16)
	res = append(res, ERROR)
	res = append(res, v.str...)
	res = append(res, '\r', '\n')
	return res
}

// ,[<+|->]<integral>[.<fractional>][<E|e>[sign]<exponent>]\r\n
func (v *Value) marshalDouble() []byte {
	// 正无穷大、负无穷大和 NaN 值编码如下：
	// ,inf\r\n
	// ,-inf\r\n
	// ,nan\r\n
	if math.IsInf(v.double, 1) {
		return []byte(",inf\r\n")
	}
	if math.IsInf(v.double, -1) {
		return []byte(",-inf\r\n")
	}
	if math.IsNaN(v.double) {
		return []byte(",nan\r\n")
	}

	res := make([]byte, 0, 32)
	res = append(res, DOUBLE)
	if v.double > 0 {
		res = append(res, '+')
	}

	// AppendFloat 会自动设置负号
	res = strconv.AppendFloat(res, v.double, 'g', -1, 64)
	res = append(res, '\r', '\n')
	return res
}

type Writer struct {
	writer io.Writer
}

func NewWriter(w io.Writer) Writer {
	return Writer{writer: w}
}

// Write 将Value写入
func (w *Writer) Write(v *Value) error {
	res := v.Marshal()

	_, err := w.writer.Write(res)
	if err != nil {
		return errors.WithStack(err)
	}

	return nil
}

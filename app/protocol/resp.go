package protocol

import (
	"bufio"
	"io"
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
)

// 定义类型常量
const (
	TARRAY  = "array"
	TBULK   = "bulk"
	TSTRING = "string"
	TNULL   = "null"
	TERROR  = "error"
)

type Value struct {
	typ   string
	str   string
	num   int
	bulk  string
	array []*Value
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
	default:
		return &Value{}, errors.New("无效的运算操作")
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

// Marshal 将Value结构体转为resp格式 用于客户端响应
func (v *Value) Marshal() []byte {
	switch v.typ {
	case TARRAY:
		return v.marshalArray()
	case TBULK:
		return v.marshalBulk()
	case TSTRING:
		return v.marshalString()
	case TNULL:
		return v.marshalNull()
	case TERROR:
		return v.marshalError()
	default:
		return nil
	}
}

// *<number-of-elements>\r\n<element-1>...<element-n>
func (v *Value) marshalArray() []byte {
	var bytes []byte
	bytes = append(bytes, ARRAY)
	bytes = append(bytes, strconv.Itoa(len(v.array))...)
	bytes = append(bytes, '\r', '\n')

	for i := 0; i < len(v.array); i++ {
		bytes = append(bytes, v.array[i].Marshal()...)
	}
	return bytes
}

// $<length>\r\n<data>\r\n
func (v *Value) marshalBulk() []byte {
	var bytes []byte
	bytes = append(bytes, BULK)
	bytes = append(bytes, '\r', '\n')
	bytes = append(bytes, v.bulk...)
	bytes = append(bytes, '\r', '\n')
	return bytes
}

// +OK\r\n
func (v *Value) marshalString() []byte {
	var bytes []byte
	bytes = append(bytes, STRING)
	bytes = append(bytes, v.str...)
	bytes = append(bytes, '\r', '\n')
	return bytes
}

func (v *Value) marshalNull() []byte {
	return []byte("$-1\r\n")
}

// -Error message\r\n
func (v *Value) marshalError() []byte {
	var bytes []byte
	bytes = append(bytes, ERROR)
	bytes = append(bytes, v.str...)
	bytes = append(bytes, '\r', '\n')
	return bytes
}

type Writer struct {
	writer io.Writer
}

func NewWriter(w io.Writer) Writer {
	return Writer{writer: w}
}

// Write 将Value写入
func (w *Writer) Write(v Value) error {
	bytes := v.Marshal()

	_, err := w.writer.Write(bytes)
	if err != nil {
		return errors.WithStack(err)
	}
	
	return nil
}

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
		return &Value{}, errors.New("无效的操作运算")
	}
}

func (r *Resp) readString() (*Value, error) {
	v := new(Value)
	v.typ = "string"
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
	v.typ = "bulk"

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
	v.typ = "array"

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

package connection

import (
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"syscall"

	"github.com/codecrafters-io/redis-starter-go/app/command"
	"github.com/codecrafters-io/redis-starter-go/app/protocol"
	"github.com/pkg/errors"
)

func isNormalDisconnect(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
		return true
	}
	// Windows 特有：WSAECONNRESET (10054)
	if errors.Is(err, syscall.WSAECONNRESET) {
		return true
	}
	// 兜底匹配（不同 Go/平台封装不一致时）
	msg := err.Error()
	return strings.Contains(msg, "forcibly closed by the remote host") ||
		strings.Contains(msg, "connection reset by peer")
}

func Handle(conn net.Conn) {
	defer conn.Close()
	remote := conn.RemoteAddr()

	resp := protocol.NewResp(conn)
	writer := protocol.NewWriter(conn)
	handler := command.NewHandler()

	for {
		value, err := resp.Read()
		if err != nil {
			if isNormalDisconnect(err) {
				return
			}
			log.Printf("[conn %s] read error: %v", remote, err)
			return
		}

		// 检查是否是数组类型 (Redis 命令都是数组格式)
		if value.Array() == nil {
			log.Println("Invalid command format: expected array")
			continue
		}

		// 解析命令
		if len(value.Array()) == 0 {
			log.Println("Empty command")
			continue
		}

		// 获取命令
		cmd := value.Array()[0].Bulk()

		// 获取命令参数
		args := value.Array()[1:]

		response, err := handler.Handle(cmd, args)
		if err != nil {
			// 将错误返回为 RESP 错误回复，避免 nil 传入 Writer
			response = new(protocol.Value).SetError(fmt.Sprintf("ERR %v", err)) // 或 SetError，如果你实现了
		}

		writer.Write(response)
		log.Printf("resp: %+v", *response)
	}
}

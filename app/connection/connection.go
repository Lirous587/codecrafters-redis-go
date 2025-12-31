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

	// 1. 处理标准库定义的 EOF 和连接关闭
	if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
		return true
	}

	// 2. 使用 syscall.ECONNRESET (这是 Linux/Unix 下的连接重置)
	// 大多数情况下，这能涵盖远程主机强制关闭的情况
	if errors.Is(err, syscall.ECONNRESET) {
		return true
	}

	// 3. 兜底匹配：字符串检查
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "forcibly closed") ||
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
			// 优先写入被指定错误
			if resErr := response.Error(); resErr != nil {
				response = new(protocol.Value).SetError(resErr.Error())
			} else {
				response = new(protocol.Value).SetError(fmt.Sprintf("ERR %v", err))
			}
		}

		writer.Write(response)
		log.Printf("resp: %+v", *response)
	}
}

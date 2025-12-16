package main

import (
	"fmt"
	"log"
	"net"
	"os"

	"github.com/codecrafters-io/redis-starter-go/app/command"
	"github.com/codecrafters-io/redis-starter-go/app/protocol"
	"github.com/pkg/errors"
)

var _ = net.Listen
var _ = os.Exit

func main() {
	l, err := net.Listen("tcp", ":6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	log.Println("server start at ", l.Addr())

	for {
		conn, err := l.Accept()
		if err != nil {
			log.Println("Error accepting connection: ", errors.New(err.Error()))
		}

		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	resp := protocol.NewResp(conn)
	writer := protocol.NewWriter(conn)

	for {
		value, err := resp.Read()
		if err != nil {
			log.Println("Error reading:", err.Error())
			break
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

		response, err := command.Handle(cmd, args)
		if err != nil {
			// 将错误返回为 RESP 错误回复，避免 nil 传入 Writer
			response = new(protocol.Value).SetError(fmt.Sprintf("ERR %v", err)) // 或 SetError，如果你实现了
		}

		writer.Write(response)
		log.Printf("resp: %+v", *response)
	}
}

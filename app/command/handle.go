package command

import (
	"errors"
	"fmt"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/protocol"
	"github.com/codecrafters-io/redis-starter-go/app/store"
)

type command string

const (
	// TCODE 用于临时测试某个resp协议编码
	TCODE command = "TCODE"

	PING   command = "PING"
	ECHO   command = "ECHO"
	SET    command = "SET"
	GET    command = "GET"
	LPUSH  command = "LPUSH"
	RPUSH  command = "RPUSH"
	LRANGE command = "LRANGE"
	LLEN   command = "LLEN"
	LPOP   command = "LPOP"
	BLPOP  command = "BLPOP"
	TYPE   command = "TYPE"
	XADD   command = "XADD"
	XRANGE command = "XRANGE"
	XREAD  command = "XREAD"
)

type handlers map[command]func(args []*protocol.Value) (*protocol.Value, error)

func NewHandler() handlers {
	store := store.NewKVStore()

	return handlers{
		TCODE:  handleTCODE,
		PING:   handlePING,
		ECHO:   handleECHO,
		SET:    store.HandleSET,
		GET:    store.HandleGET,
		LPUSH:  store.HandleLPUSH,
		RPUSH:  store.HandleRPUSH,
		LRANGE: store.HandleLRANGE,
		LLEN:   store.HandleLLEN,
		LPOP:   store.HandleLPOP,
		BLPOP:  store.HandleBLPOP,
		TYPE:   store.HandleTYPE,
		XADD:   store.HandleXADD,
		XRANGE: store.HandleXRANGE,
		XREAD:  store.HandleXREAD,
	}
}

func (h handlers) Handle(cmd string, args []*protocol.Value) (*protocol.Value, error) {
	// 规范命令
	c := command(strings.ToUpper(cmd))

	handler, ok := h[c]
	if !ok {
		return nil, fmt.Errorf("unknown command '%q'", cmd)
	}

	return handler(args)
}

func handlePING(args []*protocol.Value) (*protocol.Value, error) {
	switch len(args) {
	case 0:
		return new(protocol.Value).SetStr("PONG"), nil
	case 1:
		return new(protocol.Value).SetBulk(args[0].Bulk()), nil
	default:
		return nil, errors.New("ERR wrong number of arguments for 'ping' command")
	}
}

func handleECHO(args []*protocol.Value) (*protocol.Value, error) {
	if len(args) != 1 {
		return nil, errors.New("ERR wrong number of arguments for 'echo' command")
	}

	return new(protocol.Value).SetBulk(args[0].Bulk()), nil
}

func handleTCODE(args []*protocol.Value) (*protocol.Value, error) {
	magicDouble := 3.14159
	return new(protocol.Value).SetDouble(magicDouble), nil
}

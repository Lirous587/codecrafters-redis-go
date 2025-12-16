package command

import (
	"errors"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/protocol"
)

type command string

const (
	PING command = "PING"
	ECHO command = "ECHO"
	SET  command = "SET"
	GET  command = "GET"
)

var handlers = map[command]func(args []*protocol.Value) (*protocol.Value, error){
	PING: handlePing,
	ECHO: handleEcho,
	GET:  handleGet,
	SET:  handleSet,
}

func Handle(cmd string, args []*protocol.Value) (*protocol.Value, error) {
	// 规范命令
	c := command(strings.ToUpper(cmd))

	handler, ok := handlers[c]
	if !ok {
		return nil, errors.New("ERROR invalid command")
	}

	return handler(args)
}

func handlePing(args []*protocol.Value) (*protocol.Value, error) {
	switch len(args) {
	case 0:
		return new(protocol.Value).SetStr("PONG"), nil
	case 1:
		return new(protocol.Value).SetBulk(args[0].Bulk()), nil
	default:
		return nil, errors.New("ERR wrong number of arguments for 'ping' command")
	}
}

func handleEcho(args []*protocol.Value) (*protocol.Value, error) {
	switch len(args) {
	case 1:
		return new(protocol.Value).SetBulk(args[0].Bulk()), nil
	default:
		return nil, errors.New("ERR wrong number of arguments for 'echo' command")
	}
}

// TODO
func handleGet(args []*protocol.Value) (*protocol.Value, error) {
	panic("todo")
	return nil, nil
}

// TODO
func handleSet(args []*protocol.Value) (*protocol.Value, error) {
	panic("todo")
	return nil, nil
}

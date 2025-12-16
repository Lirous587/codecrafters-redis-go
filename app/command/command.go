package command

import (
	"errors"
	"strings"
	"sync"

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
		return nil, errors.New("unknown command '" + cmd + "'")
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
	if len(args) != 1 {
		return nil, errors.New("ERR wrong number of arguments for 'echo' command")
	}

	return new(protocol.Value).SetBulk(args[0].Bulk()), nil
}

var sets = map[string]string{}
var setsMu = sync.RWMutex{}

func handleSet(args []*protocol.Value) (*protocol.Value, error) {
	if len(args) != 2 {
		return nil, errors.New("ERR wrong number of arguments for 'set' command")
	}

	key := args[0].Bulk()
	value := args[1].Bulk()

	if key == "" {
		return nil, errors.New("ERR invalid key")
	}

	setsMu.Lock()
	defer setsMu.Unlock()
	sets[key] = value

	return new(protocol.Value).SetStr("OK"), nil
}

func handleGet(args []*protocol.Value) (*protocol.Value, error) {
	if len(args) != 1 {
		return nil, errors.New("ERR wrong number of arguments for 'get' command")
	}

	key := args[0].Bulk()
	if key == "" {
		return nil, errors.New("ERR invalid key")
	}

	setsMu.Lock()
	defer setsMu.Unlock()
	value, ok := sets[key]
	if !ok {
		return new(protocol.Value).SetNull(), nil
	}

	return new(protocol.Value).SetBulk(value), nil
}

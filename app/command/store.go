package command

import (
	"errors"
	"strconv"
	"sync"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/protocol"
)

type entry struct {
	data      []byte
	expiresAt time.Time
}

type KVStore struct {
	store map[string]entry
	mutex sync.RWMutex
}

var store = KVStore{
	store: make(map[string]entry),
	mutex: sync.RWMutex{},
}

func handleSet(args []*protocol.Value) (*protocol.Value, error) {
	// 允许 SET key value [EX seconds | PX milliseconds]
	if len(args) != 2 && len(args) != 4 {
		return nil, errors.New("ERR wrong number of arguments for 'set' command")
	}

	key := args[0].Bulk()
	value := args[1].Bulk()

	if key == "" {
		return nil, errors.New("ERR invalid key")
	}

	var expAt time.Time
	if len(args) == 4 {
		opt := args[2].Bulk()
		numStr := args[3].Bulk()
		num, err := strconv.ParseInt(numStr, 10, 64)
		if err != nil || num < 0 {
			return nil, errors.New("ERR value is not an integer or out of range")
		}

		switch opt {
		case "EX":
			// 秒
			expAt = time.Now().Add(time.Duration(num) * time.Second)
		case "PX":
			// 毫秒
			expAt = time.Now().Add(time.Duration(num) * time.Millisecond)
		}
	}

	store.mutex.Lock()
	defer store.mutex.Unlock()
	store.store[key] = entry{
		data:      []byte(value),
		expiresAt: expAt,
	}

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

	store.mutex.Lock()
	defer store.mutex.Unlock()
	entry, ok := store.store[key]

	if !ok {
		return new(protocol.Value).SetNull(), nil
	}

	// 过期时间不为0且已过期
	if !entry.expiresAt.IsZero() && entry.expiresAt.Before(time.Now()) {
		return new(protocol.Value).SetNull(), nil
	}

	return new(protocol.Value).SetBulk(string(entry.data)), nil
}

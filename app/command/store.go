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

var kvOnce sync.Once
var kvStore *KVStore

func NewKVStore() *KVStore {
	kvOnce.Do(func() {
		kvStore = &KVStore{
			store: make(map[string]entry),
		}

		go func() {
			kvStore.handleActiveDelete()
		}()
	})

	return kvStore
}

func (s *KVStore) set(key string, entry *entry) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.store[key] = *entry
}

func (s *KVStore) get(key string) (entry *entry, exist bool, isExpired bool) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	v, ok := s.store[key]

	if !ok {
		return nil, false, false
	}

	isExpired = !v.expiresAt.IsZero() && !v.expiresAt.After(time.Now())

	return &v, true, isExpired
}

func (s *KVStore) delete(key string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	delete(s.store, key)
}

func (s *KVStore) handleSet(args []*protocol.Value) (*protocol.Value, error) {
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

	s.set(key, &entry{
		data:      []byte(value),
		expiresAt: expAt,
	})

	return new(protocol.Value).SetStr("OK"), nil
}

func (s *KVStore) handleGet(args []*protocol.Value) (*protocol.Value, error) {
	if len(args) != 1 {
		return nil, errors.New("ERR wrong number of arguments for 'get' command")
	}

	key := args[0].Bulk()
	if key == "" {
		return nil, errors.New("ERR invalid key")
	}

	entry, exist, isExpired := s.get(key)

	if !exist {
		return new(protocol.Value).SetNull(), nil
	}

	// 过期则删除并返回nil
	if isExpired {
		s.delete(key)
		return new(protocol.Value).SetNull(), nil
	}

	return new(protocol.Value).SetBulk(string(entry.data)), nil
}

func (s *KVStore) handleActiveDelete() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	hz := 10
	for range ticker.C {
		count := 0
		s.mutex.Lock()
		now := time.Now()
		for k, v := range s.store {
			if count >= hz {
				break
			}

			if !v.expiresAt.IsZero() && !v.expiresAt.After(now) {
				delete(s.store, k)
			}
			count++
		}
		s.mutex.Unlock()
	}
}

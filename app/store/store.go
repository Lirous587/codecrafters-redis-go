package store

import (
	"strconv"
	"sync"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/protocol"
	"github.com/pkg/errors"
)

type ValueType int8

const (
	TypeString ValueType = iota
	TypeList
)

type Entity struct {
	Type      ValueType
	ExpiredAt time.Time
	Data      interface{}
}

type KVStore struct {
	store map[string]*Entity
	// 用于 List 的阻塞等待 对于一个key可能有多个连接阻塞等待 故需要用一个[]chan去处理 以此实现后续的FIFO
	listWaiters map[string][]chan ListPayload
	// 未来可能需要的阻塞
	// streamWaiters map[string][]chan StreamPayload
	// zsetWaiters   map[string][]chan ZSetPayload
	mutex sync.RWMutex
}

var kvOnce sync.Once
var kvStore *KVStore

func NewKVStore() *KVStore {
	kvOnce.Do(func() {
		kvStore = &KVStore{
			store: make(map[string]*Entity),
			listWaiters: make(map[string][]chan ListPayload),
		}

		go func() {
			kvStore.handleActiveDelete()
		}()
	})

	return kvStore
}

// ---------------------------------------------------------
// raw 操作
// 约定：调用这些方法前，必须已经持有相应的锁
// ---------------------------------------------------------

// rawSet 外部必须持有写锁
func (s *KVStore) rawSet(key string, entity *Entity) {
	s.store[key] = entity
}

// rawGet 外部必须持有写锁
func (s *KVStore) rawGet(key string) (*Entity, bool) {
	entity, ok := s.store[key]

	if !ok {
		return nil, false
	}

	isExpired := !entity.ExpiredAt.IsZero() && !entity.ExpiredAt.After(time.Now())
	if isExpired {
		delete(s.store, key)
		return nil, false
	}

	return entity, true
}

func (s *KVStore) Set(key string, entity *Entity) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.rawSet(key, entity)
}

// get 获取数据同时内部处理过期逻辑
// 1.没过期则返回存在
// 2.过期则返回不存在且删除
func (s *KVStore) Get(key string) (*Entity, bool) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.rawGet(key)
}

func (s *KVStore) handleActiveDelete() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	hz := 10
	for range ticker.C {
		count := 0
		s.mutex.Lock()
		now := time.Now()
		for k, entity := range s.store {
			if count >= hz {
				break
			}

			if !entity.ExpiredAt.IsZero() && !entity.ExpiredAt.After(now) {
				delete(s.store, k)
			}
			count++
		}
		s.mutex.Unlock()
	}
}

func (s *KVStore) HandleSet(args []*protocol.Value) (*protocol.Value, error) {
	// 允许 SET key value [EX seconds | PX milliseconds]
	if len(args) != 2 && len(args) != 4 {
		return nil, errors.New(emsgArgsNumber("set"))
	}

	key := args[0].Bulk()
	value := args[1].Bulk()

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

	s.Set(key, &Entity{
		Type:      TypeString,
		ExpiredAt: expAt,
		Data:      value,
	})

	return new(protocol.Value).SetStr("OK"), nil
}

func (s *KVStore) HandleGet(args []*protocol.Value) (*protocol.Value, error) {
	if len(args) != 1 {
		return nil, errors.New(emsgArgsNumber("get"))
	}

	key := args[0].Bulk()

	entity, exist := s.Get(key)

	if !exist {
		return new(protocol.Value).SetNull(), nil
	}

	str, ok := entity.Data.(string)
	if !ok {
		return new(protocol.Value).SetNull(), nil
	}

	return new(protocol.Value).SetBulk(str), nil
}

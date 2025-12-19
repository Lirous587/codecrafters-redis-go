package store

import (
	"errors"
	"strconv"
	"sync"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/protocol"
)

type ValueType int8

const (
	TypeString ValueType = iota
	TypeList
)

type Value struct {
	Type      ValueType
	ExpiredAt time.Time
	Data      interface{}
}

type KVStore struct {
	store map[string]Value
	mutex sync.RWMutex
}

var kvOnce sync.Once
var kvStore *KVStore

func NewKVStore() *KVStore {
	kvOnce.Do(func() {
		kvStore = &KVStore{
			store: make(map[string]Value),
		}

		go func() {
			kvStore.handleActiveDelete()
		}()
	})

	return kvStore
}

func (s *KVStore) set(key string, value *Value) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.store[key] = *value
}

// 获取数据
// 1.没过期则返回存在
// 2.过期则返回不存在且删除
func (s *KVStore) get(key string) (*Value, bool) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	v, ok := s.store[key]

	if !ok {
		return nil, false
	}

	isExpired := !v.ExpiredAt.IsZero() && !v.ExpiredAt.After(time.Now())
	// 过期则顺手删除 避免交给上级去处理
	if isExpired {
		delete(s.store, key)
		return nil, false
	}

	return &v, true
}

func (s *KVStore) delete(key string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	delete(s.store, key)
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

			if !v.ExpiredAt.IsZero() && !v.ExpiredAt.After(now) {
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

	s.set(key, &Value{
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

	value, exist := s.get(key)

	if !exist {
		return new(protocol.Value).SetNull(), nil
	}

	str, ok := value.Data.(string)
	if !ok {
		return new(protocol.Value).SetNull(), nil
	}

	return new(protocol.Value).SetBulk(str), nil
}

// HandleRPush
// 将所有指定的值插入到存储在 key 的列表尾部。
// 如果 key 不存在，则在执行推送操作之前将其创建为空列表。
// 当 key 包含的值不是列表时，将返回错误。
func (s *KVStore) HandleRPush(args []*protocol.Value) (*protocol.Value, error) {
	if len(args) < 2 {
		return nil, errors.New(emsgArgsNumber("rpush"))
	}

	key := args[0].Bulk()

	s.mutex.RLock()
	value, exist := s.store[key]
	s.mutex.RUnlock()

	var list []string

	if exist {
		if value.Type != TypeList {
			return nil, errors.New(emsgKeyType())
		}
		list = value.Data.([]string)
	} else {
		list = make([]string, 0, len(args)-1)
	}

	for i := range args[1:] {
		list = append(list, args[1+i].Bulk())
	}

	s.set(key, &Value{
		Type: TypeList,
		// TODO
		// ExpiredAt: ,
		Data: list,
	})

	return new(protocol.Value).SetInteger(len(list)), nil
}

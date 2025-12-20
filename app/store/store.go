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

type Value struct {
	Type      ValueType
	ExpiredAt time.Time
	Data      interface{}
}

type KVStore struct {
	store map[string]*Value
	mutex sync.RWMutex
}

var kvOnce sync.Once
var kvStore *KVStore

func NewKVStore() *KVStore {
	kvOnce.Do(func() {
		kvStore = &KVStore{
			store: make(map[string]*Value),
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
func (s *KVStore) rawSet(key string, value *Value) {
	s.store[key] = value
}

// rawGet 外部必须持有写锁
func (s *KVStore) rawGet(key string) (*Value, bool) {
	v, ok := s.store[key]

	if !ok {
		return nil, false
	}

	isExpired := !v.ExpiredAt.IsZero() && !v.ExpiredAt.After(time.Now())
	if isExpired {
		delete(s.store, key)
		return nil, false
	}

	return v, true
}

func (s *KVStore) Set(key string, value *Value) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.rawSet(key, value)
}

// get 获取数据同时内部处理过期逻辑
// 1.没过期则返回存在
// 2.过期则返回不存在且删除
func (s *KVStore) Get(key string) (*Value, bool) {
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

	s.Set(key, &Value{
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

	value, exist := s.Get(key)

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

	s.mutex.Lock()
	defer s.mutex.Unlock()

	key := args[0].Bulk()
	value, exist := s.store[key]

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

	s.rawSet(key, &Value{
		Type: TypeList,
		// TODO
		// ExpiredAt: ,
		Data: list,
	})

	return new(protocol.Value).SetInteger(len(list)), nil
}

// HandleLRange
// 返回存储在 key 中的列表的指定元素。偏移量 start 和 stop 是零基索引， 0 是列表的第一个元素（列表的头部）， 1 是下一个元素，以此类推。
// 这些偏移量也可以是负数，表示从列表末尾开始的偏移量。例如， -1 是列表的最后一个元素， -2 是倒数第二个，以此类推。
func (s *KVStore) HandleLRange(args []*protocol.Value) (*protocol.Value, error) {
	// lrange key start stop
	if len(args) != 3 {
		return nil, errors.New(emsgArgsNumber("lrange"))
	}

	key := args[0].Bulk()

	s.mutex.Lock()
	defer s.mutex.Unlock()

	value, ok := s.store[key]
	if !ok {
		return new(protocol.Value).SetEmptyArray(), nil
	}

	if value.Type != TypeList {
		return nil, errors.New(emsgKeyType())
	}

	list := value.Data.([]string)

	startArg, err := args[1].BulkToInteger()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	// 处理负索引
	if startArg < 0 {
		startArg = len(list) + startArg
	}

	stopArg, err := args[2].BulkToInteger()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	// 处理负索引
	if stopArg < 0 {
		stopArg = len(list) + stopArg
	}

	// 1.startArg 如果越过list长度则返回null
	if startArg >= len(list) {
		return new(protocol.Value).SetEmptyArray(), nil
	}

	// 2.stopArg 如果小于 startArg 则返回null
	if stopArg < startArg {
		return new(protocol.Value).SetEmptyArray(), nil
	}
	// 3.stopArg 如果越过list长度则返回剩下数据
	if stopArg >= len(list) {
		stopArg = len(list) - 1
	}

	length := stopArg - startArg + 1

	resList := make([]*protocol.Value, 0, length)

	// 左开右闭
	for i := range list[startArg : stopArg+1] {
		resList = append(resList, new(protocol.Value).SetBulk(list[startArg+i]))
	}

	return new(protocol.Value).SetArray(resList), nil
}

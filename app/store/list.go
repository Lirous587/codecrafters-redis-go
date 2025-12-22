package store

import (
	"github.com/codecrafters-io/redis-starter-go/app/protocol"
	"github.com/codecrafters-io/redis-starter-go/app/utils"
	"github.com/pkg/errors"
)

// HandleLPush
// 将所有指定的值插入到存储在 key 的列表头部。如果 key 不存在，则在执行推送操作之前将其创建为空列表。当 key 包含的值不是列表时，将返回错误。
// 可以使用单个命令调用，在命令末尾指定多个参数来推送多个元素。元素会依次插入到列表头部，从最左边的元素到最右边的元素。所以例如，命令 LPUSH mylist a b c 将会生成一个列表，其中 c 是第一个元素， b 是第二个元素， a 是第三个元素。
func (s *KVStore) HandleLPush(args []*protocol.Value) (*protocol.Value, error) {
	if len(args) < 2 {
		return nil, errors.New(emsgArgsNumber("lpush"))
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
		// list = make([]string, 0, len(args)-1)
	}

	// 先处理prepend 最后一次性合并 避免每个元素都需要在原有基础上prepend
	prepend := make([]string, 0, len(args)-1)

	for i := len(args); i > 1; i-- {
		prepend = append(prepend, args[i-1].Bulk())
	}

	resList := append(prepend, list...)

	s.rawSet(key, &Value{
		Type: TypeList,
		// TODO
		// ExpiredAt: ,
		Data: resList,
	})

	return new(protocol.Value).SetInteger(len(resList)), nil
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
// 有count的时候始终返回array，否则返回str
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

	stopArg, err := args[2].BulkToInteger()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	safeStart, safeStop := utils.NormalizeRange(startArg, stopArg, len(list))
	subList := list[safeStart:safeStop]

	resList := make([]*protocol.Value, 0, len(subList))
	for i := range subList {
		resList = append(resList, new(protocol.Value).SetBulk(subList[i]))
	}

	return new(protocol.Value).SetArray(resList), nil
}

func (s *KVStore) HandleLlen(args []*protocol.Value) (*protocol.Value, error) {
	if len(args) != 1 {
		return nil, errors.New(emsgArgsNumber("llen"))
	}

	key := args[0].Bulk()

	s.mutex.RLock()
	defer s.mutex.RUnlock()

	value, ok := s.store[key]
	if !ok {
		return new(protocol.Value).SetInteger(0), nil
	}

	if value.Type != TypeList {
		return nil, errors.New(emsgKeyType())
	}

	list := value.Data.([]string)

	return new(protocol.Value).SetInteger(len(list)), nil
}

// HandleLpop
// 移除并返回存储在 key 中的列表的第一个元素。
// 默认情况下，该命令从列表的开头弹出一个元素。当提供可选的 count 参数时，回复将包含最多 count 个元素，具体取决于列表的长度。
func (s *KVStore) HandleLpop(args []*protocol.Value) (*protocol.Value, error) {
	if len(args) != 1 && len(args) != 2 {
		return nil, errors.New(emsgArgsNumber("lpop"))
	}

	key := args[0].Bulk()

	hasCountParam := len(args) == 2
	count := 1
	if hasCountParam {
		var err error
		if count, err = args[1].BulkToInteger(); err != nil {
			return nil, errors.WithStack(err)
		}
		if count < 0 {
			return nil, errors.New("ERR value is out of range, must be positive")
		}
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	value, ok := s.rawGet(key)
	if !ok {
		return new(protocol.Value).SetNull(), nil
	}

	if value.Type != TypeList {
		return nil, errors.New(emsgKeyType())
	}

	list := value.Data.([]string)
	length := len(list)

	if length == 0 {
		return new(protocol.Value).SetNull(), nil
	}

	disposeList := func(resLength int) {
		// 当前键已被全部删除
		if resLength == length {
			delete(s.store, key)
		} else {
			s.rawSet(key, &Value{
				Type: TypeList,
				Data: list[count:],
			})
		}
	}

	// 处理返回
	// 1.没有count参数
	if !hasCountParam {
		disposeList(1)

		return new(protocol.Value).SetBulk(list[0]), nil
	}
	// 2.有count参数
	if count > length {
		count = length
	}

	resList := make([]*protocol.Value, 0, count)

	for i := 0; i < count; i++ {
		v := new(protocol.Value).SetBulk(list[i])
		resList = append(resList, v)
	}

	// 当前键已被全部删除
	disposeList(len(resList))

	return new(protocol.Value).SetArray(resList), nil
}

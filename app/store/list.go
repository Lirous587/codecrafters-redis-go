package store

import (
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/protocol"
	"github.com/codecrafters-io/redis-starter-go/app/utils"
	"github.com/pkg/errors"
)

type ListPayload struct {
	key   string
	value string
}

// HandleLPush
// 将所有指定的值插入到存储在 key 的列表头部。如果 key 不存在，则在执行推送操作之前将其创建为空列表。当 key 包含的值不是列表时，将返回错误。
// 可以使用单个命令调用，在命令末尾指定多个参数来推送多个元素。元素会依次插入到列表头部，从最左边的元素到最右边的元素。所以例如，命令 LPUSH mylist a b c 将会生成一个列表，其中 c 是第一个元素， b 是第二个元素， a 是第三个元素。
// 整数回复：推送操作后列表的长度。
// 遵循等待者优先原则 即push时若有blpop这种消费者的话 那么将直接消费数据
// for example:
// blpop list_key 0
// lpush list_key val
// 这个时候lpush的返回结构将为0而不是1
func (s *KVStore) HandleLPush(args []*protocol.Value) (*protocol.Value, error) {
	if len(args) < 2 {
		return nil, errors.New(emsgArgsNumber("lpush"))
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	key := args[0].Bulk()

	entity, exist := s.store[key]
	var list []string
	if exist {
		if entity.Type != TypeList {
			return nil, errors.New(emsgKeyType())
		}
		list = entity.Data.([]string)
	}
	resLen := len(list) + len(args) - 1

	valuesToPush := make([]string, 0, len(args)-1)
	for i := range args[1:] {
		valuesToPush = append(valuesToPush, args[1+i].Bulk())
	}

	// remainingValues 用于收集没有被 waiter 消费掉的值
	remainingValue := make([]string, 0, len(valuesToPush))

	// 需要对每个值都进行是否消费处理
	for _, val := range valuesToPush {
		// 存在waiter 则需要让其优先消费 不做后续存储
		if waiters, ok := s.listWaiters[key]; ok && len(waiters) > 0 {
			waiter := waiters[0]
			newWaiter := waiters[1:]
			s.listWaiters[key] = newWaiter

			waiter <- ListPayload{
				key:   key,
				value: val,
			}

			continue
		}

		remainingValue = append(remainingValue, val)
	}

	resList := append(remainingValue, list...)

	if len(resList) == 0 {
		delete(s.store, key)
	} else {
		s.rawSet(key, &Entity{
			Type: TypeList,
			// TODO
			// ExpiredAt: ,
			Data: resList,
		})
	}

	return new(protocol.Value).SetInteger(resLen), nil
}

// HandleRPush
// 将所有指定的值插入到存储在 key 的列表尾部。
// 如果 key 不存在，则在执行推送操作之前将其创建为空列表。
// 当 key 包含的值不是列表时，将返回错误。
// 整数回复：推送操作后列表的长度。
// RPUSH 同样 遵循等待者优先原则
func (s *KVStore) HandleRPush(args []*protocol.Value) (*protocol.Value, error) {
	if len(args) < 2 {
		return nil, errors.New(emsgArgsNumber("rpush"))
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	key := args[0].Bulk()

	entity, exist := s.store[key]

	var list []string
	if exist {
		if entity.Type != TypeList {
			return nil, errors.New(emsgKeyType())
		}
		list = entity.Data.([]string)
	}

	resLen := len(list) + len(args) - 1

	valuesToPush := make([]string, 0, len(args)-1)
	for i := range args[1:] {
		valuesToPush = append(valuesToPush, args[1+i].Bulk())
	}
	remainingValues := make([]string, 0, len(valuesToPush))

	for _, v := range valuesToPush {
		if waiters, ok := s.listWaiters[key]; ok && len(waiters) > 0 {
			waiter := waiters[0]
			newWaiters := waiters[1:]
			waiter <- ListPayload{
				key:   key,
				value: v,
			}
			s.listWaiters[key] = newWaiters
			continue
		}
		remainingValues = append(remainingValues, v)
	}

	resList := append(list, remainingValues...)

	s.rawSet(key, &Entity{
		Type: TypeList,
		// TODO
		// ExpiredAt: ,
		Data: resList,
	})

	return new(protocol.Value).SetInteger(resLen), nil
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

	entity, ok := s.store[key]
	if !ok {
		return new(protocol.Value).SetEmptyArray(), nil
	}

	if entity.Type != TypeList {
		return nil, errors.New(emsgKeyType())
	}

	list := entity.Data.([]string)

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

func (s *KVStore) HandleLLen(args []*protocol.Value) (*protocol.Value, error) {
	if len(args) != 1 {
		return nil, errors.New(emsgArgsNumber("llen"))
	}

	key := args[0].Bulk()

	s.mutex.RLock()
	defer s.mutex.RUnlock()

	entity, ok := s.store[key]
	if !ok {
		return new(protocol.Value).SetInteger(0), nil
	}

	if entity.Type != TypeList {
		return nil, errors.New(emsgKeyType())
	}

	list := entity.Data.([]string)

	return new(protocol.Value).SetInteger(len(list)), nil
}

// HandleLpop
// 移除并返回存储在 key 中的列表的第一个元素。
// 默认情况下，该命令从列表的开头弹出一个元素。当提供可选的 count 参数时，回复将包含最多 count 个元素，具体取决于列表的长度。
func (s *KVStore) HandleLPop(args []*protocol.Value) (*protocol.Value, error) {
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

	entity, ok := s.rawGet(key)
	if !ok {
		return new(protocol.Value).SetNull(), nil
	}

	if entity.Type != TypeList {
		return nil, errors.New(emsgKeyType())
	}

	list := entity.Data.([]string)
	length := len(list)

	if length == 0 {
		return new(protocol.Value).SetNull(), nil
	}

	disposeList := func(resLength int) {
		// 当前键已被全部删除
		if resLength == length {
			delete(s.store, key)
		} else {
			s.rawSet(key, &Entity{
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

// HandleBLPOP
// 当调用 BLPOP 时，如果指定的至少一个键包含非空列表，则从列表头部弹出一个元素并返回给调用者，同时返回它被弹出的 key 。
// 键是按照给定的顺序进行检查的。假设键 list1 不存在，而 list2 和 list3 包含非空列表。考虑以下命令：
// BLPOP list1 list2 list3 0
// BLPOP 保证从 list2 存储的列表中返回一个元素（因为它在检查 list1 、 list2 和 list3 时是第一个非空列表）。
// 如果指定的键都不存在， BLPOP 会阻塞连接，直到另一个客户端对其中一个键执行 LPUSH 或 RPUSH 操作。
// 当其中一个列表出现新数据时，客户端返回解阻塞该键的键名和弹出的值。
// 当 BLPOP 导致客户端阻塞且指定了非零超时时，如果超时到期前没有至少一个指定的键执行推送操作，客户端将解阻塞并返回一个 nil 多批量值。
// 超时参数timeout被解释为一个双精度值，指定最大阻塞秒数。零超时可用于无限期阻塞。
// blop key1 key2 ... timeout
func (s *KVStore) HandleBLPOP(args []*protocol.Value) (*protocol.Value, error) {
	if len(args) < 2 {
		return nil, errors.New(emsgArgsNumber("blpop"))
	}

	keys := make([]string, 0, len(args)-1)
	for i := 0; i < len(args)-1; i++ {
		keys = append(keys, args[i].Bulk())
	}

	timeout, err := args[len(args)-1].BulkToDouble()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// 1.直接可以拿到数据时
	// 非阻塞处理
	// 先直接遍历key 以确保按顺寻
	s.mutex.Lock()
	for _, key := range keys {
		if entity, ok := s.store[key]; ok && entity.Type == TypeList {
			list := entity.Data.([]string)
			popVal := list[0]
			if len(list) == 1 {
				delete(s.store, key)
			} else {
				s.store[key].Data = list[1:]
			}
			s.mutex.Unlock()
			return new(protocol.Value).
					SetArray([]*protocol.Value{
						new(protocol.Value).SetBulk(key),
						new(protocol.Value).SetBulk(popVal),
					}),
				nil
		}
	}

	// 2.不可拿到数据时
	// 阻塞处理
	var timeoutCh <-chan time.Time
	if timeout > 0 {
		timeoutCh = time.After(time.Duration(timeout) * time.Second)
	}

	pendingCh := make(chan ListPayload, 1)
	for _, key := range keys {
		// 向listWaiters中添加pendingCh
		s.listWaiters[key] = append(s.listWaiters[key], pendingCh)
	}
	s.mutex.Unlock()

	// 清理函数
	// 无论是超时还是拿到数据，最后都要把这个 chan 从 map 里删掉
	// 否则 map 会无限膨胀
	cleanup := func() {
		s.mutex.Lock()
		defer s.mutex.Unlock()
		for _, key := range keys {
			listWaiters := s.listWaiters[key]

			if len(listWaiters) == 0 {
				continue
			}

			newWaiters := make([]chan ListPayload, 0, len(listWaiters))
			for _, waiter := range listWaiters {
				if pendingCh != waiter {
					newWaiters = append(newWaiters, waiter)
				}
			}
			s.listWaiters[key] = newWaiters
		}
	}
	defer cleanup()

	select {
	case res := <-pendingCh:
		return new(protocol.Value).
				SetArray([]*protocol.Value{
					new(protocol.Value).SetBulk(res.key),
					new(protocol.Value).SetBulk(res.value),
				}),
			nil
	case <-timeoutCh:
		// 超时处理
		return new(protocol.Value).SetNull(), nil
	}
}

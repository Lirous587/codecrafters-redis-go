package store

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/protocol"
	"github.com/pkg/errors"
)

// StreamEntity 中包含多个条目
type StreamEntity struct {
	timestamp int64
	seq       int64
	Fields    []string // 存储 key1 val1 key2 val2 保证顺序
}

type Stream struct {
	entities      []StreamEntity
	lastTimestamp int64
	lastSeq       int64
}

type streamHelper struct {
}

// validateAndUpdateID 验证并更新 stream 的最新 ID
// 1.id 要大于0-0 (0-1是最小值)
// 2.id 总为递增规则
// 当 timestamp相同时 后来者的seq要递增
func (h *streamHelper) validateAndUpdateID(stream *Stream, timestamp, seq int64) error {
	// 检查 ID 必须大于 0-0
	// 1.timestamp < 0 || seq < 0
	// 将范围缩小至 (0-+无穷 0-+无穷)
	// (timestamp == 0 && seq == 0)
	// 2.将范围缩小到 (0-+无穷 1-+无穷)
	if timestamp < 0 || seq < 0 || (timestamp == 0 && seq == 0) {
		return errors.New("ERR The ID specified in XADD must be greater than 0-0")
	}

	// 比较stream最后一个合法id和最新的id
	// 不合法情况
	// 1.timestamp < stream.lastTimestamp
	// 新的timestamp小于lastTimestamp
	// 2.(timestamp == stream.lastTimestamp && seq <= stream.lastSeq)
	// 新的timestamp等于lastTimeStamp且新的seq不大于lastSeq
	if timestamp < stream.lastTimestamp || (timestamp == stream.lastTimestamp && seq <= stream.lastSeq) {
		return errors.New("ERR The ID specified in XADD is equal or smaller than the target stream top item")
	}

	stream.lastTimestamp = timestamp
	stream.lastSeq = seq

	return nil
}

func (h *streamHelper) parseIDOrAutoGen(stream *Stream, id string) (int64, int64, error) {
	// 完全自动生成 暂不实现
	if id == "*" {
		timestamp := time.Now().UnixMilli()
		seq := int64(0)
		if timestamp == stream.lastTimestamp {
			seq = stream.lastSeq + 1
		}
		return timestamp, seq, nil
	}

	strs := strings.Split(id, "-")
	if len(strs) != 2 {
		return 0, 0, errors.New("ERR Invalid stream ID specified as stream command argument")
	}

	timestampStr := strs[0]
	timestamp, err := strconv.ParseInt(timestampStr, 10, 64)
	if err != nil {
		return 0, 0, errors.WithStack(err)
	}

	// 自动生成序列号
	// 特殊情况 timestamp为0时 seq从1开始
	if strs[1] == "*" {
		seq := int64(0)

		switch timestamp {
		case stream.lastTimestamp: // 如果 timestamp 与上一个相同，则递增序列号
			seq = stream.lastSeq + 1
		case 0: // timestamp 为 0 的特殊情况,seq 从 1 开始
			seq = 1
		}

		return timestamp, seq, nil
	}

	// 显式指定序列号
	seqStr := strs[1]
	seq, err := strconv.ParseInt(seqStr, 10, 64)
	if err != nil {
		return 0, 0, errors.WithStack(err)
	}

	return timestamp, seq, nil
}

// HandleXADD
// 必须参数:
// 1.key
// 2.id
// 3.field value 至少提供一个键值对
// 例如:
// -XADD stream_key 1526919030474-0 temperature 36 humidity 95
// -XADD stream_key 1526919030474-* temperature 36 humidity 95
// -XADD stream_key * temperature 36 humidity 95
//
// 返回值:
// bulk string 添加条目的ID
// nil reply (bulk string) 如果提供 NOMKSTREAM 选项且键不存在
func (s *KVStore) HandleXADD(args []*protocol.Value) (*protocol.Value, error) {
	// 参数不少于四并且参数数量必须为偶数
	if len(args) < 4 || len(args)%2 != 0 {
		return nil, errors.New(emsgArgsNumber("xadd"))
	}

	key := args[0].Bulk()

	s.mutex.Lock()
	defer s.mutex.Unlock()

	entity, ok := s.store[key]
	var stream *Stream
	if !ok {
		stream = &Stream{
			entities: make([]StreamEntity, 0, 1), // 直接为新的entity分配空间
		}
	} else {
		if entity.Type != TypeStream {
			return nil, errors.New(emsgKeyType())
		}
		stream = entity.Data.(*Stream)
	}

	id := args[1].Bulk()
	helper := new(streamHelper)
	timestamp, seq, err := helper.parseIDOrAutoGen(stream, id)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if err := helper.validateAndUpdateID(stream, timestamp, seq); err != nil {
		return new(protocol.Value).SetError(err.Error()), errors.WithStack(err)
	}

	// 构造实际的id
	actualID := fmt.Sprintf("%d-%d", timestamp, seq)

	streamEntity := StreamEntity{
		timestamp: timestamp,
		seq:       seq,
		Fields:    make([]string, 0, len(args)/2-1),
	}

	for _, v := range args[2:] {
		streamEntity.Fields = append(streamEntity.Fields, v.Bulk())
	}
	stream.entities = append(stream.entities, streamEntity)

	s.rawSet(key, &Entity{
		Type: TypeStream,
		// wait todo
		// ExpiredAt: ,
		Data: stream,
	})

	return new(protocol.Value).SetBulk(actualID), nil
}

// parseID 由于解析xrange的id
// 序列号是可选的 如果不提供
// 对于start ID 序列号默认为 0
// 对于end ID 序列号默认为最大序列号
// timestamp
// timestamp-seq
func (h *streamHelper) parseID(str string, isStart bool) (timestamp int64, seq int64, err error) {
	strs := strings.Split(str, "-")

	var timestampStr, seqStr string
	if len(strs) != 1 && len(strs) != 2 {
		return 0, 0, errors.New("ERR Invalid stream ID specified as stream command argument")
	}

	timestampStr = strs[0]

	if len(strs) == 2 {
		seqStr = strs[1]
	}

	timestamp, err = strconv.ParseInt(timestampStr, 10, 64)
	if err != nil {
		return 0, 0, errors.WithStack(err)
	}

	if seqStr != "" {
		seq, err = strconv.ParseInt(seqStr, 10, 64)
		if err != nil {
			return 0, 0, errors.WithStack(err)
		}
		return timestamp, seq, nil
	}

	// 未设置seq
	// start的seq设置为0
	if isStart {
		return timestamp, 0, nil
	} else {
		// end的seq设置为math.MaxInt64
		return timestamp, math.MaxInt64, nil
	}
}

// compareID 辅助函数
// id1 < id2:-1
// id1 = id2:0
// id1 > id2:1
func (h *streamHelper) compareID(t1, s1, t2, s2 int64) int {
	if t1 < t2 {
		return -1
	}
	if t1 > t2 {
		return 1
	}
	// 时间错相等 比较序列号
	if s1 < s2 {
		return -1
	}
	if s1 > s2 {
		return 1
	}
	return 0
}

// HandleXRange
// XRANGE some_key 1526985054069 1526985054079
// XRANGE some_key 1526985054069-* 1526985054079-*
// 序列号是可选的 如果不提供
// 对于start ID 序列号默认为 0
// 对于end ID 序列号默认为最大序列号
//
// start和end也应包含在内
// 策略
// 依旧使用切片 暂不使用基数树
// 使用sort.Search查询到start然后直到end结束
func (s *KVStore) HandleXRANGE(args []*protocol.Value) (*protocol.Value, error) {
	if len(args) != 3 {
		return nil, errors.New(emsgArgsNumber("xrange"))
	}

	s.mutex.RLock()
	defer s.mutex.RUnlock()

	key := args[0].Bulk()

	entity, ok := s.store[key]
	if !ok {
		return new(protocol.Value).SetEmptyArray(), nil
	}

	if entity.Type != TypeStream {
		return nil, errors.New(emsgKeyType())
	}

	startStr := args[1].Bulk()
	endStr := args[2].Bulk()

	helper := new(streamHelper)

	t1, s1, err := helper.parseID(startStr, true)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	t2, s2, err := helper.parseID(endStr, false)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	stream := entity.Data.(*Stream)
	entities := stream.entities

	// 找到第一个大于start的索引i
	startIndex := sort.Search(len(stream.entities), func(i int) bool {
		entity := entities[i]
		ti, si := entity.timestamp, entity.seq
		// id(i) >= id(start)
		return helper.compareID(ti, si, t1, s1) >= 0
	})

	result := new(protocol.Value).SetEmptyArray()

	// 从startIndex开始遍历直到不再满足条件
	for _, v := range entities[startIndex:] {
		ti, si := v.timestamp, v.seq
		// id(i) <= id(end) 才继续,如果 id(i) > id(end) 则退出
		if helper.compareID(ti, si, t2, s2) > 0 {
			break
		}

		entityArr := new(protocol.Value).SetEmptyArray()
		id := new(protocol.Value).SetBulk(fmt.Sprintf("%d-%d", v.timestamp, v.seq))
		fileds := make([]*protocol.Value, 0, len(v.Fields))
		for _, filed := range v.Fields {
			fileds = append(fileds, new(protocol.Value).SetBulk(filed))
		}
		fieldsArray := new(protocol.Value).SetArray(fileds)
		entityArr.Append(id, fieldsArray)
		result.Append(entityArr)
	}

	return result, nil
}

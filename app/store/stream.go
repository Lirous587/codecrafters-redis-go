package store

import (
	"fmt"
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

// validateAndUpdateStreamID 验证并更新 stream 的最新 ID
// 1.id 要大于0-0 (0-1是最小值)
// 2.id 总为递增规则
// 当 timestamp相同时 后来者的seq要递增
func validateAndUpdateStreamID(stream *Stream, timestamp, seq int64) error {
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

func parseStreamIDOrAutoGen(stream *Stream, id string) (int64, int64, error) {
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
// 例如:XADD stream_key 1526919030474-0 temperature 36 humidity 95
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
	timestamp, seq, err := parseStreamIDOrAutoGen(stream, id)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if err := validateAndUpdateStreamID(stream, timestamp, seq); err != nil {
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

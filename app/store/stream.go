package store

import (
	"strconv"
	"strings"

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
	entities        []StreamEntity
	lastIDTimeStamp int64
	lastIDSeq       int64
}

// splitStreamID 分割stream id,得到timestamp和seq
func splitStreamID(id string) (int64, int64, error) {
	strs := strings.Split(id, "-")
	if len(strs) != 2 {
		return 0, 0, errors.New("")
	}

	timestampStr := strs[0]
	timestamp, err := strconv.ParseInt(timestampStr, 10, 64)
	if err != nil {
		return 0, 0, errors.WithStack(err)
	}

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
			entities: make([]StreamEntity, 0, 1), // 除开key和id
		}
	} else {
		if entity.Type != TypeStream {
			return nil, errors.New(emsgKeyType())
		}
		stream = entity.Data.(*Stream)
	}

	id := args[1].Bulk()
	timestamp, seq, err := splitStreamID(id)
	if err != nil {
		return nil, errors.WithStack(err)
	}

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

	return new(protocol.Value).SetBulk(id), nil
}

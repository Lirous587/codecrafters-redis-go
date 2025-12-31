package store

import (
	"github.com/codecrafters-io/redis-starter-go/app/protocol"
	"github.com/pkg/errors"
)

func (s *KVStore) HandleTYPE(args []*protocol.Value) (*protocol.Value, error) {
	if len(args) > 1 {
		return nil, errors.New(emsgArgsNumber("type"))
	}

	key := args[0].Bulk()

	s.mutex.Lock()
	defer s.mutex.Unlock()
	entity, ok := s.store[key]
	if !ok {
		return new(protocol.Value).SetStr("none"), nil
	}
	switch entity.Type {
	case TypeString:
		return new(protocol.Value).SetStr("string"), nil
	case TypeList:
		return new(protocol.Value).SetStr("list"), nil
	case TypeStream:
		return new(protocol.Value).SetStr("stream"), nil
	}

	return nil, errors.New(emsgKeyType())
}

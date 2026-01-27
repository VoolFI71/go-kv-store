package storage

import (
	"hash/fnv"
	"sync"
	"unsafe"
)

const ShardCount = 16

type Shard struct {
	mu   sync.RWMutex
	data map[string]string
}

type Storage []*Shard

func New() Storage {
	s := make(Storage, ShardCount)
	for i := 0; i < ShardCount; i++ {
		s[i] = &Shard{
			data: make(map[string]string),
		}
	}
	return s
}

func (s Storage) GetShardIndex(key string) uint32 {
	hash := fnv.New32a()
	hash.Write(stringToBytesUnsafe(key))
	return hash.Sum32() % uint32(ShardCount)
}

// stringToBytesUnsafe преобразует string → []byte без копирования
// hash.Write только читает данные, не изменяет их
func stringToBytesUnsafe(s string) []byte {
	if len(s) == 0 {
		return nil
	}
	ptr := unsafe.StringData(s)
	return unsafe.Slice(ptr, len(s))
}

func (s Storage) Set(key, value string) {
	shard := s[s.GetShardIndex(key)]
	shard.mu.Lock()
	defer shard.mu.Unlock()
	shard.data[key] = value
}

func (s Storage) Get(key string) (string, bool) {
	shard := s[s.GetShardIndex(key)]
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	value, ok := shard.data[key]
	return value, ok
}

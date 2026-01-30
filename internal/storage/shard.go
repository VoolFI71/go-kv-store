package storage

import (
	"sync"
)

const ShardCount = 16
const shardMask = uint32(ShardCount - 1)

type Shard struct {
	mu   sync.RWMutex
	data map[string]string
	//_	[64]byte
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
	return fnv1a32String(key) & shardMask
}

func fnv1a32String(s string) uint32 {
	const (
		offset32 = 2166136261
		prime32  = 16777619
	)

	h := uint32(offset32)
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= prime32
	}
	return h
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

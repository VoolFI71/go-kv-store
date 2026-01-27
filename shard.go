package main

import (
	"hash/fnv"
	"sync"
)

const ShardCount = 16

type Shard struct {
	mu sync.RWMutex
	data map[string]string
}

type ShardStorage []*Shard

func NewShardStorage() ShardStorage{
	s := make(ShardStorage, ShardCount)
	for i:=0; i<ShardCount;i++{
		s[i] = &Shard{
			data: make(map[string]string),
		}
	}
	return s
}

func (s ShardStorage) GetShardIndex(key string) uint32{
	hash := fnv.New32a()
	hash.Write([]byte(key))
	return hash.Sum32() % uint32(ShardCount)
}

func (s ShardStorage) Set(key, value string) {
	shard := s[s.GetShardIndex(key)]
	shard.mu.Lock()
	defer shard.mu.Unlock()
	shard.data[key] = value
}

func (s ShardStorage) Get(key string) (string, bool) {
	shard := s[s.GetShardIndex(key)]
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	value, ok := shard.data[key]
	return value, ok
}
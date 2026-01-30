package storage

import (
	"errors"
	"strconv"
	"sync"
	"time"
)

const ShardCount = 16
const shardMask = uint32(ShardCount - 1)

type Shard struct {
	mu      sync.RWMutex
	data    map[string]string
	expires map[string]int64
}

type Storage []*Shard

var errValueNotInteger = errors.New("ERR value is not an integer or out of range")

func New() Storage {
	s := make(Storage, ShardCount)
	for i := 0; i < ShardCount; i++ {
		s[i] = &Shard{
			data:    make(map[string]string),
			expires: make(map[string]int64), //unix nano
		}
	}
	go s.startJanitor()
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
	delete(shard.expires, key)
}

func (s Storage) Get(key string) (string, bool) {
	shard := s[s.GetShardIndex(key)]
	shard.mu.RLock()
	value, ok := shard.data[key]
	if !ok {
		shard.mu.RUnlock()
		return "", false
	}
	expireAt, hasExpire := shard.expires[key]
	if !hasExpire || expireAt > time.Now().UnixNano() {
		shard.mu.RUnlock()
		return value, true
	}
	shard.mu.RUnlock()

	shard.mu.Lock()
	defer shard.mu.Unlock()
	value, ok = shard.data[key]
	if !ok {
		return "", false
	}
	expireAt, hasExpire = shard.expires[key]
	if hasExpire && expireAt <= time.Now().UnixNano() {
		delete(shard.data, key)
		delete(shard.expires, key)
		return "", false
	}
	return value, true
}

func (s Storage) Incr(key string) (int64, error) {
	shard := s[s.GetShardIndex(key)]
	shard.mu.Lock()
	defer shard.mu.Unlock()

	current := int64(0)
	if value, ok := shard.data[key]; ok {
		if expireAt, hasExpire := shard.expires[key]; hasExpire && expireAt <= time.Now().UnixNano() {
			delete(shard.data, key)
			delete(shard.expires, key)
		} else {
			parsed, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return 0, errValueNotInteger
			}
			current = parsed
		}
	}
	current++
	shard.data[key] = strconv.FormatInt(current, 10)
	return current, nil
}

func (s Storage) SetExpire(key string, seconds int64) bool {
	shard := s[s.GetShardIndex(key)]
	shard.mu.Lock()
	defer shard.mu.Unlock()
	if _, ok := shard.data[key]; !ok {
		return false
	}
	if seconds <= 0 {
		delete(shard.data, key)
		delete(shard.expires, key)
		return true
	}
	shard.expires[key] = time.Now().Add(time.Duration(seconds) * time.Second).UnixNano()
	return true
}

func (s Storage) cleanup(scanLimit int) {
	now := time.Now().UnixNano()
	for _, shard := range s {
		shard.mu.Lock()
		shard.cleanupLocked(now, scanLimit)
		shard.mu.Unlock()
	}
}

func (s Storage) startJanitor() {
	ticker := time.NewTicker(100 * time.Millisecond)
	for range ticker.C {
		s.cleanup(100)
	}
}

func (shard *Shard) cleanupLocked(now int64, scanLimit int) {
	scanned := 0
	for key, expireAt := range shard.expires {
		if expireAt <= now {
			delete(shard.expires, key)
			delete(shard.data, key)
		}
		scanned++
		if scanned >= scanLimit {
			return
		}
	}
}

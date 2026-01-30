package storage

import (
	"errors"
	"strconv"
	"sync"
	"time"
)

const ShardCount = 64
const shardMask = uint32(ShardCount - 1)

type Shard struct {
	mu      sync.RWMutex
	entries map[string]*entry
}

type entry struct {
	value    string
	expireAt int64
}

type Storage []*Shard

var errValueNotInteger = errors.New("ERR value is not an integer or out of range")
var entryPool = sync.Pool{New: func() any { return &entry{} }}

func New() Storage {
	s := make(Storage, ShardCount)
	for i := 0; i < ShardCount; i++ {
		s[i] = &Shard{
			entries: make(map[string]*entry),
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
	if ent, ok := shard.entries[key]; ok {
		ent.value = value
		ent.expireAt = 0
		shard.mu.Unlock()
		return
	}
	shard.entries[key] = getEntryFromPool(value)
	shard.mu.Unlock()
}

func (s Storage) Get(key string) (string, bool) {
	shard := s[s.GetShardIndex(key)]
	shard.mu.RLock()
	ent, ok := shard.entries[key]
	if !ok {
		shard.mu.RUnlock()
		return "", false
	}
	now := time.Now().UnixNano()
	if ent.expireAt == 0 || ent.expireAt > now {
		shard.mu.RUnlock()
		return ent.value, true
	}
	shard.mu.RUnlock()

	shard.mu.Lock()
	ent, ok = shard.entries[key]
	if !ok {
		shard.mu.Unlock()
		return "", false
	}
	if ent.expireAt != 0 && ent.expireAt <= now {
		deleteEntryLocked(shard, key, ent)
		shard.mu.Unlock()
		return "", false
	}
	value := ent.value
	shard.mu.Unlock()
	return value, true
}

func (s Storage) Incr(key string) (int64, error) {
	shard := s[s.GetShardIndex(key)]
	shard.mu.Lock()

	current := int64(0)
	now := time.Now().UnixNano()
	ent, ok := shard.entries[key]
	if ok && ent.expireAt != 0 && ent.expireAt <= now {
		deleteEntryLocked(shard, key, ent)
		ok = false
	}
	if ok {
		parsed, err := strconv.ParseInt(ent.value, 10, 64)
		if err != nil {
			shard.mu.Unlock()
			return 0, errValueNotInteger
		}
		current = parsed
	}
	current++
	if ok {
		ent.value = strconv.FormatInt(current, 10)
	} else {
		shard.entries[key] = getEntryFromPool(strconv.FormatInt(current, 10))
	}
	shard.mu.Unlock()
	return current, nil
}

func (s Storage) SetExpire(key string, seconds int64) bool {
	shard := s[s.GetShardIndex(key)]
	shard.mu.Lock()
	ent, ok := shard.entries[key]
	if !ok {
		shard.mu.Unlock()
		return false
	}
	if seconds <= 0 {
		deleteEntryLocked(shard, key, ent)
		shard.mu.Unlock()
		return true
	}
	ent.expireAt = time.Now().Add(time.Duration(seconds) * time.Second).UnixNano()
	shard.mu.Unlock()
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
	for key, ent := range shard.entries {
		if ent.expireAt != 0 && ent.expireAt <= now {
			deleteEntryLocked(shard, key, ent)
		}
		scanned++
		if scanned >= scanLimit {
			return
		}
	}
}

func getEntryFromPool(value string) *entry {
	ent := entryPool.Get().(*entry)
	ent.value = value
	ent.expireAt = 0
	return ent
}

func deleteEntryLocked(shard *Shard, key string, ent *entry) {
	delete(shard.entries, key)
	ent.value = ""
	ent.expireAt = 0
	entryPool.Put(ent)
}

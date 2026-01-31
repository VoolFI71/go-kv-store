package storage

import (
	"errors"
	"strconv"
	"sync"
	"time"
	"unsafe"
)

const ShardCount = 64
const shardMask uint64 = ShardCount - 1

type Shard struct {
	mu      sync.RWMutex
	entries map[uint64]*entry
}

type entry struct {
	key      string
	value    string
	expireAt int64
	next     *entry
}

type Storage []*Shard

var errValueNotInteger = errors.New("ERR value is not an integer or out of range")
var entryPool = sync.Pool{New: func() any { return &entry{} }}

func New() Storage {
	const preallocPerShard = 5_000_000 / ShardCount
	s := make(Storage, ShardCount)
	for i := 0; i < ShardCount; i++ {
		s[i] = &Shard{
			entries: make(map[uint64]*entry, preallocPerShard),
		}
	}
	go s.startJanitor()
	return s
}

func (s Storage) shardForHash(hash uint64) *Shard {
	return s[int(hash&shardMask)]
}

func (s Storage) SetHashed(hash uint64, key, value string) {
	s.SetHashedWithExpireAt(hash, key, value, 0)
}

func (s Storage) SetHashedWithTTLSeconds(hash uint64, key, value string, ttlSeconds int64) {
	if ttlSeconds <= 0 {
		s.SetHashedWithExpireAt(hash, key, value, 0)
		return
	}
	s.SetHashedWithExpireAt(hash, key, value, time.Now().Add(time.Duration(ttlSeconds)*time.Second).UnixNano())
}

func (s Storage) SetHashedWithExpireAt(hash uint64, key, value string, expireAt int64) {
	shard := s.shardForHash(hash)
	shard.mu.Lock()
	prev, ent := shard.findEntry(hash, key)
	if ent != nil {
		ent.value = cloneString(value)
		ent.expireAt = expireAt
		shard.mu.Unlock()
		return
	}
	_ = prev
	newEnt := getEntryFromPool(key, value)
	newEnt.expireAt = expireAt
	newEnt.next = shard.entries[hash]
	shard.entries[hash] = newEnt
	shard.mu.Unlock()
}

func (s Storage) GetHashed(hash uint64, key string) (string, bool) {
	shard := s.shardForHash(hash)
	now := time.Now().UnixNano()

	shard.mu.RLock()
	ent := shard.findEntryRead(hash, key)
	if ent == nil {
		shard.mu.RUnlock()
		return "", false
	}
	if ent.expireAt == 0 || ent.expireAt > now {
		value := ent.value
		shard.mu.RUnlock()
		return value, true
	}
	shard.mu.RUnlock()

	shard.mu.Lock()
	prev, ent := shard.findEntry(hash, key)
	if ent == nil {
		shard.mu.Unlock()
		return "", false
	}
	if ent.expireAt != 0 && ent.expireAt <= now {
		deleteEntryLocked(shard, hash, prev, ent)
		shard.mu.Unlock()
		return "", false
	}
	value := ent.value
	shard.mu.Unlock()
	return value, true
}

func (s Storage) IncrHashed(hash uint64, key string) (int64, error) {
	shard := s.shardForHash(hash)
	shard.mu.Lock()

	current := int64(0)
	now := time.Now().UnixNano()
	prev, ent := shard.findEntry(hash, key)
	if ent != nil && ent.expireAt != 0 && ent.expireAt <= now {
		deleteEntryLocked(shard, hash, prev, ent)
		ent = nil
	}
	if ent != nil {
		parsed, err := strconv.ParseInt(ent.value, 10, 64)
		if err != nil {
			shard.mu.Unlock()
			return 0, errValueNotInteger
		}
		current = parsed
	}
	current++
	if ent != nil {
		ent.value = strconv.FormatInt(current, 10)
	} else {
		newEnt := getEntryFromPool(key, strconv.FormatInt(current, 10))
		newEnt.next = shard.entries[hash]
		shard.entries[hash] = newEnt
	}
	shard.mu.Unlock()
	return current, nil
}

func (s Storage) SetExpireHashed(hash uint64, key string, seconds int64) bool {
	shard := s.shardForHash(hash)
	shard.mu.Lock()
	prev, ent := shard.findEntry(hash, key)
	if ent == nil {
		shard.mu.Unlock()
		return false
	}
	if seconds <= 0 {
		deleteEntryLocked(shard, hash, prev, ent)
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
	maxScan := scanLimit
	removed := 0
	if scanLimit < 1000 {
		maxScan = scanLimit * 10
	}

	scanned := 0
	for hash, head := range shard.entries {
		prev := (*entry)(nil)
		cur := head
		for cur != nil {
			next := cur.next
			if cur.expireAt != 0 && cur.expireAt <= now {
				deleteEntryLocked(shard, hash, prev, cur)
				removed++
			} else {
				prev = cur
			}
			scanned++
			if scanned >= scanLimit && removed == 0 {
				return
			}
			if scanned >= maxScan {
				return
			}
			cur = next
		}
	}
}

func (shard *Shard) findEntry(hash uint64, key string) (*entry, *entry) {
	cur := shard.entries[hash]
	var prev *entry
	for cur != nil {
		if cur.key == key {
			return prev, cur
		}
		prev = cur
		cur = cur.next
	}
	return nil, nil
}

func (shard *Shard) findEntryRead(hash uint64, key string) *entry {
	cur := shard.entries[hash]
	for cur != nil {
		if cur.key == key {
			return cur
		}
		cur = cur.next
	}
	return nil
}

func getEntryFromPool(key, value string) *entry {
	ent := entryPool.Get().(*entry)
	ent.key = cloneString(key)
	ent.value = cloneString(value)
	ent.expireAt = 0
	ent.next = nil
	return ent
}

func cloneString(s string) string {
	b := make([]byte, len(s))
	copy(b, s)
	return unsafe.String(unsafe.SliceData(b), len(b))
}

func deleteEntryLocked(shard *Shard, hash uint64, prev, ent *entry) {
	if prev == nil {
		if ent.next == nil {
			delete(shard.entries, hash)
		} else {
			shard.entries[hash] = ent.next
		}
	} else {
		prev.next = ent.next
	}
	ent.key = ""
	ent.value = ""
	ent.expireAt = 0
	ent.next = nil
	entryPool.Put(ent)
}

package locksmith

import "time"

// BatchItem is a single item for batch write operations.
type BatchItem[K Key, V any] struct {
	Key   K
	Value V
}

// BatchTTLItem is a single item with TTL for batch write operations.
type BatchTTLItem[K Key, V any] struct {
	Key   K
	Value V
	TTL   time.Duration
}

// Set stores value without TTL.
func (s *Store[K, V]) Set(key K, value V) error {
	return s.setEntry(key, entry[V]{
		value: value,
		size:  s.entrySize(key, value),
	}, false)
}

// SetBatch stores multiple key-value pairs without TTL in one atomic operation.
func (s *Store[K, V]) SetBatch(items []BatchItem[K, V]) error {
	if len(items) == 0 {
		return nil
	}
	entries := make(map[K]entry[V], len(items))
	for _, item := range items {
		entries[item.Key] = entry[V]{
			value: item.Value,
			size:  s.entrySize(item.Key, item.Value),
		}
	}
	return s.setEntriesBatch(entries, false)
}

// SetWithTTL stores value with TTL.
// ttl must be greater than zero.
func (s *Store[K, V]) SetWithTTL(key K, value V, ttl time.Duration) error {
	if ttl <= 0 {
		return ErrInvalidTTL
	}

	return s.setEntry(key, entry[V]{
		value:     value,
		expiresAt: time.Now().Add(ttl).UnixNano(),
		size:      s.entrySize(key, value),
	}, false)
}

// SetBatchWithTTL stores multiple key-value pairs with TTL in one atomic operation.
func (s *Store[K, V]) SetBatchWithTTL(items []BatchTTLItem[K, V]) error {
	if len(items) == 0 {
		return nil
	}
	now := time.Now()
	entries := make(map[K]entry[V], len(items))
	for _, item := range items {
		if item.TTL <= 0 {
			return ErrInvalidTTL
		}
		entries[item.Key] = entry[V]{
			value:     item.Value,
			expiresAt: now.Add(item.TTL).UnixNano(),
			size:      s.entrySize(item.Key, item.Value),
		}
	}
	return s.setEntriesBatch(entries, false)
}

// Load stores value without TTL and ignores dump mode.
// Use this to preload cache data while dump mode is active.
func (s *Store[K, V]) Load(key K, value V) error {
	return s.setEntry(key, entry[V]{
		value: value,
		size:  s.entrySize(key, value),
	}, true)
}

// LoadBatch stores multiple key-value pairs without TTL and ignores dump mode.
func (s *Store[K, V]) LoadBatch(items []BatchItem[K, V]) error {
	if len(items) == 0 {
		return nil
	}
	entries := make(map[K]entry[V], len(items))
	for _, item := range items {
		entries[item.Key] = entry[V]{
			value: item.Value,
			size:  s.entrySize(item.Key, item.Value),
		}
	}
	return s.setEntriesBatch(entries, true)
}

// LoadWithTTL stores value with TTL and ignores dump mode.
// Use this to preload cache data while dump mode is active.
func (s *Store[K, V]) LoadWithTTL(key K, value V, ttl time.Duration) error {
	if ttl <= 0 {
		return ErrInvalidTTL
	}

	return s.setEntry(key, entry[V]{
		value:     value,
		expiresAt: time.Now().Add(ttl).UnixNano(),
		size:      s.entrySize(key, value),
	}, true)
}

// LoadBatchWithTTL stores multiple key-value pairs with TTL and ignores dump mode.
func (s *Store[K, V]) LoadBatchWithTTL(items []BatchTTLItem[K, V]) error {
	if len(items) == 0 {
		return nil
	}
	now := time.Now()
	entries := make(map[K]entry[V], len(items))
	for _, item := range items {
		if item.TTL <= 0 {
			return ErrInvalidTTL
		}
		entries[item.Key] = entry[V]{
			value:     item.Value,
			expiresAt: now.Add(item.TTL).UnixNano(),
			size:      s.entrySize(item.Key, item.Value),
		}
	}
	return s.setEntriesBatch(entries, true)
}

// Get returns value by key.
// If record expired, it is removed and ok=false is returned.
func (s *Store[K, V]) Get(key K) (value V, ok bool, err error) {
	nowNano := time.Now().UnixNano()

	var keys int
	var volume int
	var keysChanged bool
	var volumeChanged bool
	var invalidated *InvalidationEvent

	s.mu.Lock()
	if s.dumpMode.Load() {
		s.mu.Unlock()
		return value, false, ErrCacheNotReady
	}

	ent, exists := s.data[key]
	if !exists {
		s.mu.Unlock()
		return value, false, nil
	}

	if ent.expired(nowNano) {
		delete(s.data, key)
		delete(s.ttl, key)
		s.volume -= ent.size

		keys = len(s.data)
		volume = s.volume
		keysChanged = true
		volumeChanged = true
		s.usageMask = calcUsageMask(s.volume, s.maxVolume)
		s.updateMetaLocked()
		ev := makeInvalidationEvent(key, ent.expiresAt, InvalidationSourceGet)
		invalidated = &ev
		s.mu.Unlock()

		s.notifyChanges(keysChanged, keys, volumeChanged, volume)
		s.notifyTTLInvalidated(*invalidated)
		return value, false, nil
	}

	s.mu.Unlock()
	return ent.value, true, nil
}

// InvalidationEvents returns a channel with TTL invalidation events.
// It is nil when Options.InvalidationChannelBuffer <= 0.
func (s *Store[K, V]) InvalidationEvents() <-chan InvalidationEvent {
	return s.invCh
}

// Delete removes a key if present.
func (s *Store[K, V]) Delete(key K) (bool, error) {
	var deleted bool
	var keys int
	var volume int

	s.mu.Lock()
	if s.dumpMode.Load() {
		s.mu.Unlock()
		return false, ErrCacheNotReady
	}

	ent, ok := s.data[key]
	if ok {
		delete(s.data, key)
		delete(s.ttl, key)
		s.volume -= ent.size
		s.usageMask = calcUsageMask(s.volume, s.maxVolume)
		s.updateMetaLocked()
		deleted = true
		keys = len(s.data)
		volume = s.volume
	}
	s.mu.Unlock()

	if deleted {
		s.notifyChanges(true, keys, true, volume)
	}

	return deleted, nil
}

// Stats returns current store statistics.
func (s *Store[K, V]) Stats() Stats {
	s.mu.RLock()
	meta := s.meta
	stats := Stats{
		Keys:         meta.Objects,
		Volume:       meta.Volume,
		MaxVolume:    meta.MaxVolume,
		UsagePercent: meta.UsagePercent,
		DumpModeOn:   s.dumpMode.Load(),
	}
	s.mu.RUnlock()
	return stats
}

// Meta returns cache accounting metadata.
func (s *Store[K, V]) Meta() Meta {
	s.mu.RLock()
	meta := s.meta
	s.mu.RUnlock()
	return meta
}

// SetDumpMode switches dump mode on/off.
// When enabled, Set/Get/Delete return ErrCacheNotReady.
// Use Load/LoadWithTTL to preload data during dump mode.
func (s *Store[K, V]) SetDumpMode(on bool) {
	s.dumpMode.Store(on)
}

// DumpModeOn reports whether dump mode is active.
func (s *Store[K, V]) DumpModeOn() bool {
	return s.dumpMode.Load()
}

package locksmith

import (
	"sync"
	"sync/atomic"
	"time"
)

const DefaultGCInterval = time.Second

const (
	mask25 uint8 = 1 << iota
	mask50
	mask75
	mask90
)

// Store is a concurrent key-value storage with optional per-record TTL.
type Store[K Key, V any] struct {
	mu     sync.RWMutex
	data   map[K]entry[V]
	ttl    map[K]int64 // mirrors keys that have TTL, for cheaper GC scans
	volume int

	gcInterval time.Duration
	hooks      Hooks
	estimator  SizeEstimator[K, V]
	maxVolume  int
	meta       Meta
	usageMask  uint8
	dumpMode   atomic.Bool
	invCh      chan InvalidationEvent
	invOnce    sync.Once

	stopCh chan struct{}
	doneCh chan struct{}
}

// New creates a store and starts background GC worker.
func New[K Key, V any](opts Options[K, V]) *Store[K, V] {
	if opts.GCInterval <= 0 {
		opts.GCInterval = DefaultGCInterval
	}
	if opts.SizeEstimator == nil {
		opts.SizeEstimator = autoSizeEstimator[K, V]
	}

	s := &Store[K, V]{
		data:       make(map[K]entry[V], 1024),
		ttl:        make(map[K]int64, 256),
		gcInterval: opts.GCInterval,
		hooks:      opts.Hooks,
		estimator:  opts.SizeEstimator,
		maxVolume:  opts.MaxVolume,
		meta: Meta{
			Objects:      0,
			Volume:       0,
			MaxVolume:    opts.MaxVolume,
			UsagePercent: 0,
		},
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}
	if opts.InvalidationChannelBuffer > 0 {
		s.invCh = make(chan InvalidationEvent, opts.InvalidationChannelBuffer)
	}
	s.dumpMode.Store(opts.DumpModeOn)

	go s.gcLoop()
	return s
}

// Close stops background GC worker.
func (s *Store[K, V]) Close() {
	select {
	case <-s.doneCh:
		s.closeInvalidationChannel()
		return
	case <-s.stopCh:
		<-s.doneCh
		s.closeInvalidationChannel()
		return
	default:
		close(s.stopCh)
		<-s.doneCh
		s.closeInvalidationChannel()
	}
}

func (s *Store[K, V]) closeInvalidationChannel() {
	if s.invCh == nil {
		return
	}
	s.invOnce.Do(func() {
		close(s.invCh)
	})
}

func (s *Store[K, V]) setEntry(key K, ent entry[V], ignoreDumpMode bool) error {
	var keys int
	var volume int
	var keysChanged bool
	var volumeChanged bool
	var usageEnterMask uint8

	s.mu.Lock()
	if !ignoreDumpMode && s.dumpMode.Load() {
		s.mu.Unlock()
		return ErrCacheNotReady
	}

	prev, exists := s.data[key]

	nextVolume := s.volume + ent.size
	if exists {
		nextVolume -= prev.size
	}
	if s.maxVolume > 0 && nextVolume > s.maxVolume {
		s.mu.Unlock()
		return ErrCacheFull
	}

	prevMask := s.usageMask
	s.data[key] = ent

	if ent.expiresAt == 0 {
		delete(s.ttl, key)
	} else {
		s.ttl[key] = ent.expiresAt
	}

	if exists {
		if prev.size != ent.size {
			s.volume = nextVolume
			volumeChanged = true
		}
	} else {
		s.volume = nextVolume
		keysChanged = true
		volumeChanged = true
	}

	if keysChanged {
		keys = len(s.data)
	}
	if volumeChanged {
		volume = s.volume
		newMask := calcUsageMask(volume, s.maxVolume)
		usageEnterMask = newMask &^ prevMask
		s.usageMask = newMask
	}
	if keysChanged || volumeChanged {
		s.updateMetaLocked()
	}
	s.mu.Unlock()

	s.notifyChanges(keysChanged, keys, volumeChanged, volume)
	s.notifyFillHooks(usageEnterMask, volume)
	return nil
}

func (s *Store[K, V]) setEntriesBatch(entries map[K]entry[V], ignoreDumpMode bool) error {
	if len(entries) == 0 {
		return nil
	}

	var keys int
	var volume int
	var keysChanged bool
	var volumeChanged bool
	var usageEnterMask uint8

	s.mu.Lock()
	if !ignoreDumpMode && s.dumpMode.Load() {
		s.mu.Unlock()
		return ErrCacheNotReady
	}

	nextVolume := s.volume
	for key, ent := range entries {
		prev, exists := s.data[key]
		if exists {
			if prev.size != ent.size {
				nextVolume += ent.size - prev.size
				volumeChanged = true
			}
		} else {
			nextVolume += ent.size
			keysChanged = true
			volumeChanged = true
		}
	}

	if s.maxVolume > 0 && nextVolume > s.maxVolume {
		s.mu.Unlock()
		return ErrCacheFull
	}

	prevMask := s.usageMask
	for key, ent := range entries {
		s.data[key] = ent
		if ent.expiresAt == 0 {
			delete(s.ttl, key)
		} else {
			s.ttl[key] = ent.expiresAt
		}
	}

	if volumeChanged {
		s.volume = nextVolume
		volume = s.volume
		newMask := calcUsageMask(volume, s.maxVolume)
		usageEnterMask = newMask &^ prevMask
		s.usageMask = newMask
	}
	if keysChanged {
		keys = len(s.data)
	}
	if keysChanged || volumeChanged {
		s.updateMetaLocked()
	}
	s.mu.Unlock()

	s.notifyChanges(keysChanged, keys, volumeChanged, volume)
	s.notifyFillHooks(usageEnterMask, volume)
	return nil
}

func (s *Store[K, V]) entrySize(key K, value V) int {
	if s.estimator == nil {
		return 1
	}
	size := s.estimator(key, value)
	if size < 0 {
		return 0
	}
	return size
}

func (s *Store[K, V]) updateMetaLocked() {
	usagePercent := 0
	if s.maxVolume > 0 {
		usagePercent = (s.volume * 100) / s.maxVolume
	}
	s.meta = Meta{
		Objects:      len(s.data),
		Volume:       s.volume,
		MaxVolume:    s.maxVolume,
		UsagePercent: usagePercent,
	}
}

func calcUsageMask(volume int, maxVolume int) uint8 {
	if maxVolume <= 0 || volume <= 0 {
		return 0
	}

	var mask uint8
	usage := (volume * 100) / maxVolume
	if usage >= 25 {
		mask |= mask25
	}
	if usage >= 50 {
		mask |= mask50
	}
	if usage >= 75 {
		mask |= mask75
	}
	if usage >= 90 {
		mask |= mask90
	}
	return mask
}

package locksmith

import "time"

// Key is a supported key type for the store.
// Only int and string keys are allowed.
type Key interface {
	~int | ~string
}

type entry[V any] struct {
	value     V
	expiresAt int64 // unix nano; 0 means no TTL
	size      int
}

func (e entry[V]) expired(nowNano int64) bool {
	return e.expiresAt != 0 && nowNano >= e.expiresAt
}

// Stats contains runtime information about the store.
type Stats struct {
	Keys         int
	Volume       int
	MaxVolume    int
	UsagePercent int
	DumpModeOn   bool
}

// UsageEvent is emitted for fill-threshold hooks.
type UsageEvent struct {
	Percent   int
	Volume    int
	MaxVolume int
}

type InvalidationSource string

const (
	InvalidationSourceGet InvalidationSource = "get"
	InvalidationSourceGC  InvalidationSource = "gc"
)

// InvalidationEvent is emitted when TTL entry is invalidated and removed.
type InvalidationEvent struct {
	Key           any
	ExpiredAt     time.Time
	InvalidatedAt time.Time
	Source        InvalidationSource
}

// Meta is an internal cache accounting snapshot.
// It is updated during cache mutations.
type Meta struct {
	Objects      int
	Volume       int
	MaxVolume    int
	UsagePercent int
}

// Hooks are optional callbacks for observing store events.
type Hooks struct {
	// OnKeysChanged is called whenever key count changes.
	OnKeysChanged func(keys int)

	// OnVolumeChanged is called whenever current cache volume changes.
	OnVolumeChanged func(volume int)

	// OnGCStart is called whenever a GC cycle starts.
	OnGCStart func()

	// OnFill25 is called once when usage crosses 25% upward.
	OnFill25 func(UsageEvent)
	// OnFill50 is called once when usage crosses 50% upward.
	OnFill50 func(UsageEvent)
	// OnFill75 is called once when usage crosses 75% upward.
	OnFill75 func(UsageEvent)
	// OnFill90 is called once when usage crosses 90% upward.
	OnFill90 func(UsageEvent)

	// OnTTLInvalidated is called when an expired entry is removed.
	OnTTLInvalidated func(InvalidationEvent)
}

// SizeEstimator computes per-entry volume.
type SizeEstimator[K Key, V any] func(key K, value V) int

// Options configure store behavior.
type Options[K Key, V any] struct {
	GCInterval    time.Duration
	Hooks         Hooks
	SizeEstimator SizeEstimator[K, V]
	MaxVolume     int
	DumpModeOn    bool
	// InvalidationChannelBuffer enables invalidation events channel when > 0.
	InvalidationChannelBuffer int
}

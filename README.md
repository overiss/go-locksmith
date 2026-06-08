# go-locksmith

`go-locksmith` is a lightweight in-memory key-value store for Go with:

- optional per-record TTL
- background cleanup of expired records
- hooks for cache observability (keys count, volume, GC starts, fill thresholds)
- optional dump mode (cache warm-up gate)
- optional max cache volume guard

## Features

- Generic store with restricted key types: `int` or `string`
- Optional TTL on a per-entry basis
- Background GC loop for expired entries
- Concurrent-safe operations
- Runtime stats:
  - number of keys
  - current cache volume
  - usage percent and dump mode status
- Optional dump mode for startup warm-up
- Optional max cache volume limit
- Optional hooks:
  - `OnKeysChanged`
  - `OnVolumeChanged`
  - `OnGCStart`
  - `OnFill25`, `OnFill50`, `OnFill75`, `OnFill90`
  - `OnTTLInvalidated`

## Installation

```bash
go get github.com/overiss/go-locksmith
```

## Quick Start

```go
package main

import (
	"fmt"
	"time"

	"github.com/overiss/go-locksmith"
)

func main() {
	store := locksmith.New[string, string](locksmith.Options[string, string]{
		GCInterval: 500 * time.Millisecond,
	})
	defer store.Close()

	_ = store.Set("name", "alice")
	_ = store.SetWithTTL("session", "token-123", 2*time.Second)

	if v, ok, _ := store.Get("name"); ok {
		fmt.Println("name:", v)
	}

	time.Sleep(3 * time.Second)
	if _, ok, _ := store.Get("session"); !ok {
		fmt.Println("session expired")
	}
}
```

## API Overview

### Store creation

```go
store := locksmith.New[K, V](locksmith.Options[K, V]{ ... })
defer store.Close()
```

### Set without TTL

```go
store.Set(key, value)
```

`Set` returns `error` (`ErrCacheNotReady`, `ErrCacheFull`).

### Batch set without TTL

```go
err := store.SetBatch([]locksmith.BatchItem[string, string]{
	{Key: "k1", Value: "v1"},
	{Key: "k2", Value: "v2"},
})
```

`SetBatch` is atomic (all-or-nothing) and returns `error`.

### Set with TTL

```go
store.SetWithTTL(key, value, 5*time.Second)
```

`SetWithTTL` returns `error` (`ErrInvalidTTL`, `ErrCacheNotReady`, `ErrCacheFull`).

### Batch set with TTL

```go
err := store.SetBatchWithTTL([]locksmith.BatchTTLItem[string, string]{
	{Key: "session:1", Value: "token-a", TTL: 30 * time.Second},
	{Key: "session:2", Value: "token-b", TTL: time.Minute},
})
```

`SetBatchWithTTL` is atomic (all-or-nothing) and returns `error`.

### Read

```go
value, ok, err := store.Get(key)
```

If a key is expired at read time, it is removed and `ok == false` is returned.
If dump mode is active, returns `ErrCacheNotReady`.

### Delete

```go
deleted, err := store.Delete(key)
```

If dump mode is active, returns `ErrCacheNotReady`.

### Stats

```go
stats := store.Stats()
fmt.Println(stats.Keys, stats.Volume, stats.UsagePercent, stats.DumpModeOn)
```

`Stats` fields:

- `Keys` — current number of entries
- `Volume` — current cache volume (based on `SizeEstimator`)
- `MaxVolume` — configured max volume (`0` means unlimited)
- `UsagePercent` — `Volume * 100 / MaxVolume` (`0` when unlimited)
- `DumpModeOn` — current dump mode flag

## New Parameters

```go
type Options[K Key, V any] struct {
	GCInterval    time.Duration
	Hooks         Hooks
	SizeEstimator SizeEstimator[K, V]
	MaxVolume     int
	DumpModeOn    bool
	InvalidationChannelBuffer int
}
```

- `GCInterval`: background GC interval. If zero or negative, default is used.
- `Hooks`: optional callbacks for observability.
- `SizeEstimator`: optional function to calculate per-entry volume.
  - If nil, built-in automatic estimator is used.
  - If it returns a negative value, it is clamped to `0`.
- `MaxVolume`: max allowed volume (`0` means unlimited).
- `DumpModeOn`: initial dump mode state.
- `InvalidationChannelBuffer`: enables buffered TTL invalidation events channel when `> 0`.

## Hooks

```go
type Hooks struct {
	OnKeysChanged   func(keys int)
	OnVolumeChanged func(volume int)
	OnGCStart       func()
	OnFill25        func(event UsageEvent)
	OnFill50        func(event UsageEvent)
	OnFill75        func(event UsageEvent)
	OnFill90        func(event UsageEvent)
	OnTTLInvalidated func(event InvalidationEvent)
}
```

- `OnKeysChanged` is called when number of keys changes.
- `OnVolumeChanged` is called when total volume changes.
- `OnGCStart` is called at the start of every GC cycle.
- Fill hooks are called once per upward threshold crossing.
- `OnTTLInvalidated` is called when expired entry is removed (via `Get` or background `GC`).

```go
type UsageEvent struct {
	Percent   int
	Volume    int
	MaxVolume int
}
```

```go
type InvalidationEvent struct {
	Key           any
	ExpiredAt     time.Time
	InvalidatedAt time.Time
	Source        InvalidationSource // "get" or "gc"
}
```

### Invalidation events channel

If you prefer channel-based consumption instead of a hook:

```go
store := locksmith.New[string, string](locksmith.Options[string, string]{
	InvalidationChannelBuffer: 128,
})

events := store.InvalidationEvents() // <-chan InvalidationEvent (nil if disabled)
```

## Dump mode

When dump mode is on:

- `Set`, `SetBatch`, `SetWithTTL`, `SetBatchWithTTL`, `Get`, `Delete` return `ErrCacheNotReady`.
- `Load`, `LoadBatch`, `LoadWithTTL`, `LoadBatchWithTTL` still work (for startup preloading).

```go
store := locksmith.New[string, string](locksmith.Options[string, string]{
	DumpModeOn: true,
})

_ = store.Load("prefill-key", "value")
_ = store.LoadBatch([]locksmith.BatchItem[string, string]{
	{Key: "k1", Value: "v1"},
	{Key: "k2", Value: "v2"},
})
store.SetDumpMode(false)
```

## Example with custom volume estimator and hooks

```go
store := locksmith.New[int, string](
	locksmith.Options[int, string]{
		GCInterval: 100 * time.Millisecond,
		MaxVolume:  1_000_000,
		Hooks: locksmith.Hooks{
		OnKeysChanged: func(keys int) {
			fmt.Println("keys:", keys)
		},
		OnVolumeChanged: func(volume int) {
			fmt.Println("volume:", volume)
		},
		OnGCStart: func() {
			fmt.Println("gc cycle started")
		},
		OnFill90: func(ev locksmith.UsageEvent) {
			fmt.Printf("cache at %d%% (%d/%d)\n", ev.Percent, ev.Volume, ev.MaxVolume)
		},
	},
		SizeEstimator: func(_ int, value string) int {
			return len(value)
		},
	},
)
defer store.Close()
```

## Example: struct as value

```go
package main

import (
	"time"

	"github.com/overiss/go-locksmith"
)

type UserProfile struct {
	ID        int
	Name      string
	Email     string
	UpdatedAt time.Time
}

func main() {
	store := locksmith.New[string, UserProfile](locksmith.Options[string, UserProfile]{
		// Example estimator for struct values.
		SizeEstimator: func(_ string, p UserProfile) int {
			return len(p.Name) + len(p.Email) + 16
		},
	})
	defer store.Close()

	_ = store.Set("user:42", UserProfile{
		ID:        42,
		Name:      "Alice",
		Email:     "alice@company.dev",
		UpdatedAt: time.Now(),
	})

	_, _, _ = store.Get("user:42")
}
```

If you do not want to write a custom estimator manually, just omit it:

```go
store := locksmith.New[string, UserProfile](locksmith.Options[string, UserProfile]{
	MaxVolume:         1_000_000,
})
```

Built-in estimator is enabled by default when `SizeEstimator` is not provided.

## Internal meta accounting

Cache keeps internal metadata and updates it during each mutation ("in-flight"):

```go
type Meta struct {
	Objects      int
	Volume       int
	MaxVolume    int
	UsagePercent int
}
```

Read metadata snapshot:

```go
meta := store.Meta()
```

## Notes

- Always call `Close()` to stop the background GC goroutine.
- TTL is optional; entries written with `Set` do not expire automatically.
- This store is in-memory only (no persistence).
- For high write rates with strict limits, tune `SizeEstimator` for stable and fast calculations.

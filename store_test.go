package locksmith

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func TestStoreWithoutTTL(t *testing.T) {
	store := New[string, string](Options[string, string]{
		GCInterval: 200 * time.Millisecond,
	})
	defer store.Close()

	if err := store.Set("user", "alice"); err != nil {
		t.Fatalf("unexpected set error: %v", err)
	}

	v, ok, err := store.Get("user")
	if err != nil {
		t.Fatalf("unexpected get error: %v", err)
	}
	if !ok {
		t.Fatalf("expected key to exist")
	}
	if v != "alice" {
		t.Fatalf("unexpected value: %q", v)
	}

	stats := store.Stats()
	if stats.Keys != 1 {
		t.Fatalf("unexpected keys: %d", stats.Keys)
	}
	if stats.Volume <= 1 {
		t.Fatalf("expected auto estimated volume > 1, got: %d", stats.Volume)
	}
}

func TestStoreWithIntKeysAndTTLGC(t *testing.T) {
	var (
		mu         sync.Mutex
		gcStarted  int
		lastKeys   int
		lastVolume int
		keysEvents int
		volEvents  int
	)

	store := New[int, string](Options[int, string]{
		GCInterval: 10 * time.Millisecond,
		Hooks: Hooks{
			OnGCStart: func() {
				mu.Lock()
				gcStarted++
				mu.Unlock()
			},
			OnKeysChanged: func(keys int) {
				mu.Lock()
				lastKeys = keys
				keysEvents++
				mu.Unlock()
			},
			OnVolumeChanged: func(volume int) {
				mu.Lock()
				lastVolume = volume
				volEvents++
				mu.Unlock()
			},
		},
		SizeEstimator: func(_ int, value string) int {
			return len(value)
		},
	})
	defer store.Close()

	if err := store.SetWithTTL(1, "abc", 25*time.Millisecond); err != nil {
		t.Fatalf("unexpected set with ttl error: %v", err)
	}
	if err := store.Set(2, "hello"); err != nil {
		t.Fatalf("unexpected set error: %v", err)
	}

	waitUntil(t, 500*time.Millisecond, func() bool {
		stats := store.Stats()
		return stats.Keys == 1 && stats.Volume == 5
	})

	_, ok, err := store.Get(1)
	if err != nil {
		t.Fatalf("unexpected get error: %v", err)
	}
	if ok {
		t.Fatalf("expected ttl key to be expired and removed")
	}

	v, ok, err := store.Get(2)
	if err != nil {
		t.Fatalf("unexpected get error: %v", err)
	}
	if !ok || v != "hello" {
		t.Fatalf("expected persistent key to remain")
	}

	mu.Lock()
	defer mu.Unlock()

	if gcStarted == 0 {
		t.Fatalf("expected gc start hook to be called")
	}
	if lastKeys != 1 {
		t.Fatalf("unexpected keys hook value: %d", lastKeys)
	}
	if lastVolume != 5 {
		t.Fatalf("unexpected volume hook value: %d", lastVolume)
	}
	if keysEvents == 0 || volEvents == 0 {
		t.Fatalf("expected keys/volume hooks to be called")
	}
}

func TestDeleteAndReplaceUpdatesVolume(t *testing.T) {
	store := New[string, string](Options[string, string]{
		GCInterval: time.Second,
		SizeEstimator: func(_ string, value string) int {
			return len(value)
		},
	})
	defer store.Close()

	if err := store.Set("k", "a"); err != nil {
		t.Fatalf("unexpected set error: %v", err)
	}
	if s := store.Stats(); s.Volume != 1 || s.Keys != 1 {
		t.Fatalf("unexpected stats after first set: %+v", s)
	}

	if err := store.Set("k", "abcdef"); err != nil {
		t.Fatalf("unexpected set error: %v", err)
	}
	if s := store.Stats(); s.Volume != 6 || s.Keys != 1 {
		t.Fatalf("unexpected stats after replace: %+v", s)
	}

	deleted, err := store.Delete("k")
	if err != nil {
		t.Fatalf("unexpected delete error: %v", err)
	}
	if !deleted {
		t.Fatalf("expected delete to return true")
	}
	if s := store.Stats(); s.Volume != 0 || s.Keys != 0 {
		t.Fatalf("unexpected stats after delete: %+v", s)
	}
}

func TestDumpModeRejectsRegularOpsAndAllowsLoad(t *testing.T) {
	store := New[string, string](Options[string, string]{
		DumpModeOn: true,
	})
	defer store.Close()

	if err := store.Set("k", "v"); !errors.Is(err, ErrCacheNotReady) {
		t.Fatalf("expected ErrCacheNotReady from Set, got %v", err)
	}
	if _, _, err := store.Get("k"); !errors.Is(err, ErrCacheNotReady) {
		t.Fatalf("expected ErrCacheNotReady from Get, got %v", err)
	}
	if _, err := store.Delete("k"); !errors.Is(err, ErrCacheNotReady) {
		t.Fatalf("expected ErrCacheNotReady from Delete, got %v", err)
	}

	if err := store.Load("k", "v"); err != nil {
		t.Fatalf("unexpected load error: %v", err)
	}
	store.SetDumpMode(false)

	v, ok, err := store.Get("k")
	if err != nil {
		t.Fatalf("unexpected get error after dump mode off: %v", err)
	}
	if !ok || v != "v" {
		t.Fatalf("unexpected read result after dump mode off: ok=%v value=%q", ok, v)
	}
}

func TestMaxVolumeAndFillHooks(t *testing.T) {
	var (
		mu     sync.Mutex
		events []int
	)

	store := New[string, string](Options[string, string]{
		MaxVolume: 100,
		SizeEstimator: func(_ string, value string) int {
			return len(value)
		},
		Hooks: Hooks{
			OnFill25: func(ev UsageEvent) {
				mu.Lock()
				events = append(events, ev.Percent)
				mu.Unlock()
			},
			OnFill50: func(ev UsageEvent) {
				mu.Lock()
				events = append(events, ev.Percent)
				mu.Unlock()
			},
			OnFill75: func(ev UsageEvent) {
				mu.Lock()
				events = append(events, ev.Percent)
				mu.Unlock()
			},
			OnFill90: func(ev UsageEvent) {
				mu.Lock()
				events = append(events, ev.Percent)
				mu.Unlock()
			},
		},
	})
	defer store.Close()

	if err := store.Set("a", string(make([]byte, 25))); err != nil {
		t.Fatalf("set a error: %v", err)
	}
	if err := store.Set("b", string(make([]byte, 25))); err != nil {
		t.Fatalf("set b error: %v", err)
	}
	if err := store.Set("c", string(make([]byte, 25))); err != nil {
		t.Fatalf("set c error: %v", err)
	}
	if err := store.Set("d", string(make([]byte, 15))); err != nil {
		t.Fatalf("set d error: %v", err)
	}

	stats := store.Stats()
	if stats.Volume != 90 || stats.UsagePercent != 90 {
		t.Fatalf("unexpected stats: %+v", stats)
	}

	mu.Lock()
	got := append([]int(nil), events...)
	mu.Unlock()
	want := []int{25, 50, 75, 90}
	if len(got) != len(want) {
		t.Fatalf("unexpected thresholds count: got=%v want=%v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("unexpected threshold order: got=%v want=%v", got, want)
		}
	}

	if err := store.Set("overflow", string(make([]byte, 11))); !errors.Is(err, ErrCacheFull) {
		t.Fatalf("expected ErrCacheFull, got %v", err)
	}
}

type userProfile struct {
	Name  string
	Email string
	Tags  []string
}

func TestAutoSizeEstimator(t *testing.T) {
	store := New[string, userProfile](Options[string, userProfile]{
		MaxVolume: 10_000,
	})
	defer store.Close()

	value := userProfile{
		Name:  "Alice",
		Email: "alice@company.dev",
		Tags:  []string{"eng", "lead", "platform"},
	}

	if err := store.Set("user:42", value); err != nil {
		t.Fatalf("unexpected set error: %v", err)
	}

	stats := store.Stats()
	if stats.Keys != 1 {
		t.Fatalf("unexpected keys: %d", stats.Keys)
	}
	if stats.Volume <= 1 {
		t.Fatalf("expected auto estimated volume > 1, got: %d", stats.Volume)
	}
	if stats.UsagePercent == 0 {
		t.Fatalf("expected non-zero usage percent with max volume set")
	}

	meta := store.Meta()
	if meta.Objects != stats.Keys || meta.Volume != stats.Volume {
		t.Fatalf("meta/stats mismatch: meta=%+v stats=%+v", meta, stats)
	}
}

func TestDefaultEstimatorWorksWithoutCustomConfig(t *testing.T) {
	store := New[int, userProfile](Options[int, userProfile]{})
	defer store.Close()

	if err := store.Set(1, userProfile{
		Name:  "Bob",
		Email: "bob@example.org",
		Tags:  []string{"sre", "oncall"},
	}); err != nil {
		t.Fatalf("unexpected set error: %v", err)
	}

	stats := store.Stats()
	if stats.Volume <= 1 {
		t.Fatalf("expected default estimator volume > 1, got %d", stats.Volume)
	}
	if stats.Keys != 1 {
		t.Fatalf("expected one key, got %d", stats.Keys)
	}
}

func TestSetBatchAndLoadBatch(t *testing.T) {
	store := New[string, string](Options[string, string]{
		DumpModeOn: true,
		MaxVolume:  1000,
		SizeEstimator: func(_ string, value string) int {
			return len(value)
		},
	})
	defer store.Close()

	if err := store.SetBatch([]BatchItem[string, string]{
		{Key: "a", Value: "one"},
		{Key: "b", Value: "two"},
	}); !errors.Is(err, ErrCacheNotReady) {
		t.Fatalf("expected ErrCacheNotReady from SetBatch, got %v", err)
	}

	if err := store.LoadBatch([]BatchItem[string, string]{
		{Key: "a", Value: "one"},
		{Key: "b", Value: "two"},
	}); err != nil {
		t.Fatalf("unexpected LoadBatch error: %v", err)
	}

	store.SetDumpMode(false)
	v, ok, err := store.Get("a")
	if err != nil || !ok || v != "one" {
		t.Fatalf("unexpected get after LoadBatch: v=%q ok=%v err=%v", v, ok, err)
	}
	stats := store.Stats()
	if stats.Keys != 2 || stats.Volume != 6 {
		t.Fatalf("unexpected stats after batch load: %+v", stats)
	}
}

func TestSetBatchAtomicOnMaxVolume(t *testing.T) {
	store := New[string, string](Options[string, string]{
		MaxVolume: 5,
		SizeEstimator: func(_ string, value string) int {
			return len(value)
		},
	})
	defer store.Close()

	if err := store.Set("existing", "abc"); err != nil {
		t.Fatalf("unexpected set error: %v", err)
	}

	err := store.SetBatch([]BatchItem[string, string]{
		{Key: "x", Value: "de"},
		{Key: "y", Value: "fg"}, // would exceed max (3 + 2 + 2 = 7)
	})
	if !errors.Is(err, ErrCacheFull) {
		t.Fatalf("expected ErrCacheFull, got %v", err)
	}

	stats := store.Stats()
	if stats.Keys != 1 || stats.Volume != 3 {
		t.Fatalf("batch should be atomic, got stats: %+v", stats)
	}
}

func TestBatchWithTTL(t *testing.T) {
	store := New[int, string](Options[int, string]{
		GCInterval: 10 * time.Millisecond,
	})
	defer store.Close()

	if err := store.SetBatchWithTTL([]BatchTTLItem[int, string]{
		{Key: 1, Value: "short", TTL: 20 * time.Millisecond},
		{Key: 2, Value: "long", TTL: 300 * time.Millisecond},
	}); err != nil {
		t.Fatalf("unexpected SetBatchWithTTL error: %v", err)
	}

	waitUntil(t, 400*time.Millisecond, func() bool {
		stats := store.Stats()
		return stats.Keys == 1
	})

	_, ok, err := store.Get(1)
	if err != nil {
		t.Fatalf("unexpected get error: %v", err)
	}
	if ok {
		t.Fatalf("expected key 1 to expire")
	}

	v, ok, err := store.Get(2)
	if err != nil {
		t.Fatalf("unexpected get error: %v", err)
	}
	if !ok || v != "long" {
		t.Fatalf("expected key 2 to remain")
	}
}

func TestTTLInvalidationFromGet(t *testing.T) {
	var (
		mu        sync.Mutex
		hookCount int
		sources   []InvalidationSource
	)

	store := New[string, string](Options[string, string]{
		GCInterval:                time.Second,
		InvalidationChannelBuffer: 16,
		Hooks: Hooks{
			OnTTLInvalidated: func(ev InvalidationEvent) {
				mu.Lock()
				hookCount++
				sources = append(sources, ev.Source)
				mu.Unlock()
			},
		},
	})
	defer store.Close()

	if err := store.SetWithTTL("lazy", "x", 20*time.Millisecond); err != nil {
		t.Fatalf("set lazy ttl error: %v", err)
	}

	time.Sleep(30 * time.Millisecond)

	_, ok, err := store.Get("lazy")
	if err != nil {
		t.Fatalf("unexpected get error: %v", err)
	}
	if ok {
		t.Fatalf("expected lazy key to be invalidated")
	}

	eventsCh := store.InvalidationEvents()
	if eventsCh == nil {
		t.Fatalf("expected invalidation channel to be enabled")
	}

	waitUntil(t, 500*time.Millisecond, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return hookCount >= 1
	})

	drained := 0
collect:
	for {
		select {
		case <-eventsCh:
			drained++
		default:
			break collect
		}
	}
	if drained == 0 {
		t.Fatalf("expected at least one event from invalidation channel")
	}

	mu.Lock()
	defer mu.Unlock()
	var hasGet bool
	for _, src := range sources {
		if src == InvalidationSourceGet {
			hasGet = true
		}
	}
	if !hasGet {
		t.Fatalf("expected get invalidation source, got: %v", sources)
	}
}

func TestTTLInvalidationFromGC(t *testing.T) {
	var (
		mu      sync.Mutex
		sources []InvalidationSource
	)

	store := New[string, string](Options[string, string]{
		GCInterval:                10 * time.Millisecond,
		InvalidationChannelBuffer: 16,
		Hooks: Hooks{
			OnTTLInvalidated: func(ev InvalidationEvent) {
				mu.Lock()
				sources = append(sources, ev.Source)
				mu.Unlock()
			},
		},
	})
	defer store.Close()

	if err := store.SetWithTTL("bg", "y", 20*time.Millisecond); err != nil {
		t.Fatalf("set bg ttl error: %v", err)
	}

	waitUntil(t, 500*time.Millisecond, func() bool {
		_, ok, err := store.Get("bg")
		return err == nil && !ok
	})

	waitUntil(t, 500*time.Millisecond, func() bool {
		mu.Lock()
		defer mu.Unlock()
		for _, src := range sources {
			if src == InvalidationSourceGC {
				return true
			}
		}
		return false
	})

	eventsCh := store.InvalidationEvents()
	if eventsCh == nil {
		t.Fatalf("expected invalidation channel to be enabled")
	}

	foundGC := false
collect:
	for {
		select {
		case ev := <-eventsCh:
			if ev.Source == InvalidationSourceGC {
				foundGC = true
			}
		default:
			break collect
		}
	}
	if !foundGC {
		t.Fatalf("expected gc invalidation event in channel")
	}
}

func waitUntil(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("condition was not met before timeout")
}

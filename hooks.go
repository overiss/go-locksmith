package locksmith

import "time"

func (s *Store[K, V]) notifyChanges(keysChanged bool, keys int, volumeChanged bool, volume int) {
	if keysChanged && s.hooks.OnKeysChanged != nil {
		s.hooks.OnKeysChanged(keys)
	}
	if volumeChanged && s.hooks.OnVolumeChanged != nil {
		s.hooks.OnVolumeChanged(volume)
	}
}

func (s *Store[K, V]) notifyGCStart() {
	if s.hooks.OnGCStart != nil {
		s.hooks.OnGCStart()
	}
}

func (s *Store[K, V]) notifyFillHooks(mask uint8, volume int) {
	if mask == 0 {
		return
	}
	max := s.maxVolume
	if max <= 0 {
		return
	}

	if mask&mask25 != 0 && s.hooks.OnFill25 != nil {
		s.hooks.OnFill25(UsageEvent{Percent: 25, Volume: volume, MaxVolume: max})
	}
	if mask&mask50 != 0 && s.hooks.OnFill50 != nil {
		s.hooks.OnFill50(UsageEvent{Percent: 50, Volume: volume, MaxVolume: max})
	}
	if mask&mask75 != 0 && s.hooks.OnFill75 != nil {
		s.hooks.OnFill75(UsageEvent{Percent: 75, Volume: volume, MaxVolume: max})
	}
	if mask&mask90 != 0 && s.hooks.OnFill90 != nil {
		s.hooks.OnFill90(UsageEvent{Percent: 90, Volume: volume, MaxVolume: max})
	}
}

func (s *Store[K, V]) notifyTTLInvalidated(event InvalidationEvent) {
	if s.hooks.OnTTLInvalidated != nil {
		s.hooks.OnTTLInvalidated(event)
	}
	if s.invCh == nil {
		return
	}
	select {
	case s.invCh <- event:
	default:
		// Drop if consumer is slower than producer.
	}
}

func makeInvalidationEvent[K Key](key K, expiresAtNano int64, source InvalidationSource) InvalidationEvent {
	now := time.Now()
	ev := InvalidationEvent{
		Key:           any(key),
		InvalidatedAt: now,
		Source:        source,
	}
	if expiresAtNano > 0 {
		ev.ExpiredAt = time.Unix(0, expiresAtNano)
	}
	return ev
}

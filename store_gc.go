package locksmith

import "time"

func (s *Store[K, V]) gcLoop() {
	ticker := time.NewTicker(s.gcInterval)
	defer func() {
		ticker.Stop()
		close(s.doneCh)
	}()

	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			s.runGC()
		}
	}
}

func (s *Store[K, V]) runGC() {
	s.notifyGCStart()

	nowNano := time.Now().UnixNano()
	var keysChanged bool
	var volumeChanged bool
	var keys int
	var volume int
	var usageEnterMask uint8
	var invalidated []InvalidationEvent

	s.mu.Lock()
	prevMask := s.usageMask
	for key, expiresAt := range s.ttl {
		if nowNano < expiresAt {
			continue
		}

		ent, ok := s.data[key]
		if !ok {
			delete(s.ttl, key)
			continue
		}
		if ent.expiresAt != expiresAt {
			// TTL was updated after ttl index was read.
			s.ttl[key] = ent.expiresAt
			continue
		}

		delete(s.data, key)
		delete(s.ttl, key)
		s.volume -= ent.size
		keysChanged = true
		volumeChanged = true
		invalidated = append(invalidated, makeInvalidationEvent(key, ent.expiresAt, InvalidationSourceGC))
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
	for _, ev := range invalidated {
		s.notifyTTLInvalidated(ev)
	}
}

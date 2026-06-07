package locksmith

import "errors"

var (
	ErrCacheNotReady = errors.New("locksmith: cache is not ready")
	ErrCacheFull     = errors.New("locksmith: cache max volume exceeded")
	ErrInvalidTTL    = errors.New("locksmith: ttl must be greater than zero")
)

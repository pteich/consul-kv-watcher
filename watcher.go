package watcher

import (
	"context"
	"time"

	"github.com/cenkalti/backoff/v4"
	consul "github.com/hashicorp/consul/api"
)

// DefaultWaitTime is the maximum wait time allowed by Consul
const DefaultWaitTime = 10 * time.Minute

// Watcher is a wrapper around the Consul client that watches for changes to a keys and directories
type Watcher struct {
	consul       *consul.Client
	backoff      *backoff.ExponentialBackOff
	debounceTime time.Duration
}

// New returns a new Watcher
func New(consulClient *consul.Client, retryTime time.Duration, debounceTime time.Duration) *Watcher {
	bf := backoff.NewExponentialBackOff()
	bf.InitialInterval = retryTime
	return &Watcher{
		consul:       consulClient,
		backoff:      bf,
		debounceTime: debounceTime,
	}
}

// WatchTree watches for changes to a directory and emit key value pairs
func (w *Watcher) WatchTree(ctx context.Context, path string) (<-chan consul.KVPairs, error) {
	out := make(chan consul.KVPairs)
	kv := w.consul.KV()
	var debounceTimer *time.Timer
	var debounceStart time.Time

	opts := &consul.QueryOptions{
		AllowStale:        true,
		RequireConsistent: false,
		UseCache:          true,
		WaitTime:          DefaultWaitTime,
	}

	go func() {
		defer close(out)

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			kvPairs, meta, err := kv.List(path, opts.WithContext(ctx))
			if err != nil {
				if consul.IsRetryableError(err) {
					opts.WaitIndex = 0
					select {
					case <-ctx.Done():
						return
					case <-time.After(w.backoff.NextBackOff()):
						continue
					}
				}

				return
			}

			w.backoff.Reset()
			if opts.WaitIndex != meta.LastIndex {
				if debounceTimer != nil {
					debounceTimer.Stop()
				}
				if opts.WaitIndex <= 0 ||
					(!debounceStart.IsZero() && time.Since(debounceStart) > 2*w.debounceTime) {
					out <- kvPairs
					debounceStart = time.Time{}
				} else {
					if debounceStart.IsZero() {
						debounceStart = time.Now()
					}
					debounceTimer = time.AfterFunc(w.debounceTime, func() {
						out <- kvPairs
						debounceStart = time.Time{}
					})
				}
				opts.WaitIndex = meta.LastIndex
			}
		}
	}()

	return out, nil
}

// WatchKey watches for changes to a key and emits a key value pair
func (w *Watcher) WatchKey(ctx context.Context, key string) (<-chan *consul.KVPair, error) {
	out := make(chan *consul.KVPair)
	kv := w.consul.KV()
	var debounceStart time.Time
	var debounceTimer *time.Timer

	opts := &consul.QueryOptions{
		AllowStale:        true,
		RequireConsistent: false,
		UseCache:          true,
		WaitTime:          DefaultWaitTime,
	}

	go func() {
		defer close(out)

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			kvPair, meta, err := kv.Get(key, opts.WithContext(ctx))
			if err != nil {
				if consul.IsRetryableError(err) {
					opts.WaitIndex = 0
					select {
					case <-ctx.Done():
						return
					case <-time.After(w.backoff.NextBackOff()):
						continue
					}
				}

				return
			}

			// reset backoff after successful load
			w.backoff.Reset()
			if opts.WaitIndex != meta.LastIndex {
				if debounceTimer != nil {
					debounceTimer.Stop()
				}

				// don't debounce and wait if we start fresh without wait index
				if opts.WaitIndex <= 0 ||
					(!debounceStart.IsZero() && time.Since(debounceStart) > 2*w.debounceTime) {
					out <- kvPair
					debounceStart = time.Time{}
				} else {
					if debounceStart.IsZero() {
						debounceStart = time.Now()
					}

					debounceTimer = time.AfterFunc(w.debounceTime, func() {
						out <- kvPair
						debounceStart = time.Time{}
					})
				}
				opts.WaitIndex = meta.LastIndex
			}
		}
	}()

	return out, nil
}

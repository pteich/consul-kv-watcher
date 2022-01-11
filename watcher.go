package watcher

import (
	"context"
	"time"

	"github.com/cenkalti/backoff/v4"
	consul "github.com/hashicorp/consul/api"
)

// Watcher is a wrapper around the Consul client that watches for changes to a keys and directories
type Watcher struct {
	consul   *consul.Client
	backoff  *backoff.ExponentialBackOff
	waitTime time.Duration
}

// New returns a new Watcher
func New(consulClient *consul.Client, waitTime time.Duration, retryTime time.Duration) *Watcher {
	bf := backoff.NewExponentialBackOff()
	bf.InitialInterval = retryTime
	return &Watcher{
		consul:   consulClient,
		waitTime: waitTime,
		backoff:  bf,
	}
}

// WatchTree watches for changes to a directory and emit key value pairs
func (w *Watcher) WatchTree(ctx context.Context, path string) (<-chan consul.KVPairs, error) {
	out := make(chan consul.KVPairs)

	opts := &consul.QueryOptions{
		AllowStale:        true,
		RequireConsistent: false,
		UseCache:          true,
		WaitTime:          w.waitTime,
	}

	go func() {
		defer close(out)

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			kvPairs, meta, err := w.consul.KV().List(path, opts.WithContext(ctx))
			if err != nil {
				if consul.IsRetryableError(err) {
					opts.WaitIndex = 0
					time.Sleep(w.backoff.NextBackOff())
					continue
				}

				return
			}

			w.backoff.Reset()
			out <- kvPairs
			opts.WaitIndex = meta.LastIndex
			opts.WaitTime = w.waitTime
		}
	}()

	return out, nil
}

// WatchKey watches for changes to a key and emits a key value pair
func (w *Watcher) WatchKey(ctx context.Context, key string) (<-chan *consul.KVPair, error) {
	out := make(chan *consul.KVPair)

	opts := &consul.QueryOptions{
		AllowStale:        true,
		RequireConsistent: false,
		UseCache:          true,
		WaitTime:          w.waitTime,
	}

	go func() {
		defer close(out)

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			kvPair, meta, err := w.consul.KV().Get(key, opts.WithContext(ctx))
			if err != nil {
				if consul.IsRetryableError(err) {
					opts.WaitIndex = 0
					time.Sleep(w.backoff.NextBackOff())
					continue
				}

				return
			}

			w.backoff.Reset()
			out <- kvPair
			opts.WaitIndex = meta.LastIndex
			opts.WaitTime = w.waitTime
		}
	}()

	return out, nil
}

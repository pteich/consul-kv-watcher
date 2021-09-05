package watcher

import (
	"context"
	"time"

	consul "github.com/hashicorp/consul/api"
)

// Watcher is a wrapper around the Consul client that watches for changes to a keys and directories
type Watcher struct {
	consul    *consul.Client
	retryTime time.Duration
	waitTime  time.Duration
}

// New returns a new Watcher
func New(consulClient *consul.Client, waitTime time.Duration, retryTime time.Duration) *Watcher {
	return &Watcher{consul: consulClient, waitTime: waitTime, retryTime: retryTime}
}

// WatchTree watches for changes to a directory and emit key value pairs
func (w *Watcher) WatchTree(ctx context.Context, path string) (<-chan consul.KVPairs, error) {
	out := make(chan consul.KVPairs)

	opts := &consul.QueryOptions{
		AllowStale:        true,
		RequireConsistent: false,
		UseCache:          true,
	}

	go func() {
		defer func() {
			close(out)
		}()

		for {
			kvPairs, meta, err := w.consul.KV().List(path, opts.WithContext(ctx))
			if err != nil {
				if consul.IsRetryableError(err) {
					opts.WaitIndex = 0
					time.Sleep(w.retryTime)
				} else {
					return
				}
			} else {
				out <- kvPairs
				opts.WaitIndex = meta.LastIndex
				opts.WaitTime = w.waitTime
			}

			select {
			case <-ctx.Done():
				return
			default:
			}
		}
	}()

	return out, nil
}

# Consul K/V Watcher

A simple wrapper around Consul API to watch for changes in a specific K/V directory or changes on one key.
Uses blocking queries and cache by default. It also implements a simple exponential backoff for retryable errors.

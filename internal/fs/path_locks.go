package fs

import (
	"sort"
	"sync"
)

type pathLockEntry struct {
	mu   sync.Mutex
	refs int
}

type pathLocks struct {
	mu      sync.Mutex
	entries map[string]*pathLockEntry
}

func newPathLocks() *pathLocks {
	return &pathLocks{entries: make(map[string]*pathLockEntry)}
}

func (l *pathLocks) Lock(paths ...string) func() {
	keys := normalizePathLockKeys(paths)
	if len(keys) == 0 {
		return func() {}
	}

	entries := make([]*pathLockEntry, len(keys))
	l.mu.Lock()
	for i, key := range keys {
		entry := l.entries[key]
		if entry == nil {
			entry = &pathLockEntry{}
			l.entries[key] = entry
		}
		entry.refs++
		entries[i] = entry
	}
	l.mu.Unlock()

	for _, entry := range entries {
		entry.mu.Lock()
	}

	return func() {
		for i := len(entries) - 1; i >= 0; i-- {
			entries[i].mu.Unlock()
		}

		l.mu.Lock()
		for i, key := range keys {
			entry := entries[i]
			entry.refs--
			if entry.refs == 0 {
				delete(l.entries, key)
			}
		}
		l.mu.Unlock()
	}
}

func normalizePathLockKeys(paths []string) []string {
	seen := make(map[string]struct{}, len(paths))
	keys := make([]string, 0, len(paths))
	for _, value := range paths {
		key := canonicalPathKey(value)
		if key == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

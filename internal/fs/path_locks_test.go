package fs

import (
	"runtime"
	"testing"
	"time"
)

func TestPathLocksSerializeSameNormalizedPath(t *testing.T) {
	locks := newPathLocks()
	unlockFirst := locks.Lock("dir/file")

	secondAcquired := make(chan func(), 1)
	go func() {
		secondAcquired <- locks.Lock("/dir/file/")
	}()

	waitForPathLockCondition(t, func() bool {
		locks.mu.Lock()
		defer locks.mu.Unlock()
		entry, ok := locks.entries["dir/file"]
		return ok && entry.refs == 2
	})
	select {
	case unlockSecond := <-secondAcquired:
		unlockSecond()
		t.Fatal("second lock acquired while the normalized path was locked")
	default:
	}

	unlockFirst()
	var unlockSecond func()
	select {
	case unlockSecond = <-secondAcquired:
	case <-time.After(time.Second):
		t.Fatal("second lock did not acquire after the first was released")
	}

	locks.mu.Lock()
	entry, ok := locks.entries["dir/file"]
	entryCount := len(locks.entries)
	refs := 0
	if ok {
		refs = entry.refs
	}
	locks.mu.Unlock()
	if !ok || entryCount != 1 || refs != 1 {
		t.Fatalf("entries=%d refs=%d ok=%v, want one remaining reservation", entryCount, refs, ok)
	}

	unlockSecond()
	locks.mu.Lock()
	remaining := len(locks.entries)
	locks.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("entries after final release=%d, want 0", remaining)
	}
}

func TestPathLocksAllowDifferentPathsConcurrently(t *testing.T) {
	locks := newPathLocks()
	unlockA := locks.Lock("a")

	bAcquired := make(chan func(), 1)
	go func() {
		bAcquired <- locks.Lock("b")
	}()

	select {
	case unlockB := <-bAcquired:
		unlockB()
	case <-time.After(time.Second):
		unlockA()
		t.Fatal("different path was blocked")
	}
	unlockA()
}

func TestPathLocksOppositeOrdersDoNotDeadlock(t *testing.T) {
	previousProcs := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(previousProcs)

	locks := newPathLocks()
	unlockA := locks.Lock("a")
	unlockB := locks.Lock("b")
	done := make(chan struct{}, 2)

	go func() {
		unlock := locks.Lock("a", "b")
		unlock()
		done <- struct{}{}
	}()
	waitForPathLockCondition(t, func() bool {
		locks.mu.Lock()
		defer locks.mu.Unlock()
		return locks.entries["a"].refs == 2 && locks.entries["b"].refs == 2
	})

	go func() {
		unlock := locks.Lock("b", "a")
		unlock()
		done <- struct{}{}
	}()
	waitForPathLockCondition(t, func() bool {
		locks.mu.Lock()
		defer locks.mu.Unlock()
		return locks.entries["a"].refs == 3 && locks.entries["b"].refs == 3
	})

	unlockB()
	runtime.Gosched()
	unlockA()

	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	for range 2 {
		select {
		case <-done:
		case <-timer.C:
			t.Fatal("opposite path orders deadlocked")
		}
	}
}

func TestPathLocksNormalizeAndDeduplicatePaths(t *testing.T) {
	locks := newPathLocks()
	acquired := make(chan func(), 1)
	go func() {
		acquired <- locks.Lock("a", "/a/", "//a//")
	}()

	var unlock func()
	select {
	case unlock = <-acquired:
	case <-time.After(time.Second):
		t.Fatal("duplicate normalized paths caused a self-deadlock")
	}

	locks.mu.Lock()
	entry, ok := locks.entries["a"]
	entryCount := len(locks.entries)
	refs := 0
	if ok {
		refs = entry.refs
	}
	locks.mu.Unlock()
	if !ok || entryCount != 1 || refs != 1 {
		t.Fatalf("entries=%d refs=%d ok=%v, want one reservation for a", entryCount, refs, ok)
	}

	unlock()
	locks.mu.Lock()
	remaining := len(locks.entries)
	locks.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("entries after release=%d, want 0", remaining)
	}
}

func TestPathLocksTreatNFDAndNFCAsOneKey(t *testing.T) {
	keys := normalizePathLockKeys([]string{"cafe\u0301.md", "caf\u00e9.md"})
	if len(keys) != 1 || keys[0] != "caf\u00e9.md" {
		t.Fatalf("keys=%q want one NFC key", keys)
	}
}

func TestPathLocksIgnoreEmptyAndRootPaths(t *testing.T) {
	locks := newPathLocks()
	locks.Lock()()
	locks.Lock("", "/", "///")()

	locks.mu.Lock()
	remaining := len(locks.entries)
	locks.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("entries=%d, want 0", remaining)
	}
}

func waitForPathLockCondition(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for path lock state")
		}
		runtime.Gosched()
	}
}

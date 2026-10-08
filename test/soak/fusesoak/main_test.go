package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"
)

func TestReadyGateWaitsForEveryWorker(t *testing.T) {
	gate := newReadyGate(3)
	allReady := make(chan struct{})
	go func() {
		gate.waitUntilReady()
		close(allReady)
	}()

	var released atomic.Int64
	for worker := 0; worker < 2; worker++ {
		go func() {
			gate.workerWait()
			released.Add(1)
		}()
	}
	select {
	case <-allReady:
		t.Fatal("gate reported ready before every worker arrived")
	case <-time.After(20 * time.Millisecond):
	}
	go func() {
		gate.workerWait()
		released.Add(1)
	}()
	select {
	case <-allReady:
	case <-time.After(time.Second):
		t.Fatal("gate did not report all workers ready")
	}
	if got := released.Load(); got != 0 {
		t.Fatalf("workers released before start: %d", got)
	}
	gate.startWorkers()
	deadline := time.Now().Add(time.Second)
	for released.Load() != 3 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := released.Load(); got != 3 {
		t.Fatalf("released=%d want=3", got)
	}
}

func TestRunSoakCyclesAndCleansSuccessfulRun(t *testing.T) {
	root := t.TempDir()
	cfg := config{
		Root:        root,
		RunID:       "short-run",
		Duration:    250 * time.Millisecond,
		Concurrency: 4,
		Size:        1024,
		Seed:        20260827,
	}
	result := runSoak(context.Background(), cfg)
	if result.ErrorCategory != "" || result.SuccessfulCycles == 0 || result.FailedCycles != 0 {
		t.Fatalf("result=%+v", result)
	}
	if result.Interrupted {
		t.Fatalf("successful result marked interrupted: %+v", result)
	}
	if result.ResidualPath != "" {
		t.Fatalf("successful result residual=%q", result.ResidualPath)
	}
	if _, err := os.Stat(filepath.Join(root, cfg.RunID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("run directory still exists: %v", err)
	}
}

func TestRunSoakRefusesAndPreservesPreexistingRunDirectory(t *testing.T) {
	root := t.TempDir()
	runDir := filepath.Join(root, "existing-run")
	if err := os.Mkdir(runDir, 0o700); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(runDir, "owned-by-someone-else")
	if err := os.WriteFile(marker, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	result := runSoak(context.Background(), config{
		Root: root, RunID: "existing-run", Duration: time.Second, Concurrency: 1, Size: 32, Seed: 1,
	})
	if result.ErrorCategory != "setup" || result.ResidualPath != runDir {
		t.Fatalf("result=%+v", result)
	}
	if content, err := os.ReadFile(marker); err != nil || string(content) != "keep" {
		t.Fatalf("preexisting marker content=%q err=%v", content, err)
	}
}

func TestValidateConfigRejectsUnsafeInputs(t *testing.T) {
	root := t.TempDir()
	valid := config{Root: root, RunID: "run-1", Duration: time.Second, Concurrency: 1, Size: 1, Seed: 1}
	tests := []struct {
		name   string
		mutate func(*config)
	}{
		{name: "empty root", mutate: func(c *config) { c.Root = "" }},
		{name: "unsafe run id", mutate: func(c *config) { c.RunID = "../escape" }},
		{name: "zero duration", mutate: func(c *config) { c.Duration = 0 }},
		{name: "zero concurrency", mutate: func(c *config) { c.Concurrency = 0 }},
		{name: "too much concurrency", mutate: func(c *config) { c.Concurrency = 257 }},
		{name: "zero size", mutate: func(c *config) { c.Size = 0 }},
		{name: "oversize", mutate: func(c *config) { c.Size = MaxSizeBytes + 1 }},
		{name: "negative cycle interval", mutate: func(c *config) { c.CycleInterval = -time.Nanosecond }},
		{name: "cycle interval over one minute", mutate: func(c *config) { c.CycleInterval = time.Minute + time.Nanosecond }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := valid
			test.mutate(&cfg)
			if err := validateConfig(cfg); err == nil {
				t.Fatalf("config=%+v unexpectedly valid", cfg)
			}
		})
	}
}

func TestParseConfigDefaultsCycleIntervalToFiveSeconds(t *testing.T) {
	root := t.TempDir()
	cfg, err := parseConfig([]string{
		"--root", root,
		"--run-id", "default-interval",
		"--duration", "1s",
	})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.CycleInterval != 5*time.Second {
		t.Fatalf("cycle interval=%s want=5s", cfg.CycleInterval)
	}
}

func TestRunSoakWaitsBetweenCyclesAndCancelsWait(t *testing.T) {
	root := t.TempDir()
	started := time.Now()
	result := runSoak(context.Background(), config{
		Root:          root,
		RunID:         "paced-run",
		Duration:      120 * time.Millisecond,
		Concurrency:   1,
		Size:          32,
		Seed:          1,
		CycleInterval: time.Second,
	})
	if result.ErrorCategory != "" || result.SuccessfulCycles != 1 || result.FailedCycles != 0 {
		t.Fatalf("result=%+v", result)
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("cancelled interval wait took %s", elapsed)
	}
}

func TestPayloadIsExactSizeASCIIAndOperationSpecific(t *testing.T) {
	create := payload(3, 9, 1024, "create", 7)
	rewrite := payload(3, 9, 1024, "rewrite", 7)
	if len(create) != 1024 || len(rewrite) != 1024 {
		t.Fatalf("sizes create=%d rewrite=%d", len(create), len(rewrite))
	}
	if !utf8.Valid(create) || !utf8.Valid(rewrite) {
		t.Fatal("payload is not valid UTF-8")
	}
	if string(create) == string(rewrite) {
		t.Fatal("operation did not change payload")
	}
	if !strings.Contains(string(create[:128]), "worker=003") || !strings.Contains(string(create[:128]), "sequence=000000009") {
		t.Fatalf("missing deterministic header: %q", create[:128])
	}
}

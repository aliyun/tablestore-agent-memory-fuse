package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const MaxSizeBytes = 102300

var runIDPattern = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,64}$`)

type config struct {
	Root          string
	RunID         string
	Duration      time.Duration
	Concurrency   int
	Size          int
	Seed          int64
	CycleInterval time.Duration
}

type result struct {
	StartUTC         string  `json:"start_utc"`
	EndUTC           string  `json:"end_utc"`
	RequestedSeconds float64 `json:"requested_seconds"`
	ElapsedSeconds   float64 `json:"elapsed_seconds"`
	Concurrency      int     `json:"concurrency"`
	SizeBytes        int     `json:"size_bytes"`
	CycleIntervalSec float64 `json:"cycle_interval_seconds"`
	SuccessfulCycles int64   `json:"successful_cycles"`
	FailedCycles     int64   `json:"failed_cycles"`
	Interrupted      bool    `json:"interrupted"`
	ErrorCategory    string  `json:"error_category,omitempty"`
	ResidualPath     string  `json:"residual_path,omitempty"`
}

type cycleFailure struct {
	category string
}

func (e *cycleFailure) Error() string { return e.category }

type readyGate struct {
	workers int
	ready   chan struct{}
	start   chan struct{}
}

func newReadyGate(workers int) *readyGate {
	return &readyGate{
		workers: workers,
		ready:   make(chan struct{}, workers),
		start:   make(chan struct{}),
	}
}

func (g *readyGate) workerWait() {
	g.ready <- struct{}{}
	<-g.start
}

func (g *readyGate) waitUntilReady() {
	for worker := 0; worker < g.workers; worker++ {
		<-g.ready
	}
}

func (g *readyGate) startWorkers() {
	close(g.start)
}

func validateConfig(cfg config) error {
	if cfg.Root == "" {
		return errors.New("root")
	}
	info, err := os.Stat(cfg.Root)
	if err != nil || !info.IsDir() {
		return errors.New("root")
	}
	if !runIDPattern.MatchString(cfg.RunID) {
		return errors.New("run-id")
	}
	if cfg.Duration <= 0 {
		return errors.New("duration")
	}
	if cfg.Concurrency <= 0 || cfg.Concurrency > 256 {
		return errors.New("concurrency")
	}
	if cfg.Size <= 0 || cfg.Size > MaxSizeBytes {
		return errors.New("size")
	}
	if cfg.CycleInterval < 0 || cfg.CycleInterval > time.Minute {
		return errors.New("cycle-interval")
	}
	return nil
}

func runSoak(parent context.Context, cfg config) result {
	started := time.Now().UTC()
	out := result{
		StartUTC:         started.Format(time.RFC3339Nano),
		RequestedSeconds: cfg.Duration.Seconds(),
		Concurrency:      cfg.Concurrency,
		SizeBytes:        cfg.Size,
		CycleIntervalSec: cfg.CycleInterval.Seconds(),
	}
	finish := func() result {
		ended := time.Now().UTC()
		out.EndUTC = ended.Format(time.RFC3339Nano)
		out.ElapsedSeconds = ended.Sub(started).Seconds()
		return out
	}
	if err := validateConfig(cfg); err != nil {
		out.ErrorCategory = "config"
		return finish()
	}

	runDir := filepath.Join(filepath.Clean(cfg.Root), cfg.RunID)
	if err := os.Mkdir(runDir, 0o700); err != nil {
		out.ErrorCategory = "setup"
		out.ResidualPath = runDir
		return finish()
	}
	sentinels := make([]string, cfg.Concurrency)
	for worker := 0; worker < cfg.Concurrency; worker++ {
		path := filepath.Join(runDir, fmt.Sprintf("sentinel-%03d.md", worker))
		content := sentinel(worker, cfg.Seed)
		if err := createSync(path, content); err != nil {
			out.ErrorCategory = "sentinel-create"
			out.ResidualPath = runDir
			return finish()
		}
		sentinels[worker] = path
	}
	var runCtx context.Context
	var cancel context.CancelFunc
	var successful atomic.Int64
	var failed atomic.Int64
	failures := make(chan string, 1)
	var wg sync.WaitGroup
	gate := newReadyGate(cfg.Concurrency)
	for worker := 0; worker < cfg.Concurrency; worker++ {
		worker := worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			gate.workerWait()
			for sequence := int64(0); ; sequence++ {
				select {
				case <-runCtx.Done():
					return
				default:
				}
				if err := runCycle(runDir, sentinels[worker], worker, sequence, cfg); err != nil {
					failed.Add(1)
					category := "operation"
					var cycleErr *cycleFailure
					if errors.As(err, &cycleErr) {
						category = cycleErr.category
					}
					select {
					case failures <- category:
					default:
					}
					cancel()
					return
				}
				successful.Add(1)
				if cfg.CycleInterval > 0 {
					timer := time.NewTimer(cfg.CycleInterval)
					select {
					case <-runCtx.Done():
						timer.Stop()
						return
					case <-timer.C:
					}
				}
			}
		}()
	}
	gate.waitUntilReady()
	runCtx, cancel = context.WithTimeout(parent, cfg.Duration)
	defer cancel()
	gate.startWorkers()
	wg.Wait()
	out.SuccessfulCycles = successful.Load()
	out.FailedCycles = failed.Load()
	select {
	case category := <-failures:
		out.ErrorCategory = category
		out.ResidualPath = runDir
		return finish()
	default:
	}
	if parent.Err() != nil {
		out.Interrupted = true
		out.ErrorCategory = "interrupted"
		out.ResidualPath = runDir
		return finish()
	}
	if !errors.Is(runCtx.Err(), context.DeadlineExceeded) {
		out.Interrupted = true
		out.ErrorCategory = "interrupted"
		out.ResidualPath = runDir
		return finish()
	}
	if err := os.RemoveAll(runDir); err != nil {
		out.ErrorCategory = "cleanup"
		out.ResidualPath = runDir
		return finish()
	}
	return finish()
}

func runCycle(runDir, sentinelPath string, worker int, sequence int64, cfg config) error {
	base := filepath.Join(runDir, fmt.Sprintf("worker-%03d.md", worker))
	moved := filepath.Join(runDir, fmt.Sprintf("worker-%03d-moved.md", worker))
	createContent := payload(worker, sequence, cfg.Size, "create", cfg.Seed)
	if err := createSync(base, createContent); err != nil {
		return &cycleFailure{category: "create"}
	}
	if err := readEqual(base, createContent); err != nil {
		return &cycleFailure{category: "read-create"}
	}
	rewriteContent := payload(worker, sequence, cfg.Size, "rewrite", cfg.Seed)
	if err := rewriteSync(base, rewriteContent); err != nil {
		return &cycleFailure{category: "rewrite"}
	}
	suffix := []byte("|append|")
	if err := appendSync(base, suffix); err != nil {
		return &cycleFailure{category: "append"}
	}
	expected := append(append([]byte(nil), rewriteContent...), suffix...)
	if err := readEqual(base, expected); err != nil {
		return &cycleFailure{category: "read-append"}
	}
	if err := os.Rename(base, moved); err != nil {
		return &cycleFailure{category: "rename"}
	}
	if err := readEqual(moved, expected); err != nil {
		return &cycleFailure{category: "read-rename"}
	}
	if err := os.Remove(moved); err != nil {
		return &cycleFailure{category: "delete"}
	}
	if _, err := os.Lstat(moved); !errors.Is(err, os.ErrNotExist) {
		return &cycleFailure{category: "delete-readback"}
	}
	if sequence%100 == 0 {
		if err := readEqual(sentinelPath, sentinel(worker, cfg.Seed)); err != nil {
			return &cycleFailure{category: "sentinel-read"}
		}
	}
	return nil
}

func payload(worker int, sequence int64, size int, operation string, seed int64) []byte {
	header := fmt.Sprintf("fusesoak-v1 worker=%03d sequence=%09d op=%s seed=%d\n", worker, sequence, operation, seed)
	out := make([]byte, size)
	for i := range out {
		out[i] = byte('a' + (int(seed)+worker+int(sequence)+i)%26)
	}
	copy(out, header)
	return out
}

func sentinel(worker int, seed int64) []byte {
	return []byte(fmt.Sprintf("fusesoak-sentinel-v1 worker=%03d seed=%d\n", worker, seed))
}

func createSync(path string, content []byte) error {
	return writeSync(path, content, os.O_WRONLY|os.O_CREATE|os.O_EXCL)
}

func rewriteSync(path string, content []byte) error {
	return writeSync(path, content, os.O_WRONLY|os.O_TRUNC)
}

func appendSync(path string, content []byte) error {
	return writeSync(path, content, os.O_WRONLY|os.O_APPEND)
}

func writeSync(path string, content []byte, flags int) error {
	file, err := os.OpenFile(path, flags, 0o600)
	if err != nil {
		return err
	}
	if _, err := file.Write(content); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}

func readEqual(path string, want []byte) error {
	got, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if !bytes.Equal(got, want) {
		return errors.New("content mismatch")
	}
	return nil
}

func parseConfig(args []string) (config, error) {
	flags := flag.NewFlagSet("fusesoak", flag.ContinueOnError)
	var cfg config
	flags.StringVar(&cfg.Root, "root", "", "mounted filesystem root")
	flags.StringVar(&cfg.RunID, "run-id", "", "unique run directory name")
	flags.DurationVar(&cfg.Duration, "duration", 24*time.Hour, "soak duration")
	flags.IntVar(&cfg.Concurrency, "concurrency", 16, "worker count")
	flags.IntVar(&cfg.Size, "size", 1024, "base payload size")
	flags.Int64Var(&cfg.Seed, "seed", 1, "deterministic payload seed")
	flags.DurationVar(&cfg.CycleInterval, "cycle-interval", 5*time.Second, "minimum wait between successful cycles")
	if err := flags.Parse(args); err != nil {
		return config{}, err
	}
	if flags.NArg() != 0 {
		return config{}, errors.New("positional arguments are not supported")
	}
	if err := validateConfig(cfg); err != nil {
		return config{}, err
	}
	return cfg, nil
}

func main() {
	cfg, err := parseConfig(os.Args[1:])
	if err != nil {
		_ = json.NewEncoder(os.Stdout).Encode(result{ErrorCategory: "config"})
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	out := runSoak(ctx, cfg)
	stop()
	_ = json.NewEncoder(os.Stdout).Encode(out)
	if out.ErrorCategory != "" || out.Interrupted {
		os.Exit(1)
	}
}

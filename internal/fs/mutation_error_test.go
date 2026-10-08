package fs

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"agent-memory-fuse/internal/client"
)

const (
	mutationPathSentinel    = "private/path-sentinel.md"
	mutationMessageSentinel = "raw-api-message-sentinel"
)

type diagnosticRoundTripFunc func(*http.Request) (*http.Response, error)

func (f diagnosticRoundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func knownSDKClientError(t *testing.T) error {
	t.Helper()
	remote, err := client.New(client.Config{
		Endpoint:     "https://example.invalid",
		Instance:     "test-instance",
		APIKey:       "test-api-key",
		StoreName:    "test-store",
		RetryBackoff: []time.Duration{},
		HTTPClient: &http.Client{Transport: diagnosticRoundTripFunc(func(*http.Request) (*http.Response, error) {
			return nil, errors.New(mutationMessageSentinel + " credential-sentinel endpoint-sentinel content-sentinel")
		})},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = remote.Read(context.Background(), mutationPathSentinel)
	if err == nil {
		t.Fatal("SDK request unexpectedly succeeded")
	}
	return err
}

type listErrorRemote struct {
	*fakeRemote
	err error
}

func (r *listErrorRemote) List(context.Context, string, string, int) (client.ListResult, error) {
	return client.ListResult{}, r.err
}

type readErrorRemote struct {
	*fakeRemote
	err error
}

func (r *readErrorRemote) Read(context.Context, string) (client.Memory, error) {
	return client.Memory{}, r.err
}

type deleteErrorRemote struct {
	*fakeRemote
	err error
}

func (r *deleteErrorRemote) Delete(context.Context, string, string) error { return r.err }

type renameErrorRemote struct {
	*fakeRemote
	err error
}

func (r *renameErrorRemote) Rename(context.Context, string, string, bool, string) (client.Memory, error) {
	return client.Memory{}, r.err
}

type blockingStatFailureRemote struct {
	*fakeRemote
	err     error
	started chan struct{}
	resume  chan struct{}
	once    sync.Once
}

func (r *blockingStatFailureRemote) Stat(context.Context, string) (client.Memory, error) {
	r.fakeRemote.mu.Lock()
	r.fakeRemote.calls = append(r.fakeRemote.calls, "Stat")
	r.fakeRemote.mu.Unlock()
	r.once.Do(func() { close(r.started) })
	<-r.resume
	return client.Memory{}, r.err
}

type lateJoinStatFailureRemote struct {
	*fakeRemote
	err           error
	firstStarted  chan struct{}
	releaseFirst  chan struct{}
	secondStarted chan struct{}
	calls         atomic.Int32
}

func (r *lateJoinStatFailureRemote) Stat(context.Context, string) (client.Memory, error) {
	r.fakeRemote.mu.Lock()
	r.fakeRemote.calls = append(r.fakeRemote.calls, "Stat")
	r.fakeRemote.mu.Unlock()
	switch r.calls.Add(1) {
	case 1:
		close(r.firstStarted)
		<-r.releaseFirst
	case 2:
		close(r.secondStarted)
	}
	return client.Memory{}, r.err
}

func TestMutationErrorEventSanitizesServiceAPIError(t *testing.T) {
	apiErr := &client.APIError{
		StatusCode: http.StatusInternalServerError,
		Code:       "INTERNAL_ERROR",
		RequestID:  "req-safe-500",
		Message:    mutationMessageSentinel + " " + mutationPathSentinel + " credential-sentinel endpoint-sentinel content-sentinel",
	}
	reported := make(chan MutationErrorEvent, 1)
	remote := &fakeRemote{
		memories:  map[string]client.Memory{},
		createErr: fmt.Errorf("wrapped-client-detail-sentinel: %w", apiErr),
	}
	filesystem := NewFileSystem(remote, Options{MutationErrorReporter: func(event MutationErrorEvent) {
		reported <- event
	}})
	handle, errno := filesystem.Create("/" + mutationPathSentinel + "/")
	if errno != 0 {
		t.Fatalf("Create errno=%v", errno)
	}
	if errno := filesystem.Commit(context.Background(), handle); errno != syscall.EIO {
		t.Fatalf("Commit errno=%v want EIO", errno)
	}

	event := receiveMutationErrorEvent(t, reported)
	wantHash := sha256.Sum256([]byte("agent-memory-fuse/mutation-path/v1\x00" + mutationPathSentinel))
	if event.Operation != "create" || event.PathSHA256 != hex.EncodeToString(wantHash[:]) {
		t.Fatalf("identity=(%q,%q) want create and normalized path hash", event.Operation, event.PathSHA256)
	}
	if !regexp.MustCompile(`^[0-9a-f]{64}$`).MatchString(event.PathSHA256) {
		t.Fatalf("path_sha256=%q is not 64 lowercase hex", event.PathSHA256)
	}
	if event.Category != "service" || event.Errno != int(syscall.EIO) || event.HTTPStatus != http.StatusInternalServerError || event.ServiceCode != "INTERNAL_ERROR" || event.RequestID != "req-safe-500" || event.Message != "service request failed" {
		t.Fatalf("event=%+v", event)
	}
	assertMutationEventSafe(t, event)
}

type updateErrorRemote struct {
	*fakeRemote
	err error
}

func (r *updateErrorRemote) Update(context.Context, string, []byte, string) (client.Memory, error) {
	return client.Memory{}, r.err
}

func TestMutationErrorEventClassifiesClientErrorFromActualUpdate(t *testing.T) {
	remote := &updateErrorRemote{
		fakeRemote: &fakeRemote{memories: map[string]client.Memory{
			mutationPathSentinel: {MemoryID: "mem-1", Path: mutationPathSentinel, Content: "old", LatestVersionID: "fmver_1"},
		}},
		err: errors.New(mutationMessageSentinel + " credential-sentinel endpoint-sentinel content-sentinel"),
	}
	reported := make(chan MutationErrorEvent, 1)
	filesystem := NewFileSystem(remote, Options{MutationErrorReporter: func(event MutationErrorEvent) { reported <- event }})
	handle, errno := filesystem.Open(context.Background(), mutationPathSentinel)
	if errno != 0 {
		t.Fatalf("Open errno=%v", errno)
	}
	if _, err := handle.WriteAt([]byte("new"), 0); err != nil {
		t.Fatal(err)
	}
	if errno := filesystem.Commit(context.Background(), handle); errno != syscall.EIO {
		t.Fatalf("Commit errno=%v want EIO", errno)
	}
	event := receiveMutationErrorEvent(t, reported)
	if event.Operation != "update" || event.Category != "client" || event.Errno != int(syscall.EIO) || event.Message != "client-side error" {
		t.Fatalf("event=%+v", event)
	}
	if event.HTTPStatus != 0 || event.ServiceCode != "" || event.RequestID != "" {
		t.Fatalf("client event has service fields: %+v", event)
	}
	assertMutationEventSafe(t, event)
}

func TestMutationErrorEventIncludesSafeSDKKind(t *testing.T) {
	event := newMutationErrorEvent("read", mutationPathSentinel, fmt.Errorf("wrapped-client-detail-sentinel: %w", knownSDKClientError(t)), syscall.EIO)
	if event.SDKKind != "transport_other" {
		t.Fatalf("SDKKind=%q want transport_other", event.SDKKind)
	}
	payload, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(payload), `"sdk_kind":"transport_other"`) {
		t.Fatalf("event JSON missing sdk_kind: %s", payload)
	}
	assertMutationEventSafe(t, event)
}

func TestMutationErrorEventServiceErrorOmitsSDKKind(t *testing.T) {
	event := newMutationErrorEvent("stat", mutationPathSentinel, &client.APIError{
		StatusCode: http.StatusInternalServerError,
		Code:       "INTERNAL_ERROR",
		Message:    mutationMessageSentinel,
	}, syscall.EIO)
	if event.SDKKind != "" {
		t.Fatalf("SDKKind=%q want empty", event.SDKKind)
	}
	payload, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(payload), "sdk_kind") {
		t.Fatalf("service event unexpectedly contains sdk_kind: %s", payload)
	}
	assertMutationEventSafe(t, event)
}

func TestRemoteOperationFailuresReportOperationAndSDKKind(t *testing.T) {
	sdkErr := knownSDKClientError(t)
	tests := []struct {
		name      string
		operation string
		run       func(reporter func(MutationErrorEvent)) syscall.Errno
	}{
		{
			name: "list", operation: "list",
			run: func(reporter func(MutationErrorEvent)) syscall.Errno {
				filesystem := NewFileSystem(&listErrorRemote{fakeRemote: &fakeRemote{memories: map[string]client.Memory{}}, err: sdkErr}, Options{MutationErrorReporter: reporter})
				return errnoFor(filesystem.EnsureFresh(context.Background()))
			},
		},
		{
			name: "stat", operation: "stat",
			run: func(reporter func(MutationErrorEvent)) syscall.Errno {
				filesystem := NewFileSystem(&statErrorRemote{fakeRemote: &fakeRemote{memories: map[string]client.Memory{}}, err: sdkErr}, Options{MutationErrorReporter: reporter})
				_, errno := filesystem.Stat(context.Background(), mutationPathSentinel)
				return errno
			},
		},
		{
			name: "read", operation: "read",
			run: func(reporter func(MutationErrorEvent)) syscall.Errno {
				filesystem := NewFileSystem(&readErrorRemote{fakeRemote: &fakeRemote{memories: map[string]client.Memory{}}, err: sdkErr}, Options{MutationErrorReporter: reporter})
				_, errno := filesystem.Open(context.Background(), mutationPathSentinel)
				return errno
			},
		},
		{
			name: "rename", operation: "rename",
			run: func(reporter func(MutationErrorEvent)) syscall.Errno {
				remote := &renameErrorRemote{fakeRemote: &fakeRemote{memories: map[string]client.Memory{}}, err: sdkErr}
				filesystem := NewFileSystem(remote, Options{MutationErrorReporter: reporter})
				filesystem.index.Upsert(client.Memory{MemoryID: "mem-safe", Path: mutationPathSentinel, LatestVersionID: "fmver-safe"})
				return filesystem.Rename(context.Background(), mutationPathSentinel, "renamed.md", false)
			},
		},
		{
			name: "delete", operation: "delete",
			run: func(reporter func(MutationErrorEvent)) syscall.Errno {
				remote := &deleteErrorRemote{fakeRemote: &fakeRemote{memories: map[string]client.Memory{}}, err: sdkErr}
				filesystem := NewFileSystem(remote, Options{MutationErrorReporter: reporter})
				filesystem.index.Upsert(client.Memory{MemoryID: "mem-safe", Path: mutationPathSentinel, LatestVersionID: "fmver-safe"})
				return filesystem.Delete(context.Background(), mutationPathSentinel)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reported := make(chan MutationErrorEvent, 1)
			if errno := test.run(func(event MutationErrorEvent) { reported <- event }); errno != syscall.EIO {
				t.Fatalf("errno=%v want EIO", errno)
			}
			event := receiveMutationErrorEvent(t, reported)
			if event.Operation != test.operation || event.SDKKind != "transport_other" || event.Category != "client" {
				t.Fatalf("event=%+v", event)
			}
			assertMutationEventSafe(t, event)
		})
	}
}

func TestRemoteOperationReportersRunAfterLocksAreReleased(t *testing.T) {
	sdkErr := knownSDKClientError(t)
	tests := []struct {
		name string
		run  func() syscall.Errno
	}{
		{
			name: "list refresh and state locks",
			run: func() syscall.Errno {
				var filesystem *FileSystem
				filesystem = NewFileSystem(&listErrorRemote{fakeRemote: &fakeRemote{memories: map[string]client.Memory{}}, err: sdkErr}, Options{MutationErrorReporter: func(MutationErrorEvent) {
					filesystem.refreshMu.Lock()
					filesystem.refreshMu.Unlock()
					filesystem.stateMu.Lock()
					filesystem.stateMu.Unlock()
				}})
				return errnoFor(filesystem.EnsureFresh(context.Background()))
			},
		},
		{
			name: "stat state and flight locks",
			run: func() syscall.Errno {
				var filesystem *FileSystem
				filesystem = NewFileSystem(&statErrorRemote{fakeRemote: &fakeRemote{memories: map[string]client.Memory{}}, err: sdkErr}, Options{MutationErrorReporter: func(MutationErrorEvent) {
					filesystem.stateMu.Lock()
					filesystem.stateMu.Unlock()
					filesystem.statMu.Lock()
					filesystem.statMu.Unlock()
				}})
				_, errno := filesystem.Stat(context.Background(), mutationPathSentinel)
				return errno
			},
		},
		{
			name: "read state lock",
			run: func() syscall.Errno {
				var filesystem *FileSystem
				filesystem = NewFileSystem(&readErrorRemote{fakeRemote: &fakeRemote{memories: map[string]client.Memory{}}, err: sdkErr}, Options{MutationErrorReporter: func(MutationErrorEvent) {
					filesystem.stateMu.Lock()
					filesystem.stateMu.Unlock()
				}})
				_, errno := filesystem.Open(context.Background(), mutationPathSentinel)
				return errno
			},
		},
		{
			name: "rename state and path locks",
			run: func() syscall.Errno {
				var filesystem *FileSystem
				filesystem = NewFileSystem(&renameErrorRemote{fakeRemote: &fakeRemote{memories: map[string]client.Memory{}}, err: sdkErr}, Options{MutationErrorReporter: func(MutationErrorEvent) {
					filesystem.stateMu.Lock()
					filesystem.stateMu.Unlock()
					unlock := filesystem.pathLocks.Lock(mutationPathSentinel, "renamed.md")
					unlock()
				}})
				filesystem.index.Upsert(client.Memory{MemoryID: "mem-safe", Path: mutationPathSentinel, LatestVersionID: "fmver-safe"})
				return filesystem.Rename(context.Background(), mutationPathSentinel, "renamed.md", false)
			},
		},
		{
			name: "delete state and path locks",
			run: func() syscall.Errno {
				var filesystem *FileSystem
				filesystem = NewFileSystem(&deleteErrorRemote{fakeRemote: &fakeRemote{memories: map[string]client.Memory{}}, err: sdkErr}, Options{MutationErrorReporter: func(MutationErrorEvent) {
					filesystem.stateMu.Lock()
					filesystem.stateMu.Unlock()
					unlock := filesystem.pathLocks.Lock(mutationPathSentinel)
					unlock()
				}})
				filesystem.index.Upsert(client.Memory{MemoryID: "mem-safe", Path: mutationPathSentinel, LatestVersionID: "fmver-safe"})
				return filesystem.Delete(context.Background(), mutationPathSentinel)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			done := make(chan syscall.Errno, 1)
			go func() { done <- test.run() }()
			select {
			case errno := <-done:
				if errno != syscall.EIO {
					t.Fatalf("errno=%v want EIO", errno)
				}
			case <-time.After(time.Second):
				t.Fatal("reporter blocked on an operation lock")
			}
		})
	}
}

func TestConcurrentCoalescedStatFailureReportsOnceOutsideStatMu(t *testing.T) {
	const callers = 8
	remote := &blockingStatFailureRemote{
		fakeRemote: &fakeRemote{memories: map[string]client.Memory{}},
		err:        knownSDKClientError(t),
		started:    make(chan struct{}),
		resume:     make(chan struct{}),
	}
	var reports atomic.Int32
	reported := make(chan struct{})
	var filesystem *FileSystem
	filesystem = NewFileSystem(remote, Options{MutationErrorReporter: func(event MutationErrorEvent) {
		filesystem.statMu.Lock()
		filesystem.statMu.Unlock()
		if event.Operation != "stat" || event.SDKKind != "transport_other" {
			t.Errorf("event=%+v", event)
		}
		reports.Add(1)
		close(reported)
	}})

	results := make(chan syscall.Errno, callers)
	go func() {
		_, errno := filesystem.Stat(context.Background(), mutationPathSentinel)
		results <- errno
	}()
	<-remote.started
	for range callers - 1 {
		go func() {
			_, errno := filesystem.Stat(context.Background(), mutationPathSentinel)
			results <- errno
		}()
	}
	waitForStatWaiters(t, filesystem, callers)
	close(remote.resume)
	for range callers {
		if errno := <-results; errno != syscall.EIO {
			t.Fatalf("Stat errno=%v want EIO", errno)
		}
	}
	if got := remote.callCount("Stat"); got != 1 {
		t.Fatalf("remote Stat calls=%d want 1", got)
	}
	select {
	case <-reported:
	case <-time.After(time.Second):
		t.Fatal("Stat failure reporter did not finish")
	}
	if got := reports.Load(); got != 1 {
		t.Fatalf("reports=%d want 1", got)
	}
}

func TestLateStatStartsNewFlightWhileFailureReporterIsBlocked(t *testing.T) {
	remote := &lateJoinStatFailureRemote{
		fakeRemote:    &fakeRemote{memories: map[string]client.Memory{}},
		err:           knownSDKClientError(t),
		firstStarted:  make(chan struct{}),
		releaseFirst:  make(chan struct{}),
		secondStarted: make(chan struct{}),
	}
	reporterEntered := make(chan struct{})
	secondReported := make(chan struct{})
	releaseReporter := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(releaseReporter) })
	var reports atomic.Int32
	filesystem := NewFileSystem(remote, Options{MutationErrorReporter: func(MutationErrorEvent) {
		switch reports.Add(1) {
		case 1:
			close(reporterEntered)
			<-releaseReporter
		case 2:
			close(secondReported)
		}
	}})

	firstResult := make(chan syscall.Errno, 1)
	go func() {
		_, errno := filesystem.Stat(context.Background(), mutationPathSentinel)
		firstResult <- errno
	}()
	<-remote.firstStarted
	close(remote.releaseFirst)
	<-reporterEntered

	lateResult := make(chan syscall.Errno, 1)
	go func() {
		_, errno := filesystem.Stat(context.Background(), mutationPathSentinel)
		lateResult <- errno
	}()

	select {
	case <-remote.secondStarted:
	case <-time.After(time.Second):
		releaseOnce.Do(func() { close(releaseReporter) })
		t.Fatal("late Stat reused the completed flight while its reporter was blocked")
	}
	for caller, result := range map[string]<-chan syscall.Errno{"first": firstResult, "late": lateResult} {
		var errno syscall.Errno
		select {
		case errno = <-result:
		case <-time.After(time.Second):
			releaseOnce.Do(func() { close(releaseReporter) })
			t.Fatalf("%s Stat remained blocked", caller)
		}
		if errno != syscall.EIO {
			t.Fatalf("%s Stat errno=%v want EIO", caller, errno)
		}
	}
	if got := remote.callCount("Stat"); got != 2 {
		t.Fatalf("remote Stat calls=%d want 2", got)
	}
	select {
	case <-secondReported:
	case <-time.After(time.Second):
		t.Fatal("second Stat failure was not reported")
	}
	if got := reports.Load(); got != 2 {
		t.Fatalf("reports=%d want 2", got)
	}
	releaseOnce.Do(func() { close(releaseReporter) })
}

func TestStatFlightDoesNotCarryReporterState(t *testing.T) {
	if _, exists := reflect.TypeOf(statFlight{}).FieldByName("reportOnce"); exists {
		t.Fatal("statFlight carries reporter-only state on the nil-reporter path")
	}
}

func waitForStatWaiters(t *testing.T, filesystem *FileSystem, want int) {
	t.Helper()
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for {
		filesystem.statMu.Lock()
		var waiters int
		for _, flight := range filesystem.statFlights {
			waiters += flight.waiters
		}
		filesystem.statMu.Unlock()
		if waiters == want {
			return
		}
		select {
		case <-deadline.C:
			t.Fatalf("stat waiters=%d want %d", waiters, want)
		default:
			runtime.Gosched()
		}
	}
}

func TestRemoteOperationReporterPanicsAreIsolated(t *testing.T) {
	sdkErr := knownSDKClientError(t)
	panicReporter := func(MutationErrorEvent) { panic("reporter-panic-sentinel") }
	tests := []struct {
		name string
		run  func() syscall.Errno
	}{
		{name: "list", run: func() syscall.Errno {
			filesystem := NewFileSystem(&listErrorRemote{fakeRemote: &fakeRemote{memories: map[string]client.Memory{}}, err: sdkErr}, Options{MutationErrorReporter: panicReporter})
			return errnoFor(filesystem.EnsureFresh(context.Background()))
		}},
		{name: "stat", run: func() syscall.Errno {
			filesystem := NewFileSystem(&statErrorRemote{fakeRemote: &fakeRemote{memories: map[string]client.Memory{}}, err: sdkErr}, Options{MutationErrorReporter: panicReporter})
			_, errno := filesystem.Stat(context.Background(), mutationPathSentinel)
			return errno
		}},
		{name: "read", run: func() syscall.Errno {
			filesystem := NewFileSystem(&readErrorRemote{fakeRemote: &fakeRemote{memories: map[string]client.Memory{}}, err: sdkErr}, Options{MutationErrorReporter: panicReporter})
			_, errno := filesystem.Open(context.Background(), mutationPathSentinel)
			return errno
		}},
		{name: "delete", run: func() syscall.Errno {
			filesystem := NewFileSystem(&deleteErrorRemote{fakeRemote: &fakeRemote{memories: map[string]client.Memory{}}, err: sdkErr}, Options{MutationErrorReporter: panicReporter})
			filesystem.index.Upsert(client.Memory{MemoryID: "mem-safe", Path: mutationPathSentinel, LatestVersionID: "fmver-safe"})
			return filesystem.Delete(context.Background(), mutationPathSentinel)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var panicValue any
			var errno syscall.Errno
			func() {
				defer func() { panicValue = recover() }()
				errno = test.run()
			}()
			if panicValue != nil || errno != syscall.EIO {
				t.Fatalf("panic=%v errno=%v want nil,EIO", panicValue, errno)
			}
		})
	}
}

type classificationCountingError struct{ asCalls int }

func (*classificationCountingError) Error() string { return "classification-counting-error" }
func (e *classificationCountingError) As(any) bool {
	e.asCalls++
	return false
}

func TestRemoteOperationsWithoutReporterDoNotAddSDKClassification(t *testing.T) {
	tests := []struct {
		name        string
		wantAsCalls int
		run         func(error) syscall.Errno
	}{
		{name: "list", wantAsCalls: 0, run: func(err error) syscall.Errno {
			filesystem := NewFileSystem(&listErrorRemote{fakeRemote: &fakeRemote{memories: map[string]client.Memory{}}, err: err}, Options{})
			if got := filesystem.EnsureFresh(context.Background()); got != err {
				t.Fatalf("EnsureFresh err=%v want identity", got)
			}
			return 0
		}},
		{name: "stat", wantAsCalls: 1, run: func(err error) syscall.Errno {
			filesystem := NewFileSystem(&statErrorRemote{fakeRemote: &fakeRemote{memories: map[string]client.Memory{}}, err: err}, Options{})
			_, errno := filesystem.Stat(context.Background(), mutationPathSentinel)
			return errno
		}},
		{name: "read", wantAsCalls: 1, run: func(err error) syscall.Errno {
			filesystem := NewFileSystem(&readErrorRemote{fakeRemote: &fakeRemote{memories: map[string]client.Memory{}}, err: err}, Options{})
			_, errno := filesystem.Open(context.Background(), mutationPathSentinel)
			return errno
		}},
		{name: "delete", wantAsCalls: 1, run: func(err error) syscall.Errno {
			filesystem := NewFileSystem(&deleteErrorRemote{fakeRemote: &fakeRemote{memories: map[string]client.Memory{}}, err: err}, Options{})
			filesystem.index.Upsert(client.Memory{MemoryID: "mem-safe", Path: mutationPathSentinel, LatestVersionID: "fmver-safe"})
			return filesystem.Delete(context.Background(), mutationPathSentinel)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			classificationErr := &classificationCountingError{}
			_ = test.run(classificationErr)
			if classificationErr.asCalls != test.wantAsCalls {
				t.Fatalf("errors.As calls=%d want %d", classificationErr.asCalls, test.wantAsCalls)
			}
		})
	}
}

func TestRemoteENOENTFailuresDoNotReport(t *testing.T) {
	notFound := &client.APIError{StatusCode: http.StatusNotFound, Code: "NOT_FOUND", Message: mutationMessageSentinel}
	reports := 0
	reporter := func(MutationErrorEvent) { reports++ }
	listFS := NewFileSystem(&listErrorRemote{fakeRemote: &fakeRemote{memories: map[string]client.Memory{}}, err: notFound}, Options{MutationErrorReporter: reporter})
	if errno := errnoFor(listFS.EnsureFresh(context.Background())); errno != syscall.ENOENT {
		t.Fatalf("list errno=%v want ENOENT", errno)
	}
	statFS := NewFileSystem(&statErrorRemote{fakeRemote: &fakeRemote{memories: map[string]client.Memory{}}, err: notFound}, Options{MutationErrorReporter: reporter})
	if _, errno := statFS.Stat(context.Background(), mutationPathSentinel); errno != syscall.ENOENT {
		t.Fatalf("stat errno=%v want ENOENT", errno)
	}
	readFS := NewFileSystem(&readErrorRemote{fakeRemote: &fakeRemote{memories: map[string]client.Memory{}}, err: notFound}, Options{MutationErrorReporter: reporter})
	if _, errno := readFS.Open(context.Background(), mutationPathSentinel); errno != syscall.ENOENT {
		t.Fatalf("read errno=%v want ENOENT", errno)
	}
	deleteFS := NewFileSystem(&deleteErrorRemote{fakeRemote: &fakeRemote{memories: map[string]client.Memory{}}, err: notFound}, Options{MutationErrorReporter: reporter})
	deleteFS.index.Upsert(client.Memory{MemoryID: "mem-safe", Path: mutationPathSentinel, LatestVersionID: "fmver-safe"})
	if errno := deleteFS.Delete(context.Background(), mutationPathSentinel); errno != syscall.ENOENT {
		t.Fatalf("delete errno=%v want ENOENT", errno)
	}
	if reports != 0 {
		t.Fatalf("reports=%d want 0", reports)
	}
}

func TestMutationErrorEventClassifiesContextCancellation(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
		want syscall.Errno
	}{
		{name: "deadline", err: context.DeadlineExceeded, want: syscall.EIO},
		{name: "canceled", err: fmt.Errorf("wrapped: %w", context.Canceled), want: syscall.EINTR},
	} {
		t.Run(test.name, func(t *testing.T) {
			reported := make(chan MutationErrorEvent, 1)
			remote := &fakeRemote{memories: map[string]client.Memory{}, createErr: test.err}
			filesystem := NewFileSystem(remote, Options{MutationErrorReporter: func(event MutationErrorEvent) { reported <- event }})
			handle, errno := filesystem.Create(mutationPathSentinel)
			if errno != 0 {
				t.Fatalf("Create errno=%v", errno)
			}
			if errno := filesystem.Commit(context.Background(), handle); errno != test.want {
				t.Fatalf("Commit errno=%v want %v", errno, test.want)
			}
			event := receiveMutationErrorEvent(t, reported)
			if event.Category != "context" || event.Message != "operation canceled" || event.Errno != int(test.want) {
				t.Fatalf("event=%+v", event)
			}
			assertMutationEventSafe(t, event)
		})
	}
}

func TestFUSEInterruptDoesNotReportRemoteError(t *testing.T) {
	reports := 0
	err := fmt.Errorf("remote read stopped: %w", context.Canceled)
	safelyReportRemoteError(func(MutationErrorEvent) { reports++ }, "read", mutationPathSentinel, err, errnoFor(err))
	if reports != 0 {
		t.Fatalf("reports=%d want 0", reports)
	}
}

func TestMutationErrorEventDropsUnsafeServiceIdentifiers(t *testing.T) {
	tests := []struct {
		name          string
		serviceCode   string
		requestID     string
		wantCode      string
		wantRequestID string
	}{
		{name: "overlong service code", serviceCode: strings.Repeat("A", 65), requestID: "req-safe", wantRequestID: "req-safe"},
		{name: "overlong request ID", serviceCode: "INTERNAL_ERROR", requestID: strings.Repeat("r", 129), wantCode: "INTERNAL_ERROR"},
		{name: "control characters", serviceCode: "BAD\nCODE", requestID: "req\tid"},
		{name: "path and credential sentinels", serviceCode: "private/path-sentinel.md", requestID: "credential=secret-sentinel"},
		{name: "non ASCII", serviceCode: "错误码", requestID: "请求编号"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			event := newMutationErrorEvent("create", "safe.md", &client.APIError{
				StatusCode: http.StatusInternalServerError,
				Code:       test.serviceCode,
				RequestID:  test.requestID,
			}, syscall.EIO)
			if event.ServiceCode != test.wantCode || event.RequestID != test.wantRequestID {
				t.Fatalf("identifiers=(%q,%q) want (%q,%q)", event.ServiceCode, event.RequestID, test.wantCode, test.wantRequestID)
			}
			payload, err := json.Marshal(event)
			if err != nil {
				t.Fatal(err)
			}
			if test.wantCode == "" && strings.Contains(string(payload), "service_code") {
				t.Fatalf("unsafe service_code was not omitted: %s", payload)
			}
			if test.wantRequestID == "" && strings.Contains(string(payload), "request_id") {
				t.Fatalf("unsafe request_id was not omitted: %s", payload)
			}
		})
	}
}

func TestMutationErrorReporterSkipsSuccessAndLocalFailures(t *testing.T) {
	t.Run("successful create and clean commit", func(t *testing.T) {
		var reports int
		filesystem := NewFileSystem(&fakeRemote{memories: map[string]client.Memory{}}, Options{MutationErrorReporter: func(MutationErrorEvent) { reports++ }})
		handle, errno := filesystem.Create("success.md")
		if errno != 0 {
			t.Fatalf("Create errno=%v", errno)
		}
		if errno := filesystem.Commit(context.Background(), handle); errno != 0 {
			t.Fatalf("Commit errno=%v", errno)
		}
		if errno := filesystem.Commit(context.Background(), handle); errno != 0 {
			t.Fatalf("clean Commit errno=%v", errno)
		}
		if reports != 0 {
			t.Fatalf("reports=%d want 0", reports)
		}
	})

	t.Run("invalid UTF-8", func(t *testing.T) {
		var reports int
		remote := &fakeRemote{memories: map[string]client.Memory{}}
		filesystem := NewFileSystem(remote, Options{MutationErrorReporter: func(MutationErrorEvent) { reports++ }})
		handle, errno := filesystem.Create("invalid.md")
		if errno != 0 {
			t.Fatalf("Create errno=%v", errno)
		}
		if _, err := handle.WriteAt([]byte{0xff}, 0); err != nil {
			t.Fatal(err)
		}
		if errno := filesystem.Commit(context.Background(), handle); errno != syscall.EILSEQ {
			t.Fatalf("Commit errno=%v want EILSEQ", errno)
		}
		if reports != 0 || remote.callCount("Create") != 0 {
			t.Fatalf("reports=%d remote Create calls=%d want 0,0", reports, remote.callCount("Create"))
		}
	})

	t.Run("oversize write", func(t *testing.T) {
		var reports int
		remote := &fakeRemote{memories: map[string]client.Memory{}}
		filesystem := NewFileSystem(remote, Options{MutationErrorReporter: func(MutationErrorEvent) { reports++ }})
		handle, errno := filesystem.Create("oversize.md")
		if errno != 0 {
			t.Fatalf("Create errno=%v", errno)
		}
		if _, err := handle.WriteAt(make([]byte, MaxContentBytes+1), 0); !errors.Is(err, ErrTooLarge) {
			t.Fatalf("WriteAt err=%v want ErrTooLarge", err)
		}
		// A created handle is dirty even though the oversize write was rejected.
		// Do not call Commit here: that would legitimately submit an empty Create.
		if reports != 0 || remote.callCount("Create") != 0 {
			t.Fatalf("reports=%d remote Create calls=%d want 0,0", reports, remote.callCount("Create"))
		}
	})

	t.Run("read-only before remote call", func(t *testing.T) {
		var reports int
		remote := &fakeRemote{memories: map[string]client.Memory{}}
		filesystem := NewFileSystem(remote, Options{MutationErrorReporter: func(MutationErrorEvent) { reports++ }})
		handle, errno := filesystem.Create("readonly.md")
		if errno != 0 {
			t.Fatalf("Create errno=%v", errno)
		}
		filesystem.serverReadOnly.Store(true)
		if errno := filesystem.Commit(context.Background(), handle); errno != syscall.EROFS {
			t.Fatalf("Commit errno=%v want EROFS", errno)
		}
		if reports != 0 || remote.callCount("Create") != 0 {
			t.Fatalf("reports=%d remote Create calls=%d want 0,0", reports, remote.callCount("Create"))
		}
	})

	t.Run("stale created handle", func(t *testing.T) {
		var reports int
		remote := &fakeRemote{memories: map[string]client.Memory{}}
		filesystem := NewFileSystem(remote, Options{MutationErrorReporter: func(MutationErrorEvent) { reports++ }})
		handle, errno := filesystem.Create("stale.md")
		if errno != 0 {
			t.Fatalf("Create errno=%v", errno)
		}
		filesystem.AbandonCreated(handle)
		if errno := filesystem.Commit(context.Background(), handle); errno != syscall.ESTALE {
			t.Fatalf("Commit errno=%v want ESTALE", errno)
		}
		if reports != 0 || remote.callCount("Create") != 0 {
			t.Fatalf("reports=%d remote Create calls=%d want 0,0", reports, remote.callCount("Create"))
		}
	})
}

type blockingClassificationError struct {
	entered chan struct{}
	resume  chan struct{}
	once    sync.Once
}

func (e *blockingClassificationError) Error() string { return "classification blocked" }

func (e *blockingClassificationError) Is(error) bool {
	e.once.Do(func() {
		close(e.entered)
		<-e.resume
	})
	return false
}

func TestCommitWithoutReporterPreservesPathLockThroughErrnoMapping(t *testing.T) {
	classificationErr := &blockingClassificationError{entered: make(chan struct{}), resume: make(chan struct{})}
	remote := &fakeRemote{memories: map[string]client.Memory{}, createErr: classificationErr}
	filesystem := NewFileSystem(remote, Options{})
	handle, errno := filesystem.Create(mutationPathSentinel)
	if errno != 0 {
		t.Fatalf("Create errno=%v", errno)
	}
	commitDone := make(chan syscall.Errno, 1)
	go func() { commitDone <- filesystem.Commit(context.Background(), handle) }()
	select {
	case <-classificationErr.entered:
	case <-time.After(time.Second):
		t.Fatal("Commit did not enter errno mapping")
	}

	lockAcquired := make(chan struct{})
	go func() {
		unlock := filesystem.pathLocks.Lock(mutationPathSentinel)
		close(lockAcquired)
		unlock()
	}()
	earlyUnlock := false
	select {
	case <-lockAcquired:
		earlyUnlock = true
	case <-time.After(100 * time.Millisecond):
	}
	close(classificationErr.resume)
	select {
	case errno := <-commitDone:
		if errno != syscall.EIO {
			t.Fatalf("Commit errno=%v want EIO", errno)
		}
	case <-time.After(time.Second):
		t.Fatal("Commit did not finish after errno mapping resumed")
	}
	select {
	case <-lockAcquired:
	case <-time.After(time.Second):
		t.Fatal("path lock was not released when Commit returned")
	}
	if earlyUnlock {
		t.Fatal("nil-reporter path released the keyed lock before errno mapping; want parent deferred-unlock semantics")
	}
}

func TestMutationErrorReporterRunsAfterHandleAndPathLocksAreReleased(t *testing.T) {
	var filesystem *FileSystem
	var handle *Handle
	reported := make(chan MutationErrorEvent, 1)
	remote := &fakeRemote{memories: map[string]client.Memory{}, createErr: errors.New("client failure")}
	filesystem = NewFileSystem(remote, Options{MutationErrorReporter: func(event MutationErrorEvent) {
		_ = handle.Memory()
		unlock := filesystem.pathLocks.Lock(mutationPathSentinel)
		unlock()
		reported <- event
	}})
	var errno syscall.Errno
	handle, errno = filesystem.Create(mutationPathSentinel)
	if errno != 0 {
		t.Fatalf("Create errno=%v", errno)
	}
	done := make(chan syscall.Errno, 1)
	go func() { done <- filesystem.Commit(context.Background(), handle) }()
	select {
	case errno := <-done:
		if errno != syscall.EIO {
			t.Fatalf("Commit errno=%v want EIO", errno)
		}
	case <-time.After(time.Second):
		t.Fatal("reporter blocked acquiring Handle.mu or the same keyed path lock")
	}
	receiveMutationErrorEvent(t, reported)
}

func TestMutationErrorReporterPanicDoesNotChangeCommitResult(t *testing.T) {
	tests := []struct {
		name  string
		setup func(t *testing.T, reporter func(MutationErrorEvent)) (*FileSystem, *Handle)
		want  syscall.Errno
	}{
		{
			name: "create",
			setup: func(t *testing.T, reporter func(MutationErrorEvent)) (*FileSystem, *Handle) {
				remote := &fakeRemote{memories: map[string]client.Memory{}, createErr: &client.APIError{StatusCode: http.StatusServiceUnavailable, Code: "UNAVAILABLE"}}
				filesystem := NewFileSystem(remote, Options{MutationErrorReporter: reporter})
				handle, errno := filesystem.Create("panic-create.md")
				if errno != 0 {
					t.Fatalf("Create errno=%v", errno)
				}
				return filesystem, handle
			},
			want: syscall.EAGAIN,
		},
		{
			name: "update",
			setup: func(t *testing.T, reporter func(MutationErrorEvent)) (*FileSystem, *Handle) {
				remote := &updateErrorRemote{
					fakeRemote: &fakeRemote{memories: map[string]client.Memory{
						"panic-update.md": {MemoryID: "mem-panic", Path: "panic-update.md", Content: "old", LatestVersionID: "fmver_1"},
					}},
					err: &client.APIError{StatusCode: http.StatusInternalServerError, Code: "INTERNAL_ERROR"},
				}
				filesystem := NewFileSystem(remote, Options{MutationErrorReporter: reporter})
				handle, errno := filesystem.Open(context.Background(), "panic-update.md")
				if errno != 0 {
					t.Fatalf("Open errno=%v", errno)
				}
				if _, err := handle.WriteAt([]byte("new"), 0); err != nil {
					t.Fatal(err)
				}
				return filesystem, handle
			},
			want: syscall.EIO,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reports := 0
			filesystem, handle := test.setup(t, func(MutationErrorEvent) {
				reports++
				panic("reporter-panic-sentinel")
			})
			got, panicValue := commitRecoveringPanic(filesystem, handle)
			if panicValue != nil {
				t.Fatalf("Commit propagated reporter panic: %v", panicValue)
			}
			if got != test.want || reports != 1 {
				t.Fatalf("Commit errno=%v reports=%d want errno=%v reports=1", got, reports, test.want)
			}
		})
	}
}

func commitRecoveringPanic(filesystem *FileSystem, handle *Handle) (errno syscall.Errno, panicValue any) {
	defer func() { panicValue = recover() }()
	errno = filesystem.Commit(context.Background(), handle)
	return errno, nil
}

func receiveMutationErrorEvent(t *testing.T, events <-chan MutationErrorEvent) MutationErrorEvent {
	t.Helper()
	select {
	case event := <-events:
		return event
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for mutation error event")
		return MutationErrorEvent{}
	}
}

func assertMutationEventSafe(t *testing.T, event MutationErrorEvent) {
	t.Helper()
	payload, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	text := string(payload)
	for _, forbidden := range []string{
		mutationPathSentinel,
		mutationMessageSentinel,
		"wrapped-client-detail-sentinel",
		"credential-sentinel",
		"endpoint-sentinel",
		"content-sentinel",
	} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("event JSON leaked %q: %s", forbidden, text)
		}
	}
	var fields map[string]any
	if err := json.Unmarshal(payload, &fields); err != nil {
		t.Fatal(err)
	}
	allowed := map[string]bool{
		"event": true, "operation": true, "class": true, "reason": true,
		"path_sha256": true, "category": true, "errno": true,
		"http_status": true, "service_code": true, "request_id": true, "sdk_kind": true, "message": true,
	}
	for field := range fields {
		if !allowed[field] {
			t.Fatalf("unsafe event field %q in %s", field, text)
		}
	}
}

var _ Remote = (*updateErrorRemote)(nil)

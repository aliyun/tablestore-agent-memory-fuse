package fs

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"agent-memory-fuse/internal/client"

	gofusefs "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// fakeRemote 记录调用序列（calls），供断言 Create/Update 分流等协议行为。
type fakeRemote struct {
	memories              map[string]client.Memory
	calls                 []string
	mu                    sync.Mutex
	lastReadPath          string
	lastItemID            string
	lastExpectedVersionID string
	listLimits            []int
	listTokens            []string
	listPages             map[string]client.ListResult
	createErr             error
	nextID                int
	nextVersion           int
	readOnly              bool // 服务端只读握手标志（file+ots 投影库）
	listPauses            []listPause
}

type listPause struct {
	ready  chan struct{}
	resume chan struct{}
}

type blockingReadRemote struct {
	*fakeRemote

	blockMu        sync.Mutex
	readsToBlock   int
	readStarted    chan string
	readCanProceed chan struct{}
}

type blockingStatRemote struct {
	*fakeRemote

	blockMu       sync.Mutex
	statsToBlock  int
	statStarted   chan string
	statCanFinish chan struct{}
}

func (r *blockingStatRemote) pauseNextStats(count int) (<-chan string, chan<- struct{}) {
	r.blockMu.Lock()
	defer r.blockMu.Unlock()
	r.statsToBlock = count
	r.statStarted = make(chan string, count)
	r.statCanFinish = make(chan struct{})
	return r.statStarted, r.statCanFinish
}

func (r *blockingStatRemote) Stat(ctx context.Context, value string) (client.Memory, error) {
	r.fakeRemote.mu.Lock()
	r.fakeRemote.calls = append(r.fakeRemote.calls, "Stat")
	memory, ok := r.fakeRemote.memories[value]
	r.fakeRemote.mu.Unlock()

	r.blockMu.Lock()
	shouldBlock := r.statsToBlock > 0
	started, finish := r.statStarted, r.statCanFinish
	if shouldBlock {
		r.statsToBlock--
	}
	r.blockMu.Unlock()
	if shouldBlock {
		started <- value
		select {
		case <-finish:
		case <-ctx.Done():
			return client.Memory{}, ctx.Err()
		}
	}
	if !ok {
		return client.Memory{}, &client.APIError{StatusCode: http.StatusNotFound, Code: "NOT_FOUND"}
	}
	memory.Content = ""
	return memory, nil
}

type statErrorRemote struct {
	*fakeRemote
	err error
}

func (r *statErrorRemote) Stat(_ context.Context, _ string) (client.Memory, error) {
	r.fakeRemote.mu.Lock()
	r.fakeRemote.calls = append(r.fakeRemote.calls, "Stat")
	r.fakeRemote.mu.Unlock()
	return client.Memory{}, r.err
}

func (r *blockingReadRemote) pauseNextReads(count int) (<-chan string, chan<- struct{}) {
	r.blockMu.Lock()
	defer r.blockMu.Unlock()
	r.readsToBlock = count
	r.readStarted = make(chan string, count)
	r.readCanProceed = make(chan struct{})
	return r.readStarted, r.readCanProceed
}

func (r *blockingReadRemote) Read(ctx context.Context, value string) (client.Memory, error) {
	r.fakeRemote.mu.Lock()
	r.fakeRemote.calls = append(r.fakeRemote.calls, "Read")
	r.fakeRemote.lastReadPath = value
	memory, ok := r.fakeRemote.memories[value]
	r.fakeRemote.mu.Unlock()
	if !ok {
		return client.Memory{}, &client.APIError{StatusCode: 404, Code: "NOT_FOUND"}
	}

	r.blockMu.Lock()
	shouldBlock := r.readsToBlock > 0
	started, proceed := r.readStarted, r.readCanProceed
	if shouldBlock {
		r.readsToBlock--
	}
	r.blockMu.Unlock()
	if shouldBlock {
		started <- value
		select {
		case <-proceed:
		case <-ctx.Done():
			return client.Memory{}, ctx.Err()
		}
	}
	return memory, nil
}

type blockingMutationRemote struct {
	*fakeRemote

	blockMu           sync.Mutex
	mutationsToBlock  int
	mutationStarted   chan string
	mutationCanFinish chan struct{}
}

// interruptMutationRemote pauses after the filesystem has entered the selected
// remote mutation. The test cancels the FUSE request context before releasing
// the call, then this remote verifies the mutation context itself is still live.
type interruptMutationRemote struct {
	*fakeRemote
	operation  string
	started    chan string
	release    chan struct{}
	contextErr chan error
}

type timeoutMutationRemote struct {
	*fakeRemote
	operation  string
	started    chan string
	contextErr chan error
	abort      chan struct{}
}

func newTimeoutMutationRemote(remote *fakeRemote, operation string) *timeoutMutationRemote {
	return &timeoutMutationRemote{
		fakeRemote: remote,
		operation:  operation,
		started:    make(chan string, 1),
		contextErr: make(chan error, 1),
		abort:      make(chan struct{}),
	}
}

func (r *timeoutMutationRemote) awaitDeadline(ctx context.Context, operation string) error {
	if r.operation != operation {
		return nil
	}
	r.started <- operation
	var err error
	select {
	case <-ctx.Done():
		err = ctx.Err()
	case <-r.abort:
		err = errors.New("timeout mutation test aborted")
	}
	r.contextErr <- err
	return err
}

func (r *timeoutMutationRemote) Create(ctx context.Context, value string, content []byte) (client.Memory, error) {
	if err := r.awaitDeadline(ctx, "Create"); err != nil {
		return client.Memory{}, err
	}
	return r.fakeRemote.Create(ctx, value, content)
}

func (r *timeoutMutationRemote) Update(ctx context.Context, itemID string, content []byte, expectedVersionID string) (client.Memory, error) {
	if err := r.awaitDeadline(ctx, "Update"); err != nil {
		return client.Memory{}, err
	}
	return r.fakeRemote.Update(ctx, itemID, content, expectedVersionID)
}

func (r *timeoutMutationRemote) Rename(ctx context.Context, itemID, to string, overwrite bool, expectedVersionID string) (client.Memory, error) {
	if err := r.awaitDeadline(ctx, "Rename"); err != nil {
		return client.Memory{}, err
	}
	return r.fakeRemote.Rename(ctx, itemID, to, overwrite, expectedVersionID)
}

func (r *timeoutMutationRemote) Delete(ctx context.Context, itemID, expectedVersionID string) error {
	if err := r.awaitDeadline(ctx, "Delete"); err != nil {
		return err
	}
	return r.fakeRemote.Delete(ctx, itemID, expectedVersionID)
}

func newInterruptMutationRemote(remote *fakeRemote, operation string) *interruptMutationRemote {
	return &interruptMutationRemote{
		fakeRemote: remote,
		operation:  operation,
		started:    make(chan string, 1),
		release:    make(chan struct{}),
		contextErr: make(chan error, 1),
	}
}

func (r *interruptMutationRemote) awaitRelease(ctx context.Context, operation string) error {
	if r.operation != operation {
		return nil
	}
	r.started <- operation
	<-r.release
	err := ctx.Err()
	r.contextErr <- err
	return err
}

func (r *interruptMutationRemote) Create(ctx context.Context, value string, content []byte) (client.Memory, error) {
	if err := r.awaitRelease(ctx, "Create"); err != nil {
		return client.Memory{}, err
	}
	return r.fakeRemote.Create(ctx, value, content)
}

func (r *interruptMutationRemote) Update(ctx context.Context, itemID string, content []byte, expectedVersionID string) (client.Memory, error) {
	if err := r.awaitRelease(ctx, "Update"); err != nil {
		return client.Memory{}, err
	}
	return r.fakeRemote.Update(ctx, itemID, content, expectedVersionID)
}

func (r *interruptMutationRemote) Rename(ctx context.Context, itemID, to string, overwrite bool, expectedVersionID string) (client.Memory, error) {
	if err := r.awaitRelease(ctx, "Rename"); err != nil {
		return client.Memory{}, err
	}
	return r.fakeRemote.Rename(ctx, itemID, to, overwrite, expectedVersionID)
}

func (r *interruptMutationRemote) Delete(ctx context.Context, itemID, expectedVersionID string) error {
	if err := r.awaitRelease(ctx, "Delete"); err != nil {
		return err
	}
	return r.fakeRemote.Delete(ctx, itemID, expectedVersionID)
}

type ambiguousCreateRemote struct {
	*fakeRemote
}

func (r *ambiguousCreateRemote) Create(ctx context.Context, value string, content []byte) (client.Memory, error) {
	if _, err := r.fakeRemote.Create(ctx, value, content); err != nil {
		return client.Memory{}, err
	}
	return client.Memory{}, &client.APIError{StatusCode: http.StatusServiceUnavailable, Code: "UNAVAILABLE"}
}

type ambiguousCreateOnceRemote struct {
	*fakeRemote
	once sync.Once
}

func (r *ambiguousCreateOnceRemote) Create(ctx context.Context, value string, content []byte) (client.Memory, error) {
	memory, err := r.fakeRemote.Create(ctx, value, content)
	if err != nil {
		return client.Memory{}, err
	}
	ambiguous := false
	r.once.Do(func() { ambiguous = true })
	if ambiguous {
		return client.Memory{}, &client.APIError{StatusCode: http.StatusServiceUnavailable, Code: "UNAVAILABLE"}
	}
	return memory, nil
}

type queuedRefreshScheduler struct {
	mu    sync.Mutex
	tasks []func()
}

type refreshHandoffListStep struct {
	err           error
	afterSnapshot func()
}

type refreshHandoffRemote struct {
	*fakeRemote

	stepMu sync.Mutex
	steps  []refreshHandoffListStep
}

func (r *refreshHandoffRemote) queueListSteps(steps ...refreshHandoffListStep) {
	r.stepMu.Lock()
	defer r.stepMu.Unlock()
	r.steps = append(r.steps, steps...)
}

func (r *refreshHandoffRemote) List(ctx context.Context, pathPrefix, nextToken string, limit int) (client.ListResult, error) {
	page, err := r.fakeRemote.List(ctx, pathPrefix, nextToken, limit)
	if err != nil {
		return client.ListResult{}, err
	}
	r.stepMu.Lock()
	var step refreshHandoffListStep
	if len(r.steps) > 0 {
		step = r.steps[0]
		r.steps = r.steps[1:]
	}
	r.stepMu.Unlock()
	if step.afterSnapshot != nil {
		step.afterSnapshot()
	}
	if step.err != nil {
		return client.ListResult{}, step.err
	}
	return page, nil
}

func (s *queuedRefreshScheduler) schedule(task func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tasks = append(s.tasks, task)
}

func (s *queuedRefreshScheduler) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.tasks)
}

func (s *queuedRefreshScheduler) take(t *testing.T) func() {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.tasks) == 0 {
		t.Fatal("no background refresh task was scheduled")
	}
	task := s.tasks[0]
	s.tasks = s.tasks[1:]
	return task
}

func (r *blockingMutationRemote) pauseNextUpdates(count int) (<-chan string, chan<- struct{}) {
	r.blockMu.Lock()
	defer r.blockMu.Unlock()
	r.mutationsToBlock = count
	r.mutationStarted = make(chan string, count)
	r.mutationCanFinish = make(chan struct{})
	return r.mutationStarted, r.mutationCanFinish
}

func (r *blockingMutationRemote) Update(ctx context.Context, itemID string, content []byte, expectedVersionID string) (client.Memory, error) {
	r.blockMu.Lock()
	shouldBlock := r.mutationsToBlock > 0
	started, finish := r.mutationStarted, r.mutationCanFinish
	if shouldBlock {
		r.mutationsToBlock--
	}
	r.blockMu.Unlock()
	if shouldBlock {
		started <- itemID
		select {
		case <-finish:
		case <-ctx.Done():
			return client.Memory{}, ctx.Err()
		}
	}
	return r.fakeRemote.Update(ctx, itemID, content, expectedVersionID)
}

func cloneListResult(page client.ListResult) client.ListResult {
	page.Memories = append([]client.Memory(nil), page.Memories...)
	return page
}

func (r *fakeRemote) pauseNextList() (<-chan struct{}, chan<- struct{}) {
	r.mu.Lock()
	defer r.mu.Unlock()
	ready := make(chan struct{})
	resume := make(chan struct{})
	r.listPauses = append(r.listPauses, listPause{ready: ready, resume: resume})
	return ready, resume
}

func (r *fakeRemote) pauseNextLists(count int) ([]<-chan struct{}, []chan<- struct{}) {
	r.mu.Lock()
	defer r.mu.Unlock()
	ready := make([]<-chan struct{}, 0, count)
	resume := make([]chan<- struct{}, 0, count)
	for range count {
		pause := listPause{ready: make(chan struct{}), resume: make(chan struct{})}
		r.listPauses = append(r.listPauses, pause)
		ready = append(ready, pause.ready)
		resume = append(resume, pause.resume)
	}
	return ready, resume
}

func (r *fakeRemote) List(ctx context.Context, _ string, nextToken string, limit int) (client.ListResult, error) {
	r.mu.Lock()
	r.calls = append(r.calls, "List")
	r.listLimits = append(r.listLimits, limit)
	r.listTokens = append(r.listTokens, nextToken)
	var page client.ListResult
	if r.listPages != nil {
		page = cloneListResult(r.listPages[nextToken])
	} else {
		out := make([]client.Memory, 0, len(r.memories))
		for _, memory := range r.memories {
			memory.Content = ""
			out = append(out, memory)
		}
		page = client.ListResult{Memories: out, ReadOnly: r.readOnly}
	}
	var pause listPause
	if len(r.listPauses) > 0 {
		pause = r.listPauses[0]
		r.listPauses = r.listPauses[1:]
	}
	r.mu.Unlock()
	if pause.ready != nil {
		close(pause.ready)
		select {
		case <-pause.resume:
		case <-ctx.Done():
			return client.ListResult{}, ctx.Err()
		}
	}
	return cloneListResult(page), nil
}
func (r *fakeRemote) Stat(_ context.Context, path string) (client.Memory, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, "Stat")
	memory, ok := r.memories[path]
	if !ok {
		return client.Memory{}, &client.APIError{StatusCode: 404, Code: "NOT_FOUND"}
	}
	memory.Content = ""
	return memory, nil
}

func (r *fakeRemote) callCount(operation string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	count := 0
	for _, call := range r.calls {
		if call == operation {
			count++
		}
	}
	return count
}
func (r *fakeRemote) Read(_ context.Context, path string) (client.Memory, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, "Read")
	r.lastReadPath = path
	memory, ok := r.memories[path]
	if !ok {
		return client.Memory{}, &client.APIError{StatusCode: 404, Code: "NOT_FOUND"}
	}
	return memory, nil
}
func (r *fakeRemote) Create(_ context.Context, path string, content []byte) (client.Memory, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, "Create")
	if r.createErr != nil {
		return client.Memory{}, r.createErr
	}
	if _, exists := r.memories[path]; exists {
		return client.Memory{}, &client.APIError{StatusCode: 409, Code: "PATH_EXISTS"}
	}
	r.nextID++
	memory := client.Memory{
		MemoryID:         fmt.Sprintf("created-%d", r.nextID),
		Path:             path,
		Content:          string(content),
		ContentSizeBytes: int64(len(content)),
		ContentSHA256:    "sha-created",
		LatestVersionID:  r.nextVersionIDLocked(),
	}
	r.memories[path] = memory
	return memory, nil
}

func (r *fakeRemote) findByID(itemID string) (string, client.Memory, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.findByIDLocked(itemID)
}

func (r *fakeRemote) findByIDLocked(itemID string) (string, client.Memory, bool) {
	for path, memory := range r.memories {
		if memory.MemoryID == itemID {
			return path, memory, true
		}
	}
	return "", client.Memory{}, false
}

func (r *fakeRemote) nextVersionIDLocked() string {
	for _, memory := range r.memories {
		var version int
		if _, err := fmt.Sscanf(memory.LatestVersionID, "fmver_%d", &version); err == nil && version > r.nextVersion {
			r.nextVersion = version
		}
	}
	r.nextVersion++
	return fmt.Sprintf("fmver_%d", r.nextVersion)
}

func (r *fakeRemote) Update(_ context.Context, itemID string, content []byte, expectedVersionID string) (client.Memory, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, "Update")
	r.lastItemID = itemID
	r.lastExpectedVersionID = expectedVersionID
	path, memory, ok := r.findByIDLocked(itemID)
	if !ok {
		return client.Memory{}, &client.APIError{StatusCode: 404, Code: "NOT_FOUND"}
	}
	if expectedVersionID != memory.LatestVersionID {
		return client.Memory{}, &client.APIError{StatusCode: 409, Code: "PRECONDITION_FAILED"}
	}
	memory.Content, memory.ContentSizeBytes = string(content), int64(len(content))
	memory.LatestVersionID = r.nextVersionIDLocked()
	memory.ContentSHA256 = "sha-" + memory.LatestVersionID
	r.memories[path] = memory
	return memory, nil
}
func (r *fakeRemote) Rename(_ context.Context, itemID, to string, overwrite bool, expectedVersionID string) (client.Memory, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, "Rename")
	r.lastItemID = itemID
	r.lastExpectedVersionID = expectedVersionID
	from, source, ok := r.findByIDLocked(itemID)
	if !ok {
		return client.Memory{}, &client.APIError{StatusCode: 404, Code: "NOT_FOUND"}
	}
	if expectedVersionID != source.LatestVersionID {
		return client.Memory{}, &client.APIError{StatusCode: 409, Code: "PRECONDITION_FAILED"}
	}
	if target, exists := r.memories[to]; exists && target.MemoryID != itemID && !overwrite {
		return client.Memory{}, &client.APIError{StatusCode: 409, Code: "PATH_EXISTS"}
	}
	nextVersionID := r.nextVersionIDLocked()
	delete(r.memories, from)
	source.Path = to
	source.LatestVersionID = nextVersionID
	r.memories[to] = source
	return source, nil
}
func (r *fakeRemote) Delete(_ context.Context, itemID, expectedVersionID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, "Delete")
	r.lastItemID = itemID
	r.lastExpectedVersionID = expectedVersionID
	path, memory, ok := r.findByIDLocked(itemID)
	if !ok {
		return &client.APIError{StatusCode: 404, Code: "NOT_FOUND"}
	}
	if expectedVersionID != memory.LatestVersionID {
		return &client.APIError{StatusCode: 409, Code: "PRECONDITION_FAILED"}
	}
	delete(r.memories, path)
	return nil
}

func TestPendingCreateMutationsNeverSendEmptyItemID(t *testing.T) {
	for _, reporterEnabled := range []bool{false, true} {
		reporterName := fmt.Sprintf("reporter=%v", reporterEnabled)
		newOptions := func() Options {
			if !reporterEnabled {
				return Options{}
			}
			return Options{MutationErrorReporter: func(MutationErrorEvent) {}}
		}

		t.Run(reporterName+"/delete", func(t *testing.T) {
			remote := &fakeRemote{memories: map[string]client.Memory{}}
			filesystem := NewFileSystem(remote, newOptions())
			handle, errno := filesystem.Create("a.md")
			if errno != 0 {
				t.Fatalf("Create errno=%v", errno)
			}
			if _, err := handle.WriteAt([]byte("must-not-resurrect"), 0); err != nil {
				t.Fatal(err)
			}
			if errno := filesystem.Delete(context.Background(), "a.md"); errno != 0 {
				t.Fatalf("Delete errno=%v", errno)
			}
			if remote.callCount("Delete") != 0 || remote.callCount("Create") != 0 {
				t.Fatalf("remote calls=%v", remote.calls)
			}
			if _, ok := filesystem.Index().Lookup("a.md"); ok {
				t.Fatal("placeholder survived Delete")
			}
			filesystem.stateMu.Lock()
			_, pending := filesystem.pendingCreated["a.md"]
			filesystem.stateMu.Unlock()
			if pending {
				t.Fatal("pending owner survived Delete")
			}
			if errno := filesystem.Commit(context.Background(), handle); errno != syscall.ESTALE {
				t.Fatalf("late Commit errno=%v want ESTALE", errno)
			}
			if remote.callCount("Create") != 0 {
				t.Fatal("deleted pending file resurrected remotely")
			}
		})

		t.Run(reporterName+"/rename", func(t *testing.T) {
			remote := &fakeRemote{memories: map[string]client.Memory{}}
			filesystem := NewFileSystem(remote, newOptions())
			handle, errno := filesystem.Create("a.md")
			if errno != 0 {
				t.Fatalf("Create errno=%v", errno)
			}
			if _, err := handle.WriteAt([]byte("renamed-before-flush"), 0); err != nil {
				t.Fatal(err)
			}
			if errno := filesystem.Rename(context.Background(), "a.md", "b.md", false); errno != 0 {
				t.Fatalf("Rename errno=%v", errno)
			}
			if remote.callCount("Rename") != 0 {
				t.Fatalf("remote Rename calls=%d", remote.callCount("Rename"))
			}
			if got := handle.Memory().Path; got != "b.md" {
				t.Fatalf("handle path=%q want b.md", got)
			}
			if _, ok := filesystem.Index().Lookup("a.md"); ok {
				t.Fatal("source placeholder survived Rename")
			}
			if node, ok := filesystem.Index().Lookup("b.md"); !ok || node.MemoryID != "" {
				t.Fatalf("target placeholder=%+v ok=%v", node, ok)
			}
			if errno := filesystem.Commit(context.Background(), handle); errno != 0 {
				t.Fatalf("Commit errno=%v", errno)
			}
			if remote.callCount("Create") != 1 || remote.callCount("Rename") != 0 {
				t.Fatalf("remote calls=%v", remote.calls)
			}
			remote.mu.Lock()
			content := remote.memories["b.md"].Content
			remote.mu.Unlock()
			if content != "renamed-before-flush" {
				t.Fatalf("content=%q", content)
			}
		})

		t.Run(reporterName+"/occupied", func(t *testing.T) {
			remote := &fakeRemote{memories: map[string]client.Memory{
				"b.md": {MemoryID: "mem_b", Path: "b.md", Content: "existing", LatestVersionID: "fmver_1"},
			}}
			filesystem := NewFileSystem(remote, newOptions())
			if err := filesystem.Refresh(context.Background()); err != nil {
				t.Fatal(err)
			}
			if _, errno := filesystem.Create("a.md"); errno != 0 {
				t.Fatalf("Create errno=%v", errno)
			}
			if errno := filesystem.Rename(context.Background(), "a.md", "b.md", true); errno != syscall.EEXIST {
				t.Fatalf("Rename errno=%v want EEXIST", errno)
			}
			if remote.callCount("Rename") != 0 {
				t.Fatalf("remote Rename calls=%d", remote.callCount("Rename"))
			}
		})
	}
}

func TestFileSystemCreateNormalizesNFDBeforeRemoteMutation(t *testing.T) {
	const nfd = "cafe\u0301.md"
	const nfc = "caf\u00e9.md"
	remote := &fakeRemote{memories: map[string]client.Memory{}}
	filesystem := NewFileSystem(remote, Options{})

	handle, errno := filesystem.Create(nfd)
	if errno != 0 {
		t.Fatalf("Create errno=%v", errno)
	}
	if _, err := handle.WriteAt([]byte("unicode"), 0); err != nil {
		t.Fatal(err)
	}
	if errno := filesystem.Commit(context.Background(), handle); errno != 0 {
		t.Fatalf("Commit errno=%v", errno)
	}
	if _, ok := remote.memories[nfc]; !ok {
		t.Fatalf("remote paths=%v want NFC %q", remote.memories, nfc)
	}
	if _, ok := remote.memories[nfd]; ok {
		t.Fatalf("remote retained NFD path %q", nfd)
	}
	if node, ok := filesystem.Index().Lookup(nfd); !ok || node.Path != nfc {
		t.Fatalf("NFD lookup node=%+v ok=%v want canonical path %q", node, ok, nfc)
	}
}

func TestFileSystemRenameAndDeleteNormalizeNFDPaths(t *testing.T) {
	const sourceNFC = "sourc\u00e9.md"
	const sourceNFD = "source\u0301.md"
	const targetNFC = "destin\u00e9.md"
	const targetNFD = "destine\u0301.md"
	remote := &fakeRemote{memories: map[string]client.Memory{
		sourceNFC: {MemoryID: "mem_unicode", Path: sourceNFC, Content: "unicode", LatestVersionID: "fmver_1"},
	}}
	filesystem := NewFileSystem(remote, Options{})
	if err := filesystem.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}

	if errno := filesystem.Rename(context.Background(), sourceNFD, targetNFD, false); errno != 0 {
		t.Fatalf("Rename errno=%v", errno)
	}
	if _, ok := remote.memories[targetNFC]; !ok {
		t.Fatalf("remote paths after Rename=%v want NFC %q", remote.memories, targetNFC)
	}
	if errno := filesystem.Delete(context.Background(), targetNFD); errno != 0 {
		t.Fatalf("Delete errno=%v", errno)
	}
	if len(remote.memories) != 0 {
		t.Fatalf("remote paths after Delete=%v want empty", remote.memories)
	}
}

type externallyRenamedUpdateRemote struct {
	*fakeRemote
	authoritative client.Memory
}

type delayedMovedUpdateRemote struct {
	*fakeRemote

	controlMu       sync.Mutex
	updateCount     int
	failLaterUpdate bool
	applied         chan client.Memory
	release         chan struct{}
	releaseOnce     sync.Once
}

type ambiguousCreateDelayedMovedUpdateRemote struct {
	*delayedMovedUpdateRemote
}

func newAmbiguousCreateDelayedMovedUpdateRemote() *ambiguousCreateDelayedMovedUpdateRemote {
	remote := newDelayedMovedUpdateRemote(false)
	remote.fakeRemote.memories = map[string]client.Memory{}
	return &ambiguousCreateDelayedMovedUpdateRemote{delayedMovedUpdateRemote: remote}
}

func (r *ambiguousCreateDelayedMovedUpdateRemote) Create(ctx context.Context, value string, content []byte) (client.Memory, error) {
	if _, err := r.fakeRemote.Create(ctx, value, content); err != nil {
		return client.Memory{}, err
	}
	return client.Memory{}, &client.APIError{StatusCode: http.StatusServiceUnavailable, Code: "UNAVAILABLE"}
}

func newDelayedMovedUpdateRemote(failLaterUpdate bool) *delayedMovedUpdateRemote {
	return &delayedMovedUpdateRemote{
		fakeRemote: &fakeRemote{memories: map[string]client.Memory{
			"a.md": {
				MemoryID: "mem_a", Path: "a.md", Content: "initial", ContentSHA256: "sha-fmver_1",
				ContentSizeBytes: 7, LatestVersionID: "fmver_1",
			},
		}},
		failLaterUpdate: failLaterUpdate,
		applied:         make(chan client.Memory, 1),
		release:         make(chan struct{}),
	}
}

func (r *delayedMovedUpdateRemote) releaseOldResponse() {
	r.releaseOnce.Do(func() { close(r.release) })
}

func (r *delayedMovedUpdateRemote) Update(ctx context.Context, itemID string, content []byte, expectedVersionID string) (client.Memory, error) {
	r.controlMu.Lock()
	r.updateCount++
	updateCount := r.updateCount
	failLaterUpdate := r.failLaterUpdate
	r.controlMu.Unlock()
	if updateCount > 1 {
		if failLaterUpdate {
			r.fakeRemote.mu.Lock()
			r.fakeRemote.calls = append(r.fakeRemote.calls, "Update")
			r.fakeRemote.lastItemID = itemID
			r.fakeRemote.lastExpectedVersionID = expectedVersionID
			r.fakeRemote.mu.Unlock()
			return client.Memory{}, &client.APIError{StatusCode: http.StatusConflict, Code: "PRECONDITION_FAILED"}
		}
		return r.fakeRemote.Update(ctx, itemID, content, expectedVersionID)
	}

	r.fakeRemote.mu.Lock()
	r.fakeRemote.calls = append(r.fakeRemote.calls, "Update")
	r.fakeRemote.lastItemID = itemID
	r.fakeRemote.lastExpectedVersionID = expectedVersionID
	oldPath, memory, ok := r.fakeRemote.findByIDLocked(itemID)
	if !ok {
		r.fakeRemote.mu.Unlock()
		return client.Memory{}, &client.APIError{StatusCode: http.StatusNotFound, Code: "NOT_FOUND"}
	}
	if memory.LatestVersionID != expectedVersionID {
		r.fakeRemote.mu.Unlock()
		return client.Memory{}, &client.APIError{StatusCode: http.StatusConflict, Code: "PRECONDITION_FAILED"}
	}
	nextVersionID := r.fakeRemote.nextVersionIDLocked()
	delete(r.fakeRemote.memories, oldPath)
	memory.Path = "b.md"
	memory.Content = string(content)
	memory.ContentSizeBytes = int64(len(content))
	memory.LatestVersionID = nextVersionID
	memory.ContentSHA256 = "sha-" + memory.LatestVersionID
	r.fakeRemote.memories["b.md"] = memory
	r.fakeRemote.mu.Unlock()

	r.applied <- memory
	select {
	case <-r.release:
		return memory, nil
	case <-ctx.Done():
		return client.Memory{}, ctx.Err()
	}
}

type delayedCreateRemote struct {
	*fakeRemote
	applied     chan client.Memory
	release     chan struct{}
	releaseOnce sync.Once
}

type reverseReleaseUpdateRemote struct {
	*fakeRemote
	started  chan string
	releases map[string]chan struct{}
}

func (r *reverseReleaseUpdateRemote) Update(ctx context.Context, itemID string, content []byte, expectedVersionID string) (client.Memory, error) {
	memory, err := r.fakeRemote.Update(ctx, itemID, content, expectedVersionID)
	if err != nil {
		return client.Memory{}, err
	}
	r.started <- itemID
	select {
	case <-r.releases[itemID]:
		return memory, nil
	case <-ctx.Done():
		return client.Memory{}, ctx.Err()
	}
}

func newDelayedCreateRemote() *delayedCreateRemote {
	return &delayedCreateRemote{
		fakeRemote: &fakeRemote{memories: map[string]client.Memory{}},
		applied:    make(chan client.Memory, 1),
		release:    make(chan struct{}),
	}
}

func (r *delayedCreateRemote) releaseOldResponse() {
	r.releaseOnce.Do(func() { close(r.release) })
}

func (r *delayedCreateRemote) Create(ctx context.Context, path string, content []byte) (client.Memory, error) {
	memory, err := r.fakeRemote.Create(ctx, path, content)
	if err != nil {
		return client.Memory{}, err
	}
	r.fakeRemote.mu.Lock()
	delete(r.fakeRemote.memories, path)
	memory.Path = "b.md"
	r.fakeRemote.memories["b.md"] = memory
	r.fakeRemote.mu.Unlock()
	r.applied <- memory
	select {
	case <-r.release:
		return memory, nil
	case <-ctx.Done():
		return client.Memory{}, ctx.Err()
	}
}

func receiveFSWithin[T any](t *testing.T, ch <-chan T, description string) T {
	t.Helper()
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	select {
	case value := <-ch:
		return value
	case <-timer.C:
		t.Fatalf("timed out waiting for %s", description)
		var zero T
		return zero
	}
}

func (r *externallyRenamedUpdateRemote) Update(_ context.Context, itemID string, content []byte, expectedVersionID string) (client.Memory, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, "Update")
	r.lastItemID = itemID
	r.lastExpectedVersionID = expectedVersionID
	oldPath, current, ok := r.findByIDLocked(itemID)
	if !ok {
		return client.Memory{}, &client.APIError{StatusCode: http.StatusNotFound, Code: "NOT_FOUND"}
	}
	if current.LatestVersionID != expectedVersionID {
		return client.Memory{}, &client.APIError{StatusCode: http.StatusConflict, Code: "PRECONDITION_FAILED"}
	}
	authoritative := r.authoritative
	authoritative.Content = string(content)
	authoritative.ContentSizeBytes = int64(len(content))
	delete(r.memories, oldPath)
	r.memories[strings.Trim(authoritative.Path, "/")] = authoritative
	return authoritative, nil
}

func TestCommitUpdateReplacesPathWithAuthoritativeExternalRename(t *testing.T) {
	for _, withReporter := range []bool{false, true} {
		t.Run(fmt.Sprintf("reporter=%t", withReporter), func(t *testing.T) {
			base := &fakeRemote{memories: map[string]client.Memory{
				"a.md": {
					MemoryID: "mem_a", Path: "a.md", Content: "old", ContentSHA256: "sha-old", ContentSizeBytes: 3,
					LatestSeq: 1, LatestVersionID: "opaque-before",
				},
			}}
			remote := &externallyRenamedUpdateRemote{
				fakeRemote: base,
				authoritative: client.Memory{
					MemoryID: "mem_a", Path: "/external.md", ContentSHA256: "sha-new", LatestSeq: 2, LatestVersionID: "opaque-after",
				},
			}
			filesystem := NewFileSystem(remote, mutationTestOptions(withReporter))
			if err := filesystem.Refresh(context.Background()); err != nil {
				t.Fatal(err)
			}
			if errno := filesystem.Mkdir("local-dir"); errno != 0 {
				t.Fatalf("Mkdir errno=%v", errno)
			}
			handle, errno := filesystem.Open(context.Background(), "a.md")
			if errno != 0 {
				t.Fatalf("Open errno=%v", errno)
			}
			// Simulate a stale snapshot that already exposed the same stable item
			// identity at another path before the authoritative Update returns.
			filesystem.Index().Upsert(client.Memory{
				MemoryID: "mem_a", Path: "stale/duplicate.md", ContentSHA256: "sha-stale", LatestVersionID: "opaque-stale",
			})
			if err := handle.Truncate(0); err != nil {
				t.Fatal(err)
			}
			if _, err := handle.WriteAt([]byte("new"), 0); err != nil {
				t.Fatal(err)
			}
			if errno := filesystem.Commit(context.Background(), handle); errno != 0 {
				t.Fatalf("Commit errno=%v", errno)
			}

			if _, exists := filesystem.Index().Lookup("a.md"); exists {
				t.Fatal("authoritative Update left old path in index")
			}
			if _, exists := filesystem.Index().Lookup("stale/duplicate.md"); exists {
				t.Fatal("authoritative Update left duplicate item identity in index")
			}
			node, exists := filesystem.Index().Lookup("external.md")
			if !exists || node.MemoryID != "mem_a" || node.LatestVersionID != "opaque-after" || node.SHA256 != "sha-new" {
				t.Fatalf("authoritative node=%+v exists=%v", node, exists)
			}
			if localDir, exists := filesystem.Index().Lookup("local-dir"); !exists || !localDir.IsDir {
				t.Fatalf("local dir=%+v exists=%v", localDir, exists)
			}
			if got := handle.Memory(); got.Path != "/external.md" || got.MemoryID != "mem_a" || got.LatestVersionID != "opaque-after" || got.Content != "new" {
				t.Fatalf("handle memory=%+v", got)
			}

			filesystem.Index().mu.RLock()
			defer filesystem.Index().mu.RUnlock()
			identityCount := 0
			for _, indexed := range filesystem.Index().nodes {
				if !indexed.IsDir && indexed.MemoryID == "mem_a" {
					identityCount++
				}
			}
			if identityCount != 1 {
				t.Fatalf("itemID mem_a occurs %d times in index, want 1", identityCount)
			}
		})
	}
}

func TestOlderCrossPathUpdateResponseCannotRollBackNewerMutation(t *testing.T) {
	tests := []struct {
		name              string
		failLaterUpdate   bool
		wantNewerErrno    syscall.Errno
		wantRemotePath    string
		wantRemoteVersion string
		mutateNewer       func(*testing.T, context.Context, *FileSystem, *Handle) syscall.Errno
	}{
		{
			name:              "commit",
			wantRemotePath:    "b.md",
			wantRemoteVersion: "fmver_3",
			mutateNewer: func(t *testing.T, ctx context.Context, filesystem *FileSystem, handle *Handle) syscall.Errno {
				t.Helper()
				if err := handle.Truncate(0); err != nil {
					t.Fatal(err)
				}
				if _, err := handle.WriteAt([]byte("newer"), 0); err != nil {
					t.Fatal(err)
				}
				return filesystem.Commit(ctx, handle)
			},
		},
		{
			name:              "rename",
			wantRemotePath:    "c.md",
			wantRemoteVersion: "fmver_3",
			mutateNewer: func(_ *testing.T, ctx context.Context, filesystem *FileSystem, _ *Handle) syscall.Errno {
				return filesystem.Rename(ctx, "b.md", "c.md", false)
			},
		},
		{
			name:           "delete",
			wantRemotePath: "",
			mutateNewer: func(_ *testing.T, ctx context.Context, filesystem *FileSystem, _ *Handle) syscall.Errno {
				return filesystem.Delete(ctx, "b.md")
			},
		},
		{
			name:              "later_failure_does_not_suppress_unique_success",
			failLaterUpdate:   true,
			wantNewerErrno:    syscall.ESTALE,
			wantRemotePath:    "b.md",
			wantRemoteVersion: "fmver_2",
			mutateNewer: func(t *testing.T, ctx context.Context, filesystem *FileSystem, handle *Handle) syscall.Errno {
				t.Helper()
				if _, err := handle.WriteAt([]byte("newer"), 0); err != nil {
					t.Fatal(err)
				}
				return filesystem.Commit(ctx, handle)
			},
		},
	}
	for _, test := range tests {
		for _, withReporter := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/reporter=%t", test.name, withReporter), func(t *testing.T) {
				ctx := context.Background()
				remote := newDelayedMovedUpdateRemote(test.failLaterUpdate)
				t.Cleanup(remote.releaseOldResponse)
				filesystem := NewFileSystem(remote, mutationTestOptions(withReporter))
				if err := filesystem.Refresh(ctx); err != nil {
					t.Fatal(err)
				}
				older, errno := filesystem.Open(ctx, "a.md")
				if errno != 0 {
					t.Fatalf("Open old handle errno=%v", errno)
				}
				if err := older.Truncate(0); err != nil {
					t.Fatal(err)
				}
				if _, err := older.WriteAt([]byte("older"), 0); err != nil {
					t.Fatal(err)
				}
				oldDone := make(chan syscall.Errno, 1)
				go func() { oldDone <- filesystem.Commit(ctx, older) }()
				applied := receiveFSWithin(t, remote.applied, "old Update to apply remotely")
				if applied.Path != "b.md" || applied.LatestVersionID != "fmver_2" {
					t.Fatalf("old authoritative result=%+v", applied)
				}

				newer, errno := filesystem.Open(ctx, "b.md")
				if errno != 0 {
					t.Fatalf("Open new path errno=%v", errno)
				}
				if errno := test.mutateNewer(t, ctx, filesystem, newer); errno != test.wantNewerErrno {
					t.Fatalf("newer mutation errno=%v want %v", errno, test.wantNewerErrno)
				}

				remote.releaseOldResponse()
				if errno := receiveFSWithin(t, oldDone, "old Update response publication"); errno != 0 {
					t.Fatalf("old Commit errno=%v", errno)
				}
				if got := older.Memory(); got.Path != "b.md" || got.LatestVersionID != "fmver_2" {
					t.Fatalf("old handle memory=%+v", got)
				}

				remote.fakeRemote.mu.Lock()
				remotePath, remoteMemory, remoteExists := remote.fakeRemote.findByIDLocked("mem_a")
				remote.fakeRemote.mu.Unlock()
				if remotePath != test.wantRemotePath || (test.wantRemotePath != "" && (!remoteExists || remoteMemory.LatestVersionID != test.wantRemoteVersion)) || (test.wantRemotePath == "" && remoteExists) {
					t.Fatalf("remote item path=%q memory=%+v exists=%v", remotePath, remoteMemory, remoteExists)
				}

				filesystem.Index().mu.RLock()
				var indexed []Node
				for _, node := range filesystem.Index().nodes {
					if !node.IsDir && node.MemoryID == "mem_a" {
						indexed = append(indexed, node)
					}
				}
				filesystem.Index().mu.RUnlock()
				if test.wantRemotePath == "" {
					if len(indexed) != 0 {
						t.Fatalf("deleted item was resurrected in index: %+v", indexed)
					}
				} else if len(indexed) != 1 || indexed[0].Path != test.wantRemotePath || indexed[0].LatestVersionID != test.wantRemoteVersion {
					t.Fatalf("indexed item=%+v want one %s@%s", indexed, test.wantRemotePath, test.wantRemoteVersion)
				}
			})
		}
	}
}

func TestOlderCreateResponseCannotResurrectDeletedItemOrPlaceholder(t *testing.T) {
	for _, withReporter := range []bool{false, true} {
		t.Run(fmt.Sprintf("reporter=%t", withReporter), func(t *testing.T) {
			ctx := context.Background()
			remote := newDelayedCreateRemote()
			t.Cleanup(remote.releaseOldResponse)
			filesystem := NewFileSystem(remote, mutationTestOptions(withReporter))
			older, errno := filesystem.Create("a.md")
			if errno != 0 {
				t.Fatalf("Create errno=%v", errno)
			}
			if _, err := older.WriteAt([]byte("created"), 0); err != nil {
				t.Fatal(err)
			}
			oldDone := make(chan syscall.Errno, 1)
			go func() { oldDone <- filesystem.Commit(ctx, older) }()
			applied := receiveFSWithin(t, remote.applied, "old Create to apply remotely")

			newer, errno := filesystem.Open(ctx, "b.md")
			if errno != 0 {
				t.Fatalf("Open remotely created item errno=%v", errno)
			}
			if newer.Memory().MemoryID != applied.MemoryID {
				t.Fatalf("newer handle=%+v want item %q", newer.Memory(), applied.MemoryID)
			}
			if errno := filesystem.Delete(ctx, "b.md"); errno != 0 {
				t.Fatalf("Delete errno=%v", errno)
			}

			remote.releaseOldResponse()
			if errno := receiveFSWithin(t, oldDone, "old Create response publication"); errno != 0 {
				t.Fatalf("old Commit errno=%v", errno)
			}
			if _, exists := filesystem.Index().Lookup("a.md"); exists {
				t.Fatal("old Create response left local placeholder")
			}
			if _, exists := filesystem.Index().Lookup("b.md"); exists {
				t.Fatal("old Create response resurrected deleted authoritative path")
			}
			filesystem.stateMu.Lock()
			_, pending := filesystem.pendingCreated["a.md"]
			filesystem.stateMu.Unlock()
			if pending {
				t.Fatal("old Create response left pendingCreated placeholder owner")
			}
			remote.fakeRemote.mu.Lock()
			_, _, remoteExists := remote.fakeRemote.findByIDLocked(applied.MemoryID)
			remote.fakeRemote.mu.Unlock()
			if remoteExists {
				t.Fatal("Delete did not remain authoritative remotely")
			}
		})
	}
}

func TestSuppressedAuthoritativeUpdateClearsOlderAmbiguousCreateOwner(t *testing.T) {
	for _, withReporter := range []bool{false, true} {
		t.Run(fmt.Sprintf("reporter=%t", withReporter), func(t *testing.T) {
			ctx := context.Background()
			remote := newAmbiguousCreateDelayedMovedUpdateRemote()
			t.Cleanup(remote.releaseOldResponse)
			filesystem := NewFileSystem(remote, mutationTestOptions(withReporter))

			created, errno := filesystem.Create("a.md")
			if errno != 0 {
				t.Fatalf("Create errno=%v", errno)
			}
			if _, err := created.WriteAt([]byte("created"), 0); err != nil {
				t.Fatal(err)
			}
			if errno := filesystem.Commit(ctx, created); errno != syscall.EAGAIN {
				t.Fatalf("ambiguous Create Commit errno=%v want EAGAIN", errno)
			}
			if err := filesystem.Refresh(ctx); err != nil {
				t.Fatalf("Refresh authoritative a.md err=%v", err)
			}

			older, errno := filesystem.Open(ctx, "a.md")
			if errno != 0 {
				t.Fatalf("Open authoritative a.md errno=%v", errno)
			}
			if _, err := older.WriteAt([]byte("updated"), 0); err != nil {
				t.Fatal(err)
			}
			oldDone := make(chan syscall.Errno, 1)
			go func() { oldDone <- filesystem.Commit(ctx, older) }()
			applied := receiveFSWithin(t, remote.applied, "old Update to move item to b.md")
			if applied.Path != "b.md" {
				t.Fatalf("old authoritative result path=%q want b.md", applied.Path)
			}
			if err := filesystem.Refresh(ctx); err != nil {
				t.Fatalf("Refresh authoritative b.md err=%v", err)
			}
			if errno := filesystem.Delete(ctx, "b.md"); errno != 0 {
				t.Fatalf("newer Delete errno=%v", errno)
			}

			remote.releaseOldResponse()
			if errno := receiveFSWithin(t, oldDone, "suppressed old Update response"); errno != 0 {
				t.Fatalf("old Update Commit errno=%v", errno)
			}
			filesystem.stateMu.Lock()
			_, pending := filesystem.pendingCreated["a.md"]
			filesystem.stateMu.Unlock()
			if pending {
				t.Fatal("suppressed authoritative Update left older pendingCreated owner")
			}
			if errno := filesystem.Commit(ctx, created); errno != syscall.ESTALE {
				t.Fatalf("old ambiguous Create handle Commit errno=%v want ESTALE", errno)
			}
			remote.fakeRemote.mu.Lock()
			remoteEmpty := len(remote.fakeRemote.memories) == 0
			remote.fakeRemote.mu.Unlock()
			if !remoteEmpty {
				t.Fatalf("old ambiguous Create handle revived remote memories: %+v", snapshotRemoteMemories(remote.fakeRemote))
			}
		})
	}
}

func TestOlderUpdateCannotOverwriteNewerDifferentItemAtReturnedPath(t *testing.T) {
	for _, withReporter := range []bool{false, true} {
		t.Run(fmt.Sprintf("reporter=%t", withReporter), func(t *testing.T) {
			ctx := context.Background()
			remote := newDelayedMovedUpdateRemote(false)
			remote.fakeRemote.memories["c.md"] = client.Memory{
				MemoryID: "mem_c", Path: "c.md", Content: "c", ContentSHA256: "sha-c", ContentSizeBytes: 1, LatestVersionID: "fmver_1",
			}
			t.Cleanup(remote.releaseOldResponse)
			filesystem := NewFileSystem(remote, mutationTestOptions(withReporter))
			if err := filesystem.Refresh(ctx); err != nil {
				t.Fatal(err)
			}
			older, errno := filesystem.Open(ctx, "a.md")
			if errno != 0 {
				t.Fatalf("Open old item errno=%v", errno)
			}
			if _, err := older.WriteAt([]byte("older"), 0); err != nil {
				t.Fatal(err)
			}
			oldDone := make(chan syscall.Errno, 1)
			go func() { oldDone <- filesystem.Commit(ctx, older) }()
			receiveFSWithin(t, remote.applied, "old item Update to b.md")

			if errno := filesystem.Rename(ctx, "c.md", "b.md", true); errno != 0 {
				t.Fatalf("newer overwrite Rename errno=%v", errno)
			}
			remote.releaseOldResponse()
			if errno := receiveFSWithin(t, oldDone, "old item delayed response"); errno != 0 {
				t.Fatalf("old Commit errno=%v", errno)
			}

			node, exists := filesystem.Index().Lookup("b.md")
			if !exists || node.MemoryID != "mem_c" || node.LatestVersionID != "fmver_3" {
				t.Fatalf("newer different-item path owner was overwritten: node=%+v exists=%v", node, exists)
			}
		})
	}
}

func TestOlderUpdateCannotRemoveNewerPendingPlaceholder(t *testing.T) {
	for _, withReporter := range []bool{false, true} {
		t.Run(fmt.Sprintf("reporter=%t", withReporter), func(t *testing.T) {
			ctx := context.Background()
			remote := newDelayedMovedUpdateRemote(false)
			t.Cleanup(remote.releaseOldResponse)
			filesystem := NewFileSystem(remote, mutationTestOptions(withReporter))
			if err := filesystem.Refresh(ctx); err != nil {
				t.Fatal(err)
			}
			older, errno := filesystem.Open(ctx, "a.md")
			if errno != 0 {
				t.Fatalf("Open old item errno=%v", errno)
			}
			if _, err := older.WriteAt([]byte("older"), 0); err != nil {
				t.Fatal(err)
			}
			oldDone := make(chan syscall.Errno, 1)
			go func() { oldDone <- filesystem.Commit(ctx, older) }()
			receiveFSWithin(t, remote.applied, "old item Update to b.md")

			newer, errno := filesystem.Create("b.md")
			if errno != 0 {
				t.Fatalf("newer placeholder Create errno=%v", errno)
			}
			remote.releaseOldResponse()
			if errno := receiveFSWithin(t, oldDone, "old item delayed response"); errno != 0 {
				t.Fatalf("old Commit errno=%v", errno)
			}

			filesystem.stateMu.Lock()
			pending := filesystem.pendingCreated["b.md"]
			filesystem.stateMu.Unlock()
			if pending.handle != newer {
				t.Fatal("old Update removed newer pendingCreated owner")
			}
			node, exists := filesystem.Index().Lookup("b.md")
			if !exists || node.IsDir || node.MemoryID != "" {
				t.Fatalf("old Update replaced newer placeholder: node=%+v exists=%v", node, exists)
			}
		})
	}
}

func TestMutationSuccessWatermarkIsCollectedWithoutOlderInflightTicket(t *testing.T) {
	ctx := context.Background()
	remote := &fakeRemote{memories: map[string]client.Memory{
		"a.md": {MemoryID: "mem_a", Path: "a.md", Content: "old", LatestVersionID: "fmver_1"},
	}}
	filesystem := NewFileSystem(remote, Options{})
	if err := filesystem.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	handle, errno := filesystem.Open(ctx, "a.md")
	if errno != 0 {
		t.Fatalf("Open errno=%v", errno)
	}
	if _, err := handle.WriteAt([]byte("new"), 0); err != nil {
		t.Fatal(err)
	}
	if errno := filesystem.Commit(ctx, handle); errno != 0 {
		t.Fatalf("Commit errno=%v", errno)
	}
	assertMutationFenceStateEmpty(t, filesystem)
}

func TestOutOfOrderMutationResponsesEventuallyCollectAllWatermarks(t *testing.T) {
	ctx := context.Background()
	base := &fakeRemote{memories: map[string]client.Memory{
		"a.md": {MemoryID: "mem_a", Path: "a.md", Content: "a", LatestVersionID: "fmver_1"},
		"b.md": {MemoryID: "mem_b", Path: "b.md", Content: "b", LatestVersionID: "fmver_1"},
		"c.md": {MemoryID: "mem_c", Path: "c.md", Content: "c", LatestVersionID: "fmver_1"},
	}}
	remote := &reverseReleaseUpdateRemote{
		fakeRemote: base,
		started:    make(chan string, 3),
		releases: map[string]chan struct{}{
			"mem_a": make(chan struct{}), "mem_b": make(chan struct{}), "mem_c": make(chan struct{}),
		},
	}
	filesystem := NewFileSystem(remote, Options{})
	if err := filesystem.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	done := make(map[string]chan syscall.Errno, 3)
	for _, value := range []string{"a.md", "b.md", "c.md"} {
		handle, errno := filesystem.Open(ctx, value)
		if errno != 0 {
			t.Fatalf("Open %s errno=%v", value, errno)
		}
		if _, err := handle.WriteAt([]byte("new-"+value), 0); err != nil {
			t.Fatal(err)
		}
		itemID := handle.Memory().MemoryID
		done[itemID] = make(chan syscall.Errno, 1)
		go func() { done[itemID] <- filesystem.Commit(ctx, handle) }()
		if started := receiveFSWithin(t, remote.started, "ordered Update start"); started != itemID {
			t.Fatalf("started item=%q want %q", started, itemID)
		}
	}
	for _, itemID := range []string{"mem_c", "mem_b", "mem_a"} {
		close(remote.releases[itemID])
		if errno := receiveFSWithin(t, done[itemID], "reverse-order Update completion"); errno != 0 {
			t.Fatalf("Commit %s errno=%v", itemID, errno)
		}
	}
	assertMutationFenceStateEmpty(t, filesystem)
}

func assertMutationFenceStateEmpty(t *testing.T, filesystem *FileSystem) {
	t.Helper()
	filesystem.stateMu.Lock()
	defer filesystem.stateMu.Unlock()
	if filesystem.completedMutationThrough != filesystem.nextMutationTicket ||
		len(filesystem.completedMutationTickets) != 0 || len(filesystem.publishedTicket) != 0 ||
		len(filesystem.publishedPathTicket) != 0 || len(filesystem.successKeysByTicket) != 0 {
		t.Fatalf("mutation fence state not collected: next=%d through=%d completed=%v items=%v paths=%v buckets=%v",
			filesystem.nextMutationTicket, filesystem.completedMutationThrough, filesystem.completedMutationTickets,
			filesystem.publishedTicket, filesystem.publishedPathTicket, filesystem.successKeysByTicket)
	}
}

func TestFailedMutationCompletesTicketWithoutSuccessWatermark(t *testing.T) {
	for _, withReporter := range []bool{false, true} {
		t.Run(fmt.Sprintf("reporter=%t", withReporter), func(t *testing.T) {
			ctx := context.Background()
			remote := &fakeRemote{memories: map[string]client.Memory{
				"a.md": {MemoryID: "mem_a", Path: "a.md", Content: "old", LatestVersionID: "fmver_1"},
			}}
			filesystem := NewFileSystem(remote, mutationTestOptions(withReporter))
			if err := filesystem.Refresh(ctx); err != nil {
				t.Fatal(err)
			}
			handle, errno := filesystem.Open(ctx, "a.md")
			if errno != 0 {
				t.Fatalf("Open errno=%v", errno)
			}
			if _, err := handle.WriteAt([]byte("new"), 0); err != nil {
				t.Fatal(err)
			}
			remote.mu.Lock()
			remote.memories["a.md"] = client.Memory{MemoryID: "mem_a", Path: "a.md", Content: "other", LatestVersionID: "fmver_2"}
			remote.mu.Unlock()
			if errno := filesystem.Commit(ctx, handle); errno != syscall.ESTALE {
				t.Fatalf("stale Commit errno=%v want ESTALE", errno)
			}
			assertMutationFenceStateEmpty(t, filesystem)
		})
	}
}

func TestDeleteTombstoneWatermarkLivesUntilOlderResponseCompletes(t *testing.T) {
	for _, withReporter := range []bool{false, true} {
		t.Run(fmt.Sprintf("reporter=%t", withReporter), func(t *testing.T) {
			ctx := context.Background()
			remote := newDelayedMovedUpdateRemote(false)
			t.Cleanup(remote.releaseOldResponse)
			filesystem := NewFileSystem(remote, mutationTestOptions(withReporter))
			if err := filesystem.Refresh(ctx); err != nil {
				t.Fatal(err)
			}
			older, errno := filesystem.Open(ctx, "a.md")
			if errno != 0 {
				t.Fatalf("Open old item errno=%v", errno)
			}
			if _, err := older.WriteAt([]byte("older"), 0); err != nil {
				t.Fatal(err)
			}
			oldDone := make(chan syscall.Errno, 1)
			go func() { oldDone <- filesystem.Commit(ctx, older) }()
			receiveFSWithin(t, remote.applied, "old Update to apply at b.md")
			if _, errno := filesystem.Open(ctx, "b.md"); errno != 0 {
				t.Fatalf("Open new path errno=%v", errno)
			}
			if errno := filesystem.Delete(ctx, "b.md"); errno != 0 {
				t.Fatalf("Delete errno=%v", errno)
			}

			filesystem.stateMu.Lock()
			itemWatermark := filesystem.publishedTicket["mem_a"]
			pathWatermark := filesystem.publishedPathTicket["b.md"]
			_, deleteCompletedOutOfOrder := filesystem.completedMutationTickets[2]
			_, hasDeleteBucket := filesystem.successKeysByTicket[2]
			completedThrough := filesystem.completedMutationThrough
			filesystem.stateMu.Unlock()
			if itemWatermark != 2 || pathWatermark != 2 || !deleteCompletedOutOfOrder || !hasDeleteBucket || completedThrough != 0 {
				t.Fatalf("delete tombstone not retained: item=%d path=%d completed2=%v bucket2=%v through=%d",
					itemWatermark, pathWatermark, deleteCompletedOutOfOrder, hasDeleteBucket, completedThrough)
			}

			remote.releaseOldResponse()
			if errno := receiveFSWithin(t, oldDone, "old Update after Delete"); errno != 0 {
				t.Fatalf("old Commit errno=%v", errno)
			}
			filesystem.Index().mu.RLock()
			for _, node := range filesystem.Index().nodes {
				if !node.IsDir && node.MemoryID == "mem_a" {
					filesystem.Index().mu.RUnlock()
					t.Fatalf("old response resurrected deleted item: %+v", node)
				}
			}
			filesystem.Index().mu.RUnlock()
			assertMutationFenceStateEmpty(t, filesystem)
		})
	}
}

func TestFileSystemOpenCommitRenameDelete(t *testing.T) {
	remote := &fakeRemote{memories: map[string]client.Memory{
		"a.md": {MemoryID: "mem_a", Path: "a.md", Content: "one", ContentSHA256: "sha-old", LatestVersionID: "fmver_1"},
	}}
	filesystem := NewFileSystem(remote, Options{})
	if err := filesystem.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	handle, errno := filesystem.Open(context.Background(), "a.md")
	if errno != 0 {
		t.Fatalf("open errno=%v", errno)
	}
	if remote.lastReadPath != "a.md" {
		t.Fatalf("open must read by path: path=%q", remote.lastReadPath)
	}
	_, _ = handle.WriteAt([]byte("two"), 0)
	_ = handle.Truncate(3)
	if errno := filesystem.Commit(context.Background(), handle); errno != 0 {
		t.Fatalf("commit errno=%v", errno)
	}
	if remote.lastItemID != "mem_a" || remote.lastExpectedVersionID != "fmver_1" {
		t.Fatalf("update identity=(%q,%q)", remote.lastItemID, remote.lastExpectedVersionID)
	}
	updated := remote.memories["a.md"]
	if updated.Content != "two" || updated.MemoryID != "mem_a" || updated.LatestVersionID == "" || updated.LatestVersionID == "fmver_1" {
		t.Fatalf("updated memory=%+v", updated)
	}
	if memory := handle.Memory(); memory.MemoryID != updated.MemoryID || memory.LatestVersionID != updated.LatestVersionID {
		t.Fatalf("handle memory=%+v want identity from %+v", memory, updated)
	}
	if node, ok := filesystem.Index().Lookup("a.md"); !ok || node.MemoryID != updated.MemoryID || node.LatestVersionID != updated.LatestVersionID {
		t.Fatalf("updated node=%+v ok=%v", node, ok)
	}
	if errno := filesystem.Rename(context.Background(), "a.md", "b.md", true); errno != 0 {
		t.Fatalf("rename errno=%v", errno)
	}
	if remote.lastItemID != "mem_a" || remote.lastExpectedVersionID != updated.LatestVersionID {
		t.Fatalf("rename identity=(%q,%q), want (mem_a,%q)", remote.lastItemID, remote.lastExpectedVersionID, updated.LatestVersionID)
	}
	renamed := remote.memories["b.md"]
	if renamed.MemoryID != "mem_a" || renamed.LatestVersionID == "" || renamed.LatestVersionID == updated.LatestVersionID {
		t.Fatalf("renamed memory=%+v", renamed)
	}
	if node, ok := filesystem.Index().Lookup("b.md"); !ok || node.MemoryID != "mem_a" || node.LatestVersionID != renamed.LatestVersionID {
		t.Fatalf("renamed node=%+v ok=%v", node, ok)
	}
	if errno := filesystem.Delete(context.Background(), "b.md"); errno != 0 {
		t.Fatalf("delete errno=%v", errno)
	}
	if remote.lastItemID != "mem_a" || remote.lastExpectedVersionID != renamed.LatestVersionID {
		t.Fatalf("delete identity=(%q,%q), want (mem_a,%q)", remote.lastItemID, remote.lastExpectedVersionID, renamed.LatestVersionID)
	}
	if _, ok := filesystem.Index().Lookup("b.md"); ok {
		t.Fatal("deleted target remains in index")
	}
}

func TestOpenAllowsConcurrentRemoteReads(t *testing.T) {
	ctx := context.Background()
	remote := &blockingReadRemote{fakeRemote: &fakeRemote{memories: map[string]client.Memory{
		"a.md": {MemoryID: "mem_a", Path: "a.md", Content: "a", LatestVersionID: "fmver_1"},
		"b.md": {MemoryID: "mem_b", Path: "b.md", Content: "b", LatestVersionID: "fmver_1"},
	}}}
	filesystem := NewFileSystem(remote, Options{})
	started, proceed := remote.pauseNextReads(2)
	type openResult struct {
		handle *Handle
		errno  syscall.Errno
	}
	results := make(chan openResult, 2)
	for _, value := range []string{"a.md", "b.md"} {
		value := value
		go func() {
			handle, errno := filesystem.Open(ctx, value)
			results <- openResult{handle: handle, errno: errno}
		}()
	}

	seen := make(map[string]bool, 2)
	for range 2 {
		select {
		case value := <-started:
			seen[value] = true
		case <-time.After(time.Second):
			close(proceed)
			for range 2 {
				<-results
			}
			t.Fatal("remote reads did not overlap")
		}
	}
	close(proceed)
	if !seen["a.md"] || !seen["b.md"] {
		t.Fatalf("started reads=%v, want both paths", seen)
	}
	for range 2 {
		result := <-results
		if result.errno != 0 || result.handle == nil {
			t.Fatalf("open result=(%v,%v)", result.handle, result.errno)
		}
	}
}

func TestStaleOpenCannotRollBackIndex(t *testing.T) {
	ctx := context.Background()
	v1 := client.Memory{
		MemoryID:         "mem_a",
		Path:             "a.md",
		Content:          "one",
		ContentSHA256:    "sha-v1",
		ContentSizeBytes: 3,
		LatestVersionID:  "fmver_1",
	}
	remote := &blockingReadRemote{fakeRemote: &fakeRemote{memories: map[string]client.Memory{"a.md": v1}}}
	filesystem := NewFileSystem(remote, Options{})
	if err := filesystem.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	committing, errno := filesystem.Open(ctx, "a.md")
	if errno != 0 {
		t.Fatalf("open committing handle errno=%v", errno)
	}
	if _, err := committing.WriteAt([]byte("two"), 0); err != nil {
		t.Fatal(err)
	}

	started, proceed := remote.pauseNextReads(1)
	type openResult struct {
		handle *Handle
		errno  syscall.Errno
	}
	staleDone := make(chan openResult, 1)
	go func() {
		handle, errno := filesystem.Open(ctx, "a.md")
		staleDone <- openResult{handle: handle, errno: errno}
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		close(proceed)
		t.Fatal("stale read did not pause")
	}

	commitDone := make(chan syscall.Errno, 1)
	go func() { commitDone <- filesystem.Commit(ctx, committing) }()
	var commitErr syscall.Errno
	select {
	case commitErr = <-commitDone:
	case <-time.After(time.Second):
		close(proceed)
		<-staleDone
		<-commitDone
		t.Fatal("commit remained blocked while stale Open read was paused")
	}
	if commitErr != 0 {
		close(proceed)
		t.Fatalf("commit v2 errno=%v", commitErr)
	}
	close(proceed)
	stale := <-staleDone
	if stale.errno != 0 {
		t.Fatalf("stale open errno=%v", stale.errno)
	}
	if memory := stale.handle.Memory(); memory.Content != "one" || memory.LatestVersionID != "fmver_1" {
		t.Fatalf("stale handle memory=%+v, want v1 snapshot", memory)
	}
	if node, ok := filesystem.Index().Lookup("a.md"); !ok || node.LatestVersionID != "fmver_2" || node.SHA256 != "sha-fmver_2" {
		t.Fatalf("stale open rolled index back: node=%+v ok=%v", node, ok)
	}
}

func TestOlderSamePathOpenCannotOverwriteNewerOpen(t *testing.T) {
	ctx := context.Background()
	v1 := client.Memory{MemoryID: "mem_a", Path: "a.md", Content: "one", ContentSHA256: "sha-v1", LatestVersionID: "fmver_1"}
	v2 := client.Memory{MemoryID: "mem_a", Path: "a.md", Content: "two", ContentSHA256: "sha-v2", LatestVersionID: "fmver_2"}
	remote := &blockingReadRemote{fakeRemote: &fakeRemote{memories: map[string]client.Memory{"a.md": v1}}}
	filesystem := NewFileSystem(remote, Options{})

	started, proceed := remote.pauseNextReads(1)
	type openResult struct {
		handle *Handle
		errno  syscall.Errno
	}
	oldDone := make(chan openResult, 1)
	go func() {
		handle, errno := filesystem.Open(ctx, "a.md")
		oldDone <- openResult{handle: handle, errno: errno}
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		close(proceed)
		t.Fatal("older Open did not pause after copying v1")
	}
	remote.mu.Lock()
	remote.memories["a.md"] = v2
	remote.mu.Unlock()
	newer, errno := filesystem.Open(ctx, "a.md")
	if errno != 0 {
		close(proceed)
		t.Fatalf("newer Open errno=%v", errno)
	}
	if memory := newer.Memory(); memory.LatestVersionID != "fmver_2" {
		close(proceed)
		t.Fatalf("newer handle memory=%+v", memory)
	}
	close(proceed)
	older := <-oldDone
	if older.errno != 0 {
		t.Fatalf("older Open errno=%v", older.errno)
	}
	if memory := older.handle.Memory(); memory.LatestVersionID != "fmver_1" {
		t.Fatalf("older handle memory=%+v, want v1", memory)
	}
	if node, ok := filesystem.Index().Lookup("a.md"); !ok || node.LatestVersionID != "fmver_2" || node.SHA256 != "sha-v2" {
		t.Fatalf("older Open overwrote newer point observation: node=%+v ok=%v", node, ok)
	}
}

func TestOlderOpenCannotOverwriteNewerRefresh(t *testing.T) {
	ctx := context.Background()
	v1 := client.Memory{MemoryID: "mem_a", Path: "a.md", Content: "one", ContentSHA256: "sha-v1", LatestVersionID: "fmver_1"}
	v2 := client.Memory{MemoryID: "mem_a", Path: "a.md", Content: "two", ContentSHA256: "sha-v2", LatestVersionID: "fmver_2"}
	remote := &blockingReadRemote{fakeRemote: &fakeRemote{memories: map[string]client.Memory{"a.md": v1}}}
	filesystem := NewFileSystem(remote, Options{})

	started, proceed := remote.pauseNextReads(1)
	oldDone := make(chan *Handle, 1)
	oldErrno := make(chan syscall.Errno, 1)
	go func() {
		handle, errno := filesystem.Open(ctx, "a.md")
		oldDone <- handle
		oldErrno <- errno
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		close(proceed)
		t.Fatal("older Open did not pause after copying v1")
	}
	remote.mu.Lock()
	remote.memories["a.md"] = v2
	remote.mu.Unlock()
	if err := filesystem.Refresh(ctx); err != nil {
		close(proceed)
		t.Fatalf("newer Refresh err=%v", err)
	}
	close(proceed)
	older := <-oldDone
	if errno := <-oldErrno; errno != 0 {
		t.Fatalf("older Open errno=%v", errno)
	}
	if memory := older.Memory(); memory.LatestVersionID != "fmver_1" {
		t.Fatalf("older handle memory=%+v, want v1", memory)
	}
	if node, ok := filesystem.Index().Lookup("a.md"); !ok || node.LatestVersionID != "fmver_2" || node.SHA256 != "sha-v2" {
		t.Fatalf("older Open overwrote newer snapshot: node=%+v ok=%v", node, ok)
	}
}

func TestOlderRefreshCannotOverwriteNewerOpen(t *testing.T) {
	ctx := context.Background()
	v1 := client.Memory{MemoryID: "mem_a", Path: "a.md", Content: "one", ContentSHA256: "sha-v1", LatestVersionID: "fmver_1"}
	v2 := client.Memory{MemoryID: "mem_a", Path: "a.md", Content: "two", ContentSHA256: "sha-v2", LatestVersionID: "fmver_2"}
	remote := &fakeRemote{memories: map[string]client.Memory{"a.md": v1}}
	filesystem := NewFileSystem(remote, Options{})

	ready, resume := remote.pauseNextList()
	refreshDone := make(chan error, 1)
	go func() { refreshDone <- filesystem.Refresh(ctx) }()
	select {
	case <-ready:
	case <-time.After(time.Second):
		close(resume)
		t.Fatal("older Refresh did not pause after copying v1")
	}
	remote.mu.Lock()
	remote.memories["a.md"] = v2
	remote.mu.Unlock()
	newer, errno := filesystem.Open(ctx, "a.md")
	if errno != 0 {
		close(resume)
		t.Fatalf("newer Open errno=%v", errno)
	}
	if memory := newer.Memory(); memory.LatestVersionID != "fmver_2" {
		close(resume)
		t.Fatalf("newer handle memory=%+v", memory)
	}
	close(resume)
	if err := <-refreshDone; err != nil {
		t.Fatalf("older Refresh err=%v", err)
	}
	if node, ok := filesystem.Index().Lookup("a.md"); !ok || node.LatestVersionID != "fmver_2" || node.SHA256 != "sha-v2" {
		t.Fatalf("older Refresh overwrote newer point observation: node=%+v ok=%v", node, ok)
	}
}

func TestDifferentPathOpenObservationsPublishIndependently(t *testing.T) {
	ctx := context.Background()
	a := client.Memory{MemoryID: "mem_a", Path: "a.md", Content: "a", ContentSHA256: "sha-a", LatestVersionID: "fmver_1"}
	b := client.Memory{MemoryID: "mem_b", Path: "b.md", Content: "b", ContentSHA256: "sha-b", LatestVersionID: "fmver_2"}
	remote := &blockingReadRemote{fakeRemote: &fakeRemote{memories: map[string]client.Memory{"a.md": a, "b.md": b}}}
	filesystem := NewFileSystem(remote, Options{})

	started, proceed := remote.pauseNextReads(1)
	aDone := make(chan syscall.Errno, 1)
	go func() {
		_, errno := filesystem.Open(ctx, "a.md")
		aDone <- errno
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		close(proceed)
		t.Fatal("a.md Open did not pause")
	}
	if _, errno := filesystem.Open(ctx, "b.md"); errno != 0 {
		close(proceed)
		t.Fatalf("b.md Open errno=%v", errno)
	}
	close(proceed)
	if errno := <-aDone; errno != 0 {
		t.Fatalf("a.md Open errno=%v", errno)
	}
	if node, ok := filesystem.Index().Lookup("a.md"); !ok || node.MemoryID != "mem_a" || node.SHA256 != "sha-a" {
		t.Fatalf("a.md observation was dropped: node=%+v ok=%v", node, ok)
	}
	if node, ok := filesystem.Index().Lookup("b.md"); !ok || node.MemoryID != "mem_b" || node.SHA256 != "sha-b" {
		t.Fatalf("b.md observation was dropped: node=%+v ok=%v", node, ok)
	}
}

func TestOpenStoresMetadataOnlyPointObservation(t *testing.T) {
	ctx := context.Background()
	content := strings.Repeat("x", MaxContentBytes)
	memory := client.Memory{
		MemoryID:         "mem_a",
		Path:             "a.md",
		Content:          content,
		ContentSizeBytes: int64(len(content)),
		ContentSHA256:    "sha-full",
		LatestVersionID:  "fmver_1",
	}
	remote := &fakeRemote{memories: map[string]client.Memory{"a.md": memory}}
	filesystem := NewFileSystem(remote, Options{})
	handle, errno := filesystem.Open(ctx, "a.md")
	if errno != 0 {
		t.Fatalf("Open errno=%v", errno)
	}
	if got := handle.Memory().Content; got != content {
		t.Fatalf("handle content length=%d want %d", len(got), len(content))
	}
	if got := string(handle.Bytes()); got != content {
		t.Fatalf("handle bytes length=%d want %d", len(got), len(content))
	}

	filesystem.stateMu.Lock()
	observation, ok := filesystem.pointObservations["a.md"]
	filesystem.stateMu.Unlock()
	if !ok {
		t.Fatal("point observation was not recorded")
	}
	if observation.memory.Content != "" {
		t.Fatalf("stored observation retained %d content bytes", len(observation.memory.Content))
	}
	if observation.memory.MemoryID != "mem_a" || observation.memory.ContentSizeBytes != int64(len(content)) || observation.memory.ContentSHA256 != "sha-full" || observation.memory.LatestVersionID != "fmver_1" {
		t.Fatalf("stored observation lost metadata: %+v", observation.memory)
	}
}

func TestRefreshSnapshotCannotRollBackConcurrentCommit(t *testing.T) {
	ctx := context.Background()
	remote := &fakeRemote{memories: map[string]client.Memory{
		"a.md": {
			MemoryID:         "mem_a",
			Path:             "a.md",
			Content:          "one",
			ContentSHA256:    "sha-v1",
			ContentSizeBytes: 3,
			LatestVersionID:  "fmver_1",
		},
	}}
	filesystem := NewFileSystem(remote, Options{})
	if err := filesystem.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	handle, errno := filesystem.Open(ctx, "a.md")
	if errno != 0 {
		t.Fatalf("open errno=%v", errno)
	}
	if _, err := handle.WriteAt([]byte("two"), 0); err != nil {
		t.Fatal(err)
	}

	snapshotReady, resumeRefresh := remote.pauseNextList()
	refreshDone := make(chan error, 1)
	go func() { refreshDone <- filesystem.Refresh(ctx) }()
	<-snapshotReady

	commitDone := make(chan syscall.Errno, 1)
	go func() { commitDone <- filesystem.Commit(ctx, handle) }()
	var commitErr syscall.Errno
	select {
	case commitErr = <-commitDone:
	case <-time.After(time.Second):
		close(resumeRefresh)
		<-refreshDone
		<-commitDone
		t.Fatal("commit remained blocked while unrelated refresh List was paused")
	}
	close(resumeRefresh)
	if err := <-refreshDone; err != nil {
		t.Fatalf("refresh err=%v", err)
	}
	if commitErr != 0 {
		t.Fatalf("commit errno=%v", commitErr)
	}

	remote.mu.Lock()
	winner := remote.memories["a.md"]
	remote.mu.Unlock()
	if winner.Content != "two" || winner.LatestVersionID != "fmver_2" {
		t.Fatalf("remote winner=%+v", winner)
	}
	if node, ok := filesystem.Index().Lookup("a.md"); !ok || node.MemoryID != "mem_a" || node.LatestVersionID != winner.LatestVersionID || node.SHA256 != winner.ContentSHA256 {
		t.Fatalf("index rolled back after v2 commit: node=%+v ok=%v winner=%+v", node, ok, winner)
	}
}

func TestEnsureFreshCoalescesQueuedRefreshes(t *testing.T) {
	ctx := context.Background()
	remote := &fakeRemote{memories: map[string]client.Memory{
		"a.md": {MemoryID: "mem_a", Path: "a.md", LatestVersionID: "fmver_1"},
	}}
	filesystem := NewFileSystem(remote, Options{DirectoryTTL: time.Hour})
	snapshotReady, resumeRefresh := remote.pauseNextList()
	firstDone := make(chan error, 1)
	go func() { firstDone <- filesystem.EnsureFresh(ctx) }()
	<-snapshotReady
	secondDone := make(chan error, 1)
	secondStarted := make(chan struct{})
	go func() {
		close(secondStarted)
		secondDone <- filesystem.EnsureFresh(ctx)
	}()
	<-secondStarted
	close(resumeRefresh)
	if err := <-firstDone; err != nil {
		t.Fatalf("first EnsureFresh err=%v", err)
	}
	if err := <-secondDone; err != nil {
		t.Fatalf("second EnsureFresh err=%v", err)
	}

	remote.mu.Lock()
	calls := append([]string(nil), remote.calls...)
	remote.mu.Unlock()
	listCalls := 0
	for _, call := range calls {
		if call == "List" {
			listCalls++
		}
	}
	if listCalls != 1 {
		t.Fatalf("List calls=%d want 1, calls=%v", listCalls, calls)
	}
	if node, ok := filesystem.Index().Lookup("a.md"); !ok || node.MemoryID != "mem_a" || node.LatestVersionID != "fmver_1" {
		t.Fatalf("node=%+v ok=%v", node, ok)
	}
	filesystem.stateMu.Lock()
	lastRefresh := filesystem.lastRefresh
	filesystem.stateMu.Unlock()
	if lastRefresh.IsZero() {
		t.Fatal("lastRefresh was not updated")
	}
}

func raceRefreshWithMkdir(t *testing.T, filesystem *FileSystem, remote *fakeRemote, dir string) {
	t.Helper()
	ready, resume := remote.pauseNextList()
	done := make(chan error, 1)
	go func() { done <- filesystem.Refresh(context.Background()) }()
	select {
	case <-ready:
	case <-time.After(time.Second):
		close(resume)
		t.Fatal("refresh List did not pause")
	}
	if errno := filesystem.Mkdir(dir); errno != 0 {
		close(resume)
		t.Fatalf("mkdir %q errno=%v", dir, errno)
	}
	close(resume)
	if err := <-done; err != nil {
		t.Fatalf("refresh err=%v", err)
	}
}

func TestRefreshRacePreservesStateAndSchedulesBackgroundRecovery(t *testing.T) {
	ctx := context.Background()
	remote := &fakeRemote{memories: map[string]client.Memory{
		"a.md": {MemoryID: "mem_a", Path: "a.md", ContentSHA256: "sha-v1", LatestVersionID: "fmver_1"},
	}}
	filesystem := NewFileSystem(remote, Options{})
	if err := filesystem.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	filesystem.stateMu.Lock()
	initialRefresh := filesystem.lastRefresh
	filesystem.stateMu.Unlock()
	remote.mu.Lock()
	remote.readOnly = true
	remote.memories["stale.md"] = client.Memory{MemoryID: "mem_stale", Path: "stale.md", LatestVersionID: "fmver_1"}
	remote.mu.Unlock()

	scheduler := &queuedRefreshScheduler{}
	filesystem.scheduleBackgroundRefresh = scheduler.schedule
	raceRefreshWithMkdir(t, filesystem, remote, "local")

	filesystem.stateMu.Lock()
	lastRefresh := filesystem.lastRefresh
	filesystem.stateMu.Unlock()
	if !lastRefresh.Equal(initialRefresh) {
		t.Fatalf("raced refresh updated lastRefresh: got=%v want=%v", lastRefresh, initialRefresh)
	}
	if filesystem.ReadOnly() {
		t.Fatal("raced refresh published server readOnly=true")
	}
	if _, ok := filesystem.Index().Lookup("stale.md"); ok {
		t.Fatal("raced refresh published its candidate index")
	}
	if node, ok := filesystem.Index().Lookup("local"); !ok || !node.IsDir {
		t.Fatalf("live local directory lost: node=%+v ok=%v", node, ok)
	}
	if got := scheduler.count(); got != 1 {
		t.Fatalf("scheduled recovery tasks=%d want 1", got)
	}

	scheduler.take(t)()
	filesystem.stateMu.Lock()
	recoveredAt := filesystem.lastRefresh
	filesystem.stateMu.Unlock()
	if !recoveredAt.After(initialRefresh) {
		t.Fatalf("background recovery did not publish lastRefresh: got=%v initial=%v", recoveredAt, initialRefresh)
	}
	if !filesystem.ReadOnly() {
		t.Fatal("background recovery did not publish server readOnly=true")
	}
	if node, ok := filesystem.Index().Lookup("stale.md"); !ok || node.MemoryID != "mem_stale" {
		t.Fatalf("background recovery node=%+v ok=%v", node, ok)
	}
	if node, ok := filesystem.Index().Lookup("local"); !ok || !node.IsDir {
		t.Fatalf("background recovery lost local directory: node=%+v ok=%v", node, ok)
	}
}

func TestRefreshRecoverySchedulingIsCoalesced(t *testing.T) {
	remote := &fakeRemote{memories: map[string]client.Memory{}}
	filesystem := NewFileSystem(remote, Options{})
	if err := filesystem.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	scheduler := &queuedRefreshScheduler{}
	filesystem.scheduleBackgroundRefresh = scheduler.schedule

	raceRefreshWithMkdir(t, filesystem, remote, "first")
	raceRefreshWithMkdir(t, filesystem, remote, "second")

	if got := scheduler.count(); got != 1 {
		t.Fatalf("coalesced recovery tasks=%d want 1", got)
	}
}

func TestRefreshRecoveryAttemptsHaveOverallDeadlines(t *testing.T) {
	remote := &fakeRemote{memories: map[string]client.Memory{}}
	filesystem := NewFileSystem(remote, Options{
		RefreshRecoveryTimeout: 20 * time.Millisecond,
		RefreshRecoveryBackoff: time.Millisecond,
	})
	defer filesystem.Close()
	if err := filesystem.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	scheduler := &queuedRefreshScheduler{}
	filesystem.scheduleBackgroundRefresh = scheduler.schedule
	raceRefreshWithMkdir(t, filesystem, remote, "foreground-race")

	ready, resume := remote.pauseNextLists(2)
	defer func() {
		for _, channel := range resume {
			close(channel)
		}
	}()
	workerDone := make(chan struct{})
	go func() {
		scheduler.take(t)()
		close(workerDone)
	}()
	for attempt := range 2 {
		select {
		case <-ready[attempt]:
		case <-time.After(250 * time.Millisecond):
			t.Fatalf("recovery attempt %d did not start", attempt+1)
		}
	}
	select {
	case <-workerDone:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("recovery worker exceeded its two bounded attempts")
	}
	filesystem.stateMu.Lock()
	scheduled := filesystem.backgroundRefreshScheduled
	filesystem.stateMu.Unlock()
	if scheduled {
		t.Fatal("recovery reservation remained after deadline exhaustion")
	}
}

func TestCloseCancelsRecoveryAndForegroundRefreshWaitIsContextBound(t *testing.T) {
	remote := &fakeRemote{memories: map[string]client.Memory{}}
	filesystem := NewFileSystem(remote, Options{
		DirectoryTTL:           time.Hour,
		RefreshRecoveryTimeout: time.Minute,
		RefreshRecoveryBackoff: time.Minute,
	})
	if err := filesystem.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	scheduler := &queuedRefreshScheduler{}
	filesystem.scheduleBackgroundRefresh = scheduler.schedule
	raceRefreshWithMkdir(t, filesystem, remote, "foreground-race")

	ready, resume := remote.pauseNextList()
	defer close(resume)
	workerDone := make(chan struct{})
	go func() {
		scheduler.take(t)()
		close(workerDone)
	}()
	select {
	case <-ready:
	case <-time.After(time.Second):
		t.Fatal("recovery List did not start")
	}

	foregroundCtx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	started := time.Now()
	err := filesystem.EnsureFresh(foregroundCtx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("foreground EnsureFresh err=%v want deadline exceeded", err)
	}
	if elapsed := time.Since(started); elapsed > 250*time.Millisecond {
		t.Fatalf("foreground EnsureFresh ignored caller deadline: elapsed=%v", elapsed)
	}

	filesystem.Close()
	select {
	case <-workerDone:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("recovery worker did not stop after filesystem Close")
	}
	filesystem.stateMu.Lock()
	scheduled := filesystem.backgroundRefreshScheduled
	filesystem.stateMu.Unlock()
	if scheduled {
		t.Fatal("recovery reservation remained after filesystem Close")
	}
}

func TestRefreshRecoveryStopsAfterTwoRacedAttempts(t *testing.T) {
	ctx := context.Background()
	remote := &fakeRemote{memories: map[string]client.Memory{}}
	filesystem := NewFileSystem(remote, Options{})
	if err := filesystem.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	filesystem.stateMu.Lock()
	initialRefresh := filesystem.lastRefresh
	filesystem.stateMu.Unlock()
	scheduler := &queuedRefreshScheduler{}
	filesystem.scheduleBackgroundRefresh = scheduler.schedule
	raceRefreshWithMkdir(t, filesystem, remote, "foreground-race")

	ready, resume := remote.pauseNextLists(2)
	task := scheduler.take(t)
	workerDone := make(chan struct{})
	go func() {
		task()
		close(workerDone)
	}()
	for attempt := range 2 {
		select {
		case <-ready[attempt]:
		case <-time.After(time.Second):
			close(resume[attempt])
			t.Fatalf("background refresh attempt %d did not start", attempt+1)
		}
		if errno := filesystem.Mkdir(fmt.Sprintf("recovery-race-%d", attempt+1)); errno != 0 {
			close(resume[attempt])
			t.Fatalf("mkdir during recovery attempt %d errno=%v", attempt+1, errno)
		}
		close(resume[attempt])
	}
	select {
	case <-workerDone:
	case <-time.After(time.Second):
		t.Fatal("background recovery did not stop after two attempts")
	}

	remote.mu.Lock()
	listCalls := 0
	for _, call := range remote.calls {
		if call == "List" {
			listCalls++
		}
	}
	remote.mu.Unlock()
	if listCalls != 4 {
		t.Fatalf("List calls=%d want 4 (initial, foreground, two recovery attempts)", listCalls)
	}
	filesystem.stateMu.Lock()
	lastRefresh := filesystem.lastRefresh
	filesystem.stateMu.Unlock()
	if !lastRefresh.Equal(initialRefresh) {
		t.Fatalf("two raced recovery attempts updated lastRefresh: got=%v want=%v", lastRefresh, initialRefresh)
	}
}

func TestRefreshRecoveryExitHandoffSchedulesNewRecovery(t *testing.T) {
	ctx := context.Background()
	remote := &refreshHandoffRemote{fakeRemote: &fakeRemote{memories: map[string]client.Memory{}}}
	filesystem := NewFileSystem(remote, Options{})
	if err := filesystem.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	filesystem.stateMu.Lock()
	initialRefresh := filesystem.lastRefresh
	filesystem.stateMu.Unlock()

	scheduler := &queuedRefreshScheduler{}
	filesystem.scheduleBackgroundRefresh = scheduler.schedule
	raceRefreshWithMkdir(t, filesystem, remote.fakeRemote, "initial-race")
	firstWorker := scheduler.take(t)
	remote.queueListSteps(
		refreshHandoffListStep{err: errors.New("first recovery attempt failed")},
		refreshHandoffListStep{err: errors.New("final recovery attempt failed")},
	)

	hookCalls := 0
	var foregroundErr error
	filesystem.afterRefreshRecoveryUnlock = func() {
		hookCalls++
		filesystem.afterRefreshRecoveryUnlock = nil
		remote.queueListSteps(refreshHandoffListStep{afterSnapshot: func() {
			if errno := filesystem.Mkdir("exit-handoff-race"); errno != 0 {
				t.Fatalf("mkdir during exit-handoff refresh errno=%v", errno)
			}
		}})
		foregroundErr = filesystem.Refresh(ctx)
	}
	firstWorker()
	if hookCalls != 1 {
		t.Fatalf("exit handoff hook calls=%d want 1", hookCalls)
	}
	if foregroundErr != nil {
		t.Fatalf("forced foreground refresh err=%v", foregroundErr)
	}
	filesystem.stateMu.Lock()
	afterForeground := filesystem.lastRefresh
	filesystem.stateMu.Unlock()
	if !afterForeground.Equal(initialRefresh) {
		t.Fatalf("raced foreground refresh updated lastRefresh: got=%v want=%v", afterForeground, initialRefresh)
	}
	if got := scheduler.count(); got != 1 {
		t.Fatalf("new recovery tasks after exit handoff=%d want 1", got)
	}

	scheduler.take(t)()
	filesystem.stateMu.Lock()
	recoveredAt := filesystem.lastRefresh
	filesystem.stateMu.Unlock()
	if !recoveredAt.After(initialRefresh) {
		t.Fatalf("handoff recovery did not publish: got=%v initial=%v", recoveredAt, initialRefresh)
	}
}

func TestRefreshRecoveryPreservesPendingCreateUntilFirstCommit(t *testing.T) {
	ctx := context.Background()
	remote := &fakeRemote{memories: map[string]client.Memory{}}
	filesystem := NewFileSystem(remote, Options{})
	scheduler := &queuedRefreshScheduler{}
	filesystem.scheduleBackgroundRefresh = scheduler.schedule

	ready, resume := remote.pauseNextList()
	refreshDone := make(chan error, 1)
	go func() { refreshDone <- filesystem.Refresh(ctx) }()
	select {
	case <-ready:
	case <-time.After(time.Second):
		close(resume)
		t.Fatal("foreground Refresh did not pause on its empty snapshot")
	}
	handle, errno := filesystem.Create("a.md")
	if errno != 0 {
		close(resume)
		t.Fatalf("Create errno=%v", errno)
	}
	close(resume)
	if err := <-refreshDone; err != nil {
		t.Fatalf("foreground Refresh err=%v", err)
	}
	if got := scheduler.count(); got != 1 {
		t.Fatalf("scheduled recovery tasks=%d want 1", got)
	}
	scheduler.take(t)()
	if node, ok := filesystem.Index().Lookup("a.md"); !ok || node.IsDir || node.MemoryID != "" {
		t.Fatalf("recovery erased pending placeholder: node=%+v ok=%v", node, ok)
	}
	if _, errno := filesystem.Create("a.md"); errno != syscall.EEXIST {
		t.Fatalf("duplicate Create after recovery errno=%v want EEXIST", errno)
	}

	if errno := filesystem.Commit(ctx, handle); errno != 0 {
		t.Fatalf("first remote Create Commit errno=%v", errno)
	}
	remote.mu.Lock()
	delete(remote.memories, "a.md")
	remote.mu.Unlock()
	if err := filesystem.Refresh(ctx); err != nil {
		t.Fatalf("authoritative Refresh after first Commit err=%v", err)
	}
	if node, ok := filesystem.Index().Lookup("a.md"); ok {
		t.Fatalf("committed path survived authoritative deletion: node=%+v", node)
	}
}

func TestFailedCreatedCommitRemainsPendingAcrossRefresh(t *testing.T) {
	ctx := context.Background()
	remote := &fakeRemote{
		memories:  map[string]client.Memory{},
		createErr: &client.APIError{StatusCode: http.StatusServiceUnavailable, Code: "UNAVAILABLE"},
	}
	filesystem := NewFileSystem(remote, Options{})
	handle, errno := filesystem.Create("a.md")
	if errno != 0 {
		t.Fatalf("Create errno=%v", errno)
	}
	if errno := filesystem.Commit(ctx, handle); errno != syscall.EAGAIN {
		t.Fatalf("failed first Commit errno=%v want EAGAIN", errno)
	}
	if err := filesystem.Refresh(ctx); err != nil {
		t.Fatalf("Refresh after failed Commit err=%v", err)
	}
	if node, ok := filesystem.Index().Lookup("a.md"); !ok || node.IsDir || node.MemoryID != "" {
		t.Fatalf("Refresh erased failed-commit placeholder: node=%+v ok=%v", node, ok)
	}
	if _, errno := filesystem.Create("a.md"); errno != syscall.EEXIST {
		t.Fatalf("duplicate Create after failed Commit errno=%v want EEXIST", errno)
	}
}

func TestAuthoritativeMutationClearsAmbiguousPendingCreate(t *testing.T) {
	for _, operation := range []string{"commit", "rename", "delete"} {
		t.Run(operation, func(t *testing.T) {
			ctx := context.Background()
			remote := &ambiguousCreateRemote{fakeRemote: &fakeRemote{memories: map[string]client.Memory{}}}
			filesystem := NewFileSystem(remote, Options{})
			created, errno := filesystem.Create("a.md")
			if errno != 0 {
				t.Fatalf("Create errno=%v", errno)
			}
			if errno := filesystem.Commit(ctx, created); errno != syscall.EAGAIN {
				t.Fatalf("ambiguous first Commit errno=%v want EAGAIN", errno)
			}
			if err := filesystem.Refresh(ctx); err != nil {
				t.Fatalf("authoritative Refresh err=%v", err)
			}
			if node, ok := filesystem.Index().Lookup("a.md"); !ok || node.MemoryID == "" || node.LatestVersionID == "" {
				t.Fatalf("authoritative node hidden by pending placeholder: node=%+v ok=%v", node, ok)
			}

			switch operation {
			case "commit":
				authoritative, errno := filesystem.Open(ctx, "a.md")
				if errno != 0 {
					t.Fatalf("authoritative Open errno=%v", errno)
				}
				if _, err := authoritative.WriteAt([]byte("updated"), 0); err != nil {
					t.Fatal(err)
				}
				if errno := filesystem.Commit(ctx, authoritative); errno != 0 {
					t.Fatalf("authoritative Commit errno=%v", errno)
				}
				remote.mu.Lock()
				delete(remote.memories, "a.md")
				remote.mu.Unlock()
			case "rename":
				if _, errno := filesystem.Create("b.md"); errno != 0 {
					t.Fatalf("target pending Create errno=%v", errno)
				}
				if errno := filesystem.Rename(ctx, "a.md", "b.md", true); errno != 0 {
					t.Fatalf("Rename errno=%v", errno)
				}
			case "delete":
				if errno := filesystem.Delete(ctx, "a.md"); errno != 0 {
					t.Fatalf("Delete errno=%v", errno)
				}
			}

			filesystem.stateMu.Lock()
			_, pendingA := filesystem.pendingCreated["a.md"]
			_, pendingB := filesystem.pendingCreated["b.md"]
			filesystem.stateMu.Unlock()
			if pendingA || pendingB {
				t.Fatalf("pending markers survived %s: a=%v b=%v", operation, pendingA, pendingB)
			}
			if errno := filesystem.Commit(ctx, created); errno != syscall.ESTALE {
				t.Fatalf("old ambiguous Create handle retried after %s: errno=%v want ESTALE", operation, errno)
			}
			if err := filesystem.Refresh(ctx); err != nil {
				t.Fatalf("post-%s Refresh err=%v", operation, err)
			}
			if node, ok := filesystem.Index().Lookup("a.md"); ok {
				t.Fatalf("post-%s Refresh revived a.md: node=%+v", operation, node)
			}
			if operation == "rename" {
				if node, ok := filesystem.Index().Lookup("b.md"); !ok || node.MemoryID == "" {
					t.Fatalf("renamed authoritative node=%+v ok=%v", node, ok)
				}
			}
		})
	}
}

func TestRefreshRemainsForcedWhenCacheIsFresh(t *testing.T) {
	ctx := context.Background()
	remote := &fakeRemote{memories: map[string]client.Memory{}}
	filesystem := NewFileSystem(remote, Options{DirectoryTTL: time.Hour})
	if err := filesystem.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	if err := filesystem.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	remote.mu.Lock()
	calls := append([]string(nil), remote.calls...)
	remote.mu.Unlock()
	if got, want := fmt.Sprint(calls), "[List List]"; got != want {
		t.Fatalf("calls=%s want %s", got, want)
	}
}

func TestConcurrentRefreshAndMutationUsesRaceSafeRemoteSnapshots(t *testing.T) {
	ctx := context.Background()
	remote := &fakeRemote{memories: map[string]client.Memory{
		"a.md": {MemoryID: "mem_a", Path: "a.md", Content: "v000", LatestVersionID: "fmver_1"},
	}}
	refreshing := NewFileSystem(remote, Options{})
	mutating := NewFileSystem(remote, Options{})
	if err := refreshing.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	if err := mutating.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	handle, errno := mutating.Open(ctx, "a.md")
	if errno != 0 {
		t.Fatalf("open errno=%v", errno)
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		for range 100 {
			if err := refreshing.Refresh(ctx); err != nil {
				t.Errorf("refresh err=%v", err)
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		<-start
		for version := 1; version <= 100; version++ {
			content := []byte(fmt.Sprintf("v%03d", version))
			if _, err := handle.WriteAt(content, 0); err != nil {
				t.Errorf("write err=%v", err)
				return
			}
			if errno := mutating.Commit(ctx, handle); errno != 0 {
				t.Errorf("commit errno=%v", errno)
				return
			}
		}
	}()
	close(start)
	wg.Wait()

	remote.mu.Lock()
	winner := remote.memories["a.md"]
	remote.mu.Unlock()
	if winner.Content != "v100" || winner.LatestVersionID == "" || winner.LatestVersionID == "fmver_1" {
		t.Fatalf("winner=%+v", winner)
	}
	if memory := handle.Memory(); memory.MemoryID != "mem_a" || memory.LatestVersionID != winner.LatestVersionID {
		t.Fatalf("handle memory=%+v winner=%+v", memory, winner)
	}
}

func TestConcurrentMutationsAllowUnrelatedPaths(t *testing.T) {
	ctx := context.Background()
	remote := &blockingMutationRemote{fakeRemote: &fakeRemote{memories: map[string]client.Memory{
		"a.md": {MemoryID: "mem_a", Path: "a.md", Content: "a", LatestVersionID: "fmver_1"},
		"b.md": {MemoryID: "mem_b", Path: "b.md", Content: "b", LatestVersionID: "fmver_1"},
	}}}
	filesystem := NewFileSystem(remote, Options{})
	if err := filesystem.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	handles := make([]*Handle, 0, 2)
	for _, value := range []string{"a.md", "b.md"} {
		handle, errno := filesystem.Open(ctx, value)
		if errno != 0 {
			t.Fatalf("open %q errno=%v", value, errno)
		}
		if _, err := handle.WriteAt([]byte("updated"), 0); err != nil {
			t.Fatal(err)
		}
		handles = append(handles, handle)
	}

	started, finish := remote.pauseNextUpdates(2)
	results := make(chan syscall.Errno, 2)
	for _, handle := range handles {
		handle := handle
		go func() { results <- filesystem.Commit(ctx, handle) }()
	}
	seen := make(map[string]bool, 2)
	for range 2 {
		select {
		case itemID := <-started:
			seen[itemID] = true
		case <-time.After(time.Second):
			close(finish)
			for range 2 {
				<-results
			}
			t.Fatal("unrelated path mutations did not overlap")
		}
	}
	close(finish)
	if !seen["mem_a"] || !seen["mem_b"] {
		t.Fatalf("started updates=%v, want both items", seen)
	}
	for range 2 {
		if errno := <-results; errno != 0 {
			t.Fatalf("commit errno=%v", errno)
		}
	}
}

func TestConcurrentMutationsSerializeSamePath(t *testing.T) {
	ctx := context.Background()
	remote := &blockingMutationRemote{fakeRemote: &fakeRemote{memories: map[string]client.Memory{
		"a.md": {MemoryID: "mem_a", Path: "a.md", Content: "one", LatestVersionID: "fmver_1"},
	}}}
	filesystem := NewFileSystem(remote, Options{})
	if err := filesystem.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	first, errno := filesystem.Open(ctx, "a.md")
	if errno != 0 {
		t.Fatalf("first open errno=%v", errno)
	}
	second, errno := filesystem.Open(ctx, "a.md")
	if errno != 0 {
		t.Fatalf("second open errno=%v", errno)
	}
	if _, err := first.WriteAt([]byte("first"), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := second.WriteAt([]byte("second"), 0); err != nil {
		t.Fatal(err)
	}

	started, finish := remote.pauseNextUpdates(2)
	firstDone := make(chan syscall.Errno, 1)
	go func() { firstDone <- filesystem.Commit(ctx, first) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		close(finish)
		t.Fatal("first same-path update did not start")
	}
	secondDone := make(chan syscall.Errno, 1)
	go func() { secondDone <- filesystem.Commit(ctx, second) }()
	waitForPathLockCondition(t, func() bool {
		filesystem.pathLocks.mu.Lock()
		defer filesystem.pathLocks.mu.Unlock()
		entry := filesystem.pathLocks.entries["a.md"]
		return entry != nil && entry.refs == 2
	})
	select {
	case <-started:
		close(finish)
		t.Fatal("second same-path update reached the remote before the first completed")
	default:
	}
	close(finish)
	if errno := <-firstDone; errno != 0 {
		t.Fatalf("first commit errno=%v", errno)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("second same-path update did not start after first completed")
	}
	if errno := <-secondDone; errno != syscall.ESTALE {
		t.Fatalf("second stale commit errno=%v want ESTALE", errno)
	}
}

func mutationTestOptions(withReporter bool) Options {
	if !withReporter {
		return Options{}
	}
	return Options{MutationErrorReporter: func(MutationErrorEvent) {}}
}

func prepareMutationOperation(t *testing.T, remote Remote, opts Options, operation string) (*FileSystem, *Handle, func(context.Context) syscall.Errno) {
	t.Helper()
	filesystem := NewFileSystem(remote, opts)
	if operation != "Create" {
		if err := filesystem.Refresh(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	switch operation {
	case "Create":
		handle, errno := filesystem.Create("a.md")
		if errno != 0 {
			t.Fatalf("Create errno=%v", errno)
		}
		if _, err := handle.WriteAt([]byte("created"), 0); err != nil {
			t.Fatal(err)
		}
		return filesystem, handle, func(ctx context.Context) syscall.Errno { return filesystem.Commit(ctx, handle) }
	case "Update":
		handle, errno := filesystem.Open(context.Background(), "a.md")
		if errno != 0 {
			t.Fatalf("Open errno=%v", errno)
		}
		if err := handle.Truncate(0); err != nil {
			t.Fatal(err)
		}
		if _, err := handle.WriteAt([]byte("updated"), 0); err != nil {
			t.Fatal(err)
		}
		return filesystem, handle, func(ctx context.Context) syscall.Errno { return filesystem.Commit(ctx, handle) }
	case "Rename":
		return filesystem, nil, func(ctx context.Context) syscall.Errno { return filesystem.Rename(ctx, "a.md", "b.md", false) }
	case "Delete":
		return filesystem, nil, func(ctx context.Context) syscall.Errno { return filesystem.Delete(ctx, "a.md") }
	default:
		t.Fatalf("unknown mutation operation %q", operation)
		return nil, nil, nil
	}
}

func assertMutationPublished(t *testing.T, filesystem *FileSystem, handle *Handle, remote *fakeRemote, operation string) {
	t.Helper()
	switch operation {
	case "Create", "Update":
		if handle == nil {
			t.Fatalf("%s handle is nil", operation)
		}
		remote.mu.Lock()
		memory, exists := remote.memories["a.md"]
		remote.mu.Unlock()
		if !exists {
			t.Fatalf("remote %s result is missing", operation)
		}
		if got := handle.Memory(); got != memory {
			t.Fatalf("%s handle memory=%+v, want authoritative state %+v", operation, got, memory)
		}
		if handle.Dirty() {
			t.Fatalf("%s handle remained dirty after successful commit", operation)
		}
		node, exists := filesystem.Index().Lookup("a.md")
		if !exists || node.MemoryID != memory.MemoryID || node.LatestVersionID != memory.LatestVersionID || node.SHA256 != memory.ContentSHA256 {
			t.Fatalf("%s index node=%+v exists=%v, want returned state %+v", operation, node, exists, memory)
		}
	case "Rename":
		remote.mu.Lock()
		memory, exists := remote.memories["b.md"]
		remote.mu.Unlock()
		if !exists {
			t.Fatal("remote Rename result is missing")
		}
		if _, oldExists := filesystem.Index().Lookup("a.md"); oldExists {
			t.Fatal("Rename left the old path in the local index")
		}
		node, newExists := filesystem.Index().Lookup("b.md")
		if !newExists || node.MemoryID != memory.MemoryID || node.LatestVersionID != memory.LatestVersionID {
			t.Fatalf("Rename index node=%+v exists=%v, want returned state %+v", node, newExists, memory)
		}
	case "Delete":
		if _, exists := remote.memories["a.md"]; exists {
			t.Fatal("remote Delete did not remove the item")
		}
		if _, exists := filesystem.Index().Lookup("a.md"); exists {
			t.Fatal("Delete left the item in the local index")
		}
	}
}

func TestMutationSurvivesInterruptAfterRemoteStart(t *testing.T) {
	tests := []struct {
		operation    string
		withReporter bool
	}{
		{operation: "Create"},
		{operation: "Create", withReporter: true},
		{operation: "Update"},
		{operation: "Update", withReporter: true},
		{operation: "Rename"},
		{operation: "Delete"},
		{operation: "Delete", withReporter: true},
	}
	for _, test := range tests {
		name := test.operation + "/reporter=nil"
		if test.withReporter {
			name = test.operation + "/reporter=non-nil"
		}
		t.Run(name, func(t *testing.T) {
			memories := map[string]client.Memory{}
			if test.operation != "Create" {
				memories["a.md"] = client.Memory{MemoryID: "mem_a", Path: "a.md", Content: "old", ContentSHA256: "sha-old", LatestVersionID: "fmver_1"}
			}
			base := &fakeRemote{memories: memories}
			remote := newInterruptMutationRemote(base, test.operation)
			filesystem, handle, mutate := prepareMutationOperation(t, remote, mutationTestOptions(test.withReporter), test.operation)

			parent, cancel := context.WithCancel(context.Background())
			result := make(chan syscall.Errno, 1)
			go func() { result <- mutate(parent) }()
			select {
			case operation := <-remote.started:
				if operation != test.operation {
					t.Fatalf("started operation=%q want %q", operation, test.operation)
				}
			case <-time.After(time.Second):
				t.Fatalf("%s did not enter the remote mutation", test.operation)
			}
			cancel()
			if parent.Err() != context.Canceled {
				t.Fatalf("parent err=%v want context.Canceled", parent.Err())
			}
			close(remote.release)

			if err := <-remote.contextErr; err != nil {
				t.Fatalf("remote %s context was canceled after entry: %v", test.operation, err)
			}
			if errno := <-result; errno != 0 {
				t.Fatalf("%s errno=%v want success", test.operation, errno)
			}
			assertMutationPublished(t, filesystem, handle, base, test.operation)
		})
	}
}

func TestCanceledBeforeMutationDoesNotCallRemote(t *testing.T) {
	tests := []struct {
		operation    string
		withReporter bool
	}{
		{operation: "Create"},
		{operation: "Create", withReporter: true},
		{operation: "Update"},
		{operation: "Update", withReporter: true},
		{operation: "Rename"},
		{operation: "Delete"},
		{operation: "Delete", withReporter: true},
	}
	for _, test := range tests {
		name := test.operation + "/reporter=nil"
		if test.withReporter {
			name = test.operation + "/reporter=non-nil"
		}
		t.Run(name, func(t *testing.T) {
			memories := map[string]client.Memory{}
			if test.operation != "Create" {
				memories["a.md"] = client.Memory{MemoryID: "mem_a", Path: "a.md", Content: "old", ContentSHA256: "sha-old", LatestVersionID: "fmver_1"}
			}
			remote := &fakeRemote{memories: memories}
			reports := 0
			opts := Options{}
			if test.withReporter {
				opts.MutationErrorReporter = func(MutationErrorEvent) { reports++ }
			}
			_, _, mutate := prepareMutationOperation(t, remote, opts, test.operation)
			parent, cancel := context.WithCancel(context.Background())
			cancel()

			if errno := mutate(parent); errno == 0 {
				t.Fatalf("%s succeeded with an already-canceled parent", test.operation)
			}
			if calls := remote.callCount(test.operation); calls != 0 {
				t.Fatalf("remote %s calls=%d want 0", test.operation, calls)
			}
			if reports != 0 {
				t.Fatalf("reports=%d want 0 before remote %s entry", reports, test.operation)
			}
		})
	}
}

func TestMutationTimeoutDefaultsToOneMinute(t *testing.T) {
	filesystem := NewFileSystem(&fakeRemote{memories: map[string]client.Memory{}}, Options{})
	if got := filesystem.opts.MutationTimeout; got != time.Minute {
		t.Fatalf("MutationTimeout=%v want %v", got, time.Minute)
	}
}

func snapshotRemoteMemories(remote *fakeRemote) map[string]client.Memory {
	remote.mu.Lock()
	defer remote.mu.Unlock()
	snapshot := make(map[string]client.Memory, len(remote.memories))
	for path, memory := range remote.memories {
		snapshot[path] = memory
	}
	return snapshot
}

func assertRemoteMemoriesEqual(t *testing.T, remote *fakeRemote, want map[string]client.Memory) {
	t.Helper()
	got := snapshotRemoteMemories(remote)
	if len(got) != len(want) {
		t.Fatalf("remote memories=%+v want %+v", got, want)
	}
	for path, wantMemory := range want {
		if gotMemory, exists := got[path]; !exists || gotMemory != wantMemory {
			t.Fatalf("remote memory[%q]=%+v exists=%v want %+v", path, gotMemory, exists, wantMemory)
		}
	}
}

func TestMutationStopsAtConfiguredTimeout(t *testing.T) {
	tests := []struct {
		operation    string
		withReporter bool
	}{
		{operation: "Create"},
		{operation: "Create", withReporter: true},
		{operation: "Update"},
		{operation: "Update", withReporter: true},
		{operation: "Rename"},
		{operation: "Delete"},
		{operation: "Delete", withReporter: true},
	}
	for _, test := range tests {
		name := test.operation + "/reporter=nil"
		if test.withReporter {
			name = test.operation + "/reporter=non-nil"
		}
		t.Run(name, func(t *testing.T) {
			memories := map[string]client.Memory{}
			if test.operation != "Create" {
				memories["a.md"] = client.Memory{MemoryID: "mem_a", Path: "a.md", Content: "old", ContentSHA256: "sha-old", ContentSizeBytes: 3, LatestVersionID: "fmver_1"}
			}
			base := &fakeRemote{memories: memories}
			remote := newTimeoutMutationRemote(base, test.operation)
			var reports []MutationErrorEvent
			opts := Options{MutationTimeout: 20 * time.Millisecond}
			if test.withReporter {
				opts.MutationErrorReporter = func(event MutationErrorEvent) { reports = append(reports, event) }
			}
			filesystem, handle, mutate := prepareMutationOperation(t, remote, opts, test.operation)
			beforeRemote := snapshotRemoteMemories(base)
			beforeA, beforeAExists := filesystem.Index().Lookup("a.md")
			beforeB, beforeBExists := filesystem.Index().Lookup("b.md")
			var beforeHandle client.Memory
			var beforeDirty bool
			if handle != nil {
				beforeHandle = handle.Memory()
				beforeDirty = handle.Dirty()
			}
			filesystem.stateMu.Lock()
			beforeGeneration := filesystem.mutationGen
			beforePending := filesystem.pendingCreated["a.md"]
			filesystem.stateMu.Unlock()

			result := make(chan syscall.Errno, 1)
			go func() { result <- mutate(context.Background()) }()
			select {
			case operation := <-remote.started:
				if operation != test.operation {
					t.Fatalf("started operation=%q want %q", operation, test.operation)
				}
			case <-time.After(time.Second):
				t.Fatalf("%s did not enter the remote mutation", test.operation)
			}

			var errno syscall.Errno
			select {
			case errno = <-result:
			case <-time.After(time.Second):
				close(remote.abort)
				<-result
				t.Fatalf("%s did not stop at MutationTimeout", test.operation)
			}
			if errno == 0 {
				t.Fatalf("%s succeeded after MutationTimeout", test.operation)
			}
			if err := <-remote.contextErr; !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("remote %s context err=%v want context.DeadlineExceeded", test.operation, err)
			}

			wantReports := 0
			if test.withReporter {
				wantReports = 1
			}
			if len(reports) != wantReports {
				t.Fatalf("reports=%d want %d", len(reports), wantReports)
			}
			if test.withReporter {
				event := reports[0]
				if event.Operation != strings.ToLower(test.operation) || event.Category != "context" || event.Errno != int(errno) {
					t.Fatalf("timeout report=%+v errno=%v", event, errno)
				}
			}

			assertRemoteMemoriesEqual(t, base, beforeRemote)
			if afterA, afterAExists := filesystem.Index().Lookup("a.md"); afterAExists != beforeAExists || afterA != beforeA {
				t.Fatalf("index a.md=(%+v,%v) want (%+v,%v)", afterA, afterAExists, beforeA, beforeAExists)
			}
			if afterB, afterBExists := filesystem.Index().Lookup("b.md"); afterBExists != beforeBExists || afterB != beforeB {
				t.Fatalf("index b.md=(%+v,%v) want (%+v,%v)", afterB, afterBExists, beforeB, beforeBExists)
			}
			filesystem.stateMu.Lock()
			afterGeneration := filesystem.mutationGen
			afterPending := filesystem.pendingCreated["a.md"]
			filesystem.stateMu.Unlock()
			if afterGeneration != beforeGeneration {
				t.Fatalf("mutation generation=%d want unchanged %d", afterGeneration, beforeGeneration)
			}
			if afterPending != beforePending {
				t.Fatalf("pending create owner=%+v want unchanged %+v", afterPending, beforePending)
			}
			if handle != nil {
				if got := handle.Memory(); got != beforeHandle {
					t.Fatalf("%s timeout changed handle memory=%+v want %+v", test.operation, got, beforeHandle)
				}
				if got := handle.Dirty(); got != beforeDirty || !got {
					t.Fatalf("%s handle Dirty()=%v want unchanged dirty state %v", test.operation, got, beforeDirty)
				}
			}
		})
	}
}

func TestMutationGenerationAdvancesExactlyOnceOnSuccessfulLocalChange(t *testing.T) {
	ctx := context.Background()
	remote := &fakeRemote{memories: map[string]client.Memory{}}
	filesystem := NewFileSystem(remote, Options{})
	assertGeneration := func(want uint64) {
		t.Helper()
		filesystem.stateMu.Lock()
		got := filesystem.mutationGen
		filesystem.stateMu.Unlock()
		if got != want {
			t.Fatalf("mutationGen=%d want %d", got, want)
		}
	}

	handle, errno := filesystem.Create("a.md")
	if errno != 0 {
		t.Fatalf("create errno=%v", errno)
	}
	assertGeneration(1)
	if _, errno := filesystem.Create("a.md"); errno != syscall.EEXIST {
		t.Fatalf("duplicate create errno=%v want EEXIST", errno)
	}
	assertGeneration(1)
	if errno := filesystem.Commit(ctx, handle); errno != 0 {
		t.Fatalf("commit errno=%v", errno)
	}
	assertGeneration(2)
	if errno := filesystem.Commit(ctx, handle); errno != 0 {
		t.Fatalf("clean commit errno=%v", errno)
	}
	assertGeneration(2)
	if errno := filesystem.Rename(ctx, "missing.md", "other.md", false); errno != syscall.ENOENT {
		t.Fatalf("missing rename errno=%v want ENOENT", errno)
	}
	assertGeneration(2)
	if errno := filesystem.Rename(ctx, "a.md", "b.md", false); errno != 0 {
		t.Fatalf("rename errno=%v", errno)
	}
	assertGeneration(3)
	if errno := filesystem.Delete(ctx, "b.md"); errno != 0 {
		t.Fatalf("delete errno=%v", errno)
	}
	assertGeneration(4)
	if errno := filesystem.Delete(ctx, "b.md"); errno != syscall.ENOENT {
		t.Fatalf("second delete errno=%v want ENOENT", errno)
	}
	assertGeneration(4)
	if errno := filesystem.Mkdir("local"); errno != 0 {
		t.Fatalf("mkdir errno=%v", errno)
	}
	assertGeneration(5)
	if errno := filesystem.Mkdir("local"); errno != syscall.EEXIST {
		t.Fatalf("duplicate mkdir errno=%v want EEXIST", errno)
	}
	assertGeneration(5)
	if errno := filesystem.Rmdir("local"); errno != 0 {
		t.Fatalf("rmdir errno=%v", errno)
	}
	assertGeneration(6)
	if errno := filesystem.Rmdir("local"); errno != syscall.ENOENT {
		t.Fatalf("second rmdir errno=%v want ENOENT", errno)
	}
	assertGeneration(6)
}

func TestCreateFirstCommitGoesCreateThenUpdate(t *testing.T) {
	ctx := context.Background()
	remote := &fakeRemote{memories: map[string]client.Memory{}}
	filesystem := NewFileSystem(remote, Options{})
	handle, errno := filesystem.Create("notes/a.md")
	if errno != 0 {
		t.Fatalf("create errno=%v", errno)
	}
	if len(remote.calls) != 0 {
		t.Fatalf("local create must not call remote, calls=%v", remote.calls)
	}
	if _, err := handle.WriteAt([]byte("v1"), 0); err != nil {
		t.Fatal(err)
	}
	if errno := filesystem.Commit(ctx, handle); errno != 0 {
		t.Fatalf("first commit errno=%v", errno)
	}
	if _, err := handle.WriteAt([]byte("v2"), 0); err != nil {
		t.Fatal(err)
	}
	if errno := filesystem.Commit(ctx, handle); errno != 0 {
		t.Fatalf("second commit errno=%v", errno)
	}
	want := []string{"Create", "Update"}
	if fmt.Sprint(remote.calls) != fmt.Sprint(want) {
		t.Fatalf("calls=%v want %v", remote.calls, want)
	}
	updated := remote.memories["notes/a.md"]
	if remote.lastItemID != updated.MemoryID || remote.lastExpectedVersionID != "fmver_1" {
		t.Fatalf("update identity=(%q,%q), memory=%+v", remote.lastItemID, remote.lastExpectedVersionID, updated)
	}
	if remote.memories["notes/a.md"].Content != "v2" {
		t.Fatalf("memory=%+v", remote.memories["notes/a.md"])
	}
}

func TestCreateStaysNewUntilFirstCommitSucceeds(t *testing.T) {
	ctx := context.Background()
	remote := &fakeRemote{memories: map[string]client.Memory{}, createErr: &client.APIError{StatusCode: 503, Code: "UNAVAILABLE"}}
	filesystem := NewFileSystem(remote, Options{})
	handle, errno := filesystem.Create("a.md")
	if errno != 0 {
		t.Fatalf("create errno=%v", errno)
	}
	if errno := filesystem.Commit(ctx, handle); errno != syscall.EAGAIN {
		t.Fatalf("failed create commit errno=%v want EAGAIN", errno)
	}
	remote.createErr = nil
	if errno := filesystem.Commit(ctx, handle); errno != 0 {
		t.Fatalf("retry commit errno=%v", errno)
	}
	want := []string{"Create", "Create"}
	if fmt.Sprint(remote.calls) != fmt.Sprint(want) {
		t.Fatalf("calls=%v want %v", remote.calls, want)
	}
}

func TestCreateAllowsLocalSlackAboveAdvertisedLimit(t *testing.T) {
	remote := &fakeRemote{memories: map[string]client.Memory{}}
	filesystem := NewFileSystem(remote, Options{})
	const wantLocalLimit = 2100
	if LocalFileLimitSlack != 100 || MaxLocalFiles != wantLocalLimit {
		t.Fatalf("local limits: slack=%d max=%d want slack=100 max=%d", LocalFileLimitSlack, MaxLocalFiles, wantLocalLimit)
	}
	for i := 1; i <= wantLocalLimit; i++ {
		if _, errno := filesystem.Create(fmt.Sprintf("bulk/f%04d.md", i)); errno != 0 {
			t.Fatalf("create file %d errno=%v, want success through %d files", i, errno, wantLocalLimit)
		}
	}
	if _, errno := filesystem.Create("overflow.md"); errno != syscall.ENOSPC {
		t.Fatalf("create file %d errno=%v want ENOSPC", wantLocalLimit+1, errno)
	}
}

func TestStatfsAdvertisesConventionalLimitAndClampsFreeFiles(t *testing.T) {
	filesystem := NewFileSystem(&fakeRemote{memories: map[string]client.Memory{}}, Options{})
	root := &dirNode{filesystem: filesystem}
	const conventionalLimit = 2000
	seed := make([]client.Memory, 0, conventionalLimit+1)
	for i := 0; i < conventionalLimit+1; i++ {
		seed = append(seed, client.Memory{MemoryID: fmt.Sprintf("m%d", i), Path: fmt.Sprintf("bulk/f%04d.md", i)})
		if i+1 < conventionalLimit {
			continue
		}
		filesystem.Index().Rebuild(seed)
		var statfs fuse.StatfsOut
		if errno := root.Statfs(context.Background(), &statfs); errno != 0 {
			t.Fatalf("statfs at %d files errno=%v", i+1, errno)
		}
		if statfs.Files != conventionalLimit || statfs.Ffree != 0 {
			t.Fatalf("at %d files: files=%d ffree=%d want files=%d ffree=0", i+1, statfs.Files, statfs.Ffree, conventionalLimit)
		}
	}
}

func TestCommitMapsServerQuotaExceededToENOSPC(t *testing.T) {
	ctx := context.Background()
	remote := &fakeRemote{memories: map[string]client.Memory{}, createErr: &client.APIError{StatusCode: http.StatusConflict, Code: "QUOTA_EXCEEDED"}}
	filesystem := NewFileSystem(remote, Options{})
	handle, errno := filesystem.Create("a.md")
	if errno != 0 {
		t.Fatalf("create errno=%v", errno)
	}
	if errno := filesystem.Commit(ctx, handle); errno != syscall.ENOSPC {
		t.Fatalf("commit errno=%v want ENOSPC", errno)
	}
}

func TestStaleHandleCommitAfterRenameReturnsESTALEWithoutRecreate(t *testing.T) {
	ctx := context.Background()
	remote := &fakeRemote{memories: map[string]client.Memory{
		"a.md": {MemoryID: "mem_a", Path: "a.md", Content: "one", ContentSHA256: "sha-1", LatestVersionID: "fmver_1"},
	}}
	filesystem := NewFileSystem(remote, Options{})
	if err := filesystem.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	handle, errno := filesystem.Open(ctx, "a.md")
	if errno != 0 {
		t.Fatalf("open errno=%v", errno)
	}
	if _, err := handle.WriteAt([]byte("two"), 0); err != nil {
		t.Fatal(err)
	}
	if errno := filesystem.Rename(ctx, "a.md", "b.md", false); errno != 0 {
		t.Fatalf("rename errno=%v", errno)
	}
	if winner := remote.memories["b.md"]; winner.LatestVersionID == "" || winner.LatestVersionID == "fmver_1" {
		t.Fatalf("rename did not advance source resource version: %+v", winner)
	}
	remote.calls = nil
	remote.lastItemID = ""
	remote.lastExpectedVersionID = ""
	if errno := filesystem.Commit(ctx, handle); errno != syscall.ESTALE {
		t.Fatalf("stale commit errno=%v want ESTALE", errno)
	}
	if remote.lastItemID != "mem_a" || remote.lastExpectedVersionID != "fmver_1" {
		t.Fatalf("stale update identity=(%q,%q), want (mem_a,fmver_1)", remote.lastItemID, remote.lastExpectedVersionID)
	}
	if got, want := fmt.Sprint(remote.calls), "[Update]"; got != want {
		t.Fatalf("stale commit calls=%s want %s", got, want)
	}
	for _, call := range remote.calls {
		if call == "Create" {
			t.Fatalf("stale handle must not recreate the file, calls=%v", remote.calls)
		}
	}
	if _, exists := remote.memories["a.md"]; exists {
		t.Fatalf("old path resurrected: %+v", remote.memories)
	}
	if winner := remote.memories["b.md"]; winner.MemoryID != "mem_a" || winner.LatestVersionID != "fmver_2" || winner.Content != "one" {
		t.Fatalf("rename winner changed: %+v", winner)
	}
}

func TestStaleHandleCommitOnVersionConflictReturnsESTALEWithoutRecreate(t *testing.T) {
	ctx := context.Background()
	remote := &fakeRemote{memories: map[string]client.Memory{
		"a.md": {MemoryID: "mem_a", Path: "a.md", Content: "one", ContentSHA256: "sha-1", LatestVersionID: "fmver_1"},
	}}
	filesystem := NewFileSystem(remote, Options{})
	if err := filesystem.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	handle, errno := filesystem.Open(ctx, "a.md")
	if errno != 0 {
		t.Fatalf("open errno=%v", errno)
	}
	if _, err := handle.WriteAt([]byte("two"), 0); err != nil {
		t.Fatal(err)
	}
	// 并发写者抢先推进了同一 item 的资源版本，旧 handle 提交必须 ESTALE。
	remote.memories["a.md"] = client.Memory{MemoryID: "mem_a", Path: "a.md", Content: "other", ContentSHA256: "sha-other", LatestVersionID: "fmver_2"}
	remote.calls = nil
	remote.lastItemID = ""
	remote.lastExpectedVersionID = ""
	if errno := filesystem.Commit(ctx, handle); errno != syscall.ESTALE {
		t.Fatalf("conflict commit errno=%v want ESTALE", errno)
	}
	if remote.lastItemID != "mem_a" || remote.lastExpectedVersionID != "fmver_1" {
		t.Fatalf("stale update identity=(%q,%q), want (mem_a,fmver_1)", remote.lastItemID, remote.lastExpectedVersionID)
	}
	if got, want := fmt.Sprint(remote.calls), "[Update]"; got != want {
		t.Fatalf("stale commit calls=%s want %s", got, want)
	}
	for _, call := range remote.calls {
		if call == "Create" {
			t.Fatalf("conflicting handle must not recreate, calls=%v", remote.calls)
		}
	}
	if winner := remote.memories["a.md"]; winner.MemoryID != "mem_a" || winner.LatestVersionID != "fmver_2" || winner.Content != "other" {
		t.Fatalf("winner content clobbered: %+v", remote.memories["a.md"])
	}
}

func TestStaleHandleCommitAfterDeleteReturnsESTALEWithoutRecreate(t *testing.T) {
	ctx := context.Background()
	remote := &fakeRemote{memories: map[string]client.Memory{
		"a.md": {MemoryID: "mem_a", Path: "a.md", Content: "one", LatestVersionID: "fmver_1"},
	}}
	filesystem := NewFileSystem(remote, Options{})
	if err := filesystem.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	handle, errno := filesystem.Open(ctx, "a.md")
	if errno != 0 {
		t.Fatalf("open errno=%v", errno)
	}
	if _, err := handle.WriteAt([]byte("two"), 0); err != nil {
		t.Fatal(err)
	}
	if errno := filesystem.Delete(ctx, "a.md"); errno != 0 {
		t.Fatalf("delete errno=%v", errno)
	}
	remote.calls = nil
	remote.lastItemID = ""
	remote.lastExpectedVersionID = ""

	if errno := filesystem.Commit(ctx, handle); errno != syscall.ESTALE {
		t.Fatalf("stale commit errno=%v want ESTALE", errno)
	}
	if remote.lastItemID != "mem_a" || remote.lastExpectedVersionID != "fmver_1" {
		t.Fatalf("stale update identity=(%q,%q), want (mem_a,fmver_1)", remote.lastItemID, remote.lastExpectedVersionID)
	}
	if got, want := fmt.Sprint(remote.calls), "[Update]"; got != want {
		t.Fatalf("stale commit calls=%s want %s", got, want)
	}
	for _, call := range remote.calls {
		if call == "Create" {
			t.Fatalf("stale handle must not recreate deleted item, calls=%v", remote.calls)
		}
	}
	if _, _, exists := remote.findByID("mem_a"); exists {
		t.Fatalf("deleted item was recreated: %+v", remote.memories)
	}
}

func TestRenameToExistingTargetWithoutOverwriteReturnsEEXIST(t *testing.T) {
	ctx := context.Background()
	remote := &fakeRemote{memories: map[string]client.Memory{
		"a.md": {MemoryID: "mem_a", Path: "a.md", ContentSHA256: "s1", LatestVersionID: "fmver_1"},
		"b.md": {MemoryID: "mem_b", Path: "b.md", ContentSHA256: "s2", LatestVersionID: "fmver_2"},
	}}
	filesystem := NewFileSystem(remote, Options{})
	if err := filesystem.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	if errno := filesystem.Rename(ctx, "a.md", "b.md", false); errno != syscall.EEXIST {
		t.Fatalf("rename errno=%v want EEXIST", errno)
	}
	if node, ok := filesystem.Index().Lookup("a.md"); !ok || node.MemoryID != "mem_a" || node.LatestVersionID != "fmver_1" {
		t.Fatalf("failed rename changed source node=%+v ok=%v", node, ok)
	}
	if node, ok := filesystem.Index().Lookup("b.md"); !ok || node.MemoryID != "mem_b" || node.LatestVersionID != "fmver_2" {
		t.Fatalf("failed rename changed target node=%+v ok=%v", node, ok)
	}
}

func TestRenameOverwritePreservesSourceIdentityAndDisplacesTarget(t *testing.T) {
	ctx := context.Background()
	remote := &fakeRemote{memories: map[string]client.Memory{
		"a.md": {MemoryID: "mem_a", Path: "a.md", Content: "source", LatestVersionID: "fmver_1"},
		"b.md": {MemoryID: "mem_b", Path: "b.md", Content: "target", LatestVersionID: "fmver_2"},
	}}
	filesystem := NewFileSystem(remote, Options{})
	if err := filesystem.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	if errno := filesystem.Rename(ctx, "a.md", "b.md", true); errno != 0 {
		t.Fatalf("rename errno=%v", errno)
	}
	if _, ok := filesystem.Index().Lookup("a.md"); ok {
		t.Fatal("rename source remains in index")
	}
	remote.mu.Lock()
	_, sourcePathExists := remote.memories["a.md"]
	remoteState := fmt.Sprint(remote.memories)
	remote.mu.Unlock()
	if sourcePathExists {
		t.Fatalf("rename source remains remotely: %s", remoteState)
	}
	if remote.lastItemID != "mem_a" || remote.lastExpectedVersionID != "fmver_1" {
		t.Fatalf("rename identity=(%q,%q)", remote.lastItemID, remote.lastExpectedVersionID)
	}
	winner := remote.memories["b.md"]
	if winner.MemoryID != "mem_a" || winner.LatestVersionID == "" || winner.LatestVersionID == "fmver_1" || winner.Content != "source" {
		t.Fatalf("winner=%+v", winner)
	}
	if _, _, exists := remote.findByID("mem_b"); exists {
		t.Fatalf("overwritten target identity survived: %+v", remote.memories)
	}
	if node, ok := filesystem.Index().Lookup("b.md"); !ok || node.MemoryID != "mem_a" || node.LatestVersionID != winner.LatestVersionID {
		t.Fatalf("target node=%+v ok=%v", node, ok)
	}
}

func TestRenameToIndexedDirectoryReturnsEISDIRWithoutRemoteMutation(t *testing.T) {
	tests := []struct {
		name       string
		target     string
		child      client.Memory
		localMkdir bool
	}{
		{name: "local empty directory", target: "empty", localMkdir: true},
		{
			name:   "implicit directory with child",
			target: "dir",
			child:  client.Memory{MemoryID: "mem_child", Path: "dir/child.md", LatestVersionID: "fmver_2"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			memories := map[string]client.Memory{
				"source.md": {MemoryID: "mem_a", Path: "source.md", Content: "source", LatestVersionID: "fmver_1"},
			}
			if test.child.MemoryID != "" {
				memories[test.child.Path] = test.child
			}
			remote := &fakeRemote{memories: memories}
			filesystem := NewFileSystem(remote, Options{})
			if err := filesystem.Refresh(ctx); err != nil {
				t.Fatal(err)
			}
			if test.localMkdir {
				if errno := filesystem.Mkdir(test.target); errno != 0 {
					t.Fatalf("mkdir errno=%v", errno)
				}
			}
			remote.mu.Lock()
			remote.calls = nil
			remote.mu.Unlock()

			if errno := filesystem.Rename(ctx, "source.md", test.target, true); errno != syscall.EISDIR {
				t.Fatalf("rename errno=%v want EISDIR", errno)
			}

			remote.mu.Lock()
			calls := append([]string(nil), remote.calls...)
			source := remote.memories["source.md"]
			_, remoteTargetExists := remote.memories[test.target]
			child := remote.memories[test.child.Path]
			remote.mu.Unlock()
			if len(calls) != 0 {
				t.Fatalf("directory rejection reached remote, calls=%v", calls)
			}
			if source.MemoryID != "mem_a" || source.LatestVersionID != "fmver_1" || source.Path != "source.md" {
				t.Fatalf("remote source changed=%+v", source)
			}
			if remoteTargetExists {
				t.Fatalf("remote file created at directory target %q", test.target)
			}
			if node, ok := filesystem.Index().Lookup("source.md"); !ok || node.MemoryID != "mem_a" || node.LatestVersionID != "fmver_1" {
				t.Fatalf("source node=%+v ok=%v", node, ok)
			}
			if node, ok := filesystem.Index().Lookup(test.target); !ok || !node.IsDir {
				t.Fatalf("target node=%+v ok=%v", node, ok)
			}
			if test.child.MemoryID != "" {
				if child.MemoryID != test.child.MemoryID || child.LatestVersionID != test.child.LatestVersionID {
					t.Fatalf("remote child changed=%+v", child)
				}
				if node, ok := filesystem.Index().Lookup(test.child.Path); !ok || node.MemoryID != test.child.MemoryID || node.LatestVersionID != test.child.LatestVersionID {
					t.Fatalf("child node=%+v ok=%v", node, ok)
				}
			}
		})
	}
}

func TestRenameSamePathFileRemainsDelegated(t *testing.T) {
	ctx := context.Background()
	remote := &fakeRemote{memories: map[string]client.Memory{
		"a.md": {MemoryID: "mem_a", Path: "a.md", LatestVersionID: "fmver_1"},
	}}
	filesystem := NewFileSystem(remote, Options{})
	if err := filesystem.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	remote.mu.Lock()
	remote.calls = nil
	remote.mu.Unlock()

	if errno := filesystem.Rename(ctx, "a.md", "a.md", true); errno != 0 {
		t.Fatalf("same-path rename errno=%v", errno)
	}
	remote.mu.Lock()
	calls := append([]string(nil), remote.calls...)
	remote.mu.Unlock()
	if got, want := fmt.Sprint(calls), "[Rename]"; got != want {
		t.Fatalf("calls=%s want %s", got, want)
	}
}

func TestFileSystemReadOnlyAndErrnoMapping(t *testing.T) {
	filesystem := NewFileSystem(&fakeRemote{memories: map[string]client.Memory{}}, Options{ReadOnly: true})
	if _, errno := filesystem.Create("a"); errno != syscall.EROFS {
		t.Fatalf("create errno=%v", errno)
	}
	tests := []struct {
		err  error
		want syscall.Errno
	}{
		{&client.APIError{StatusCode: http.StatusNotFound}, syscall.ENOENT},
		{&client.APIError{StatusCode: http.StatusConflict}, syscall.ESTALE},
		// 服务端 409 code 不再靠 message/裸状态码猜语义；资源版本过期与
		// 瞬时锁冲突必须映射成不同 errno。
		{&client.APIError{StatusCode: http.StatusConflict, Code: "PRECONDITION_FAILED"}, syscall.ESTALE},
		{&client.APIError{StatusCode: http.StatusInternalServerError, Code: "PRECONDITION_FAILED"}, syscall.ESTALE},
		{&client.APIError{StatusCode: http.StatusConflict, Code: "SHA_MISMATCH"}, syscall.ESTALE},
		{&client.APIError{StatusCode: http.StatusConflict, Code: "PATH_EXISTS"}, syscall.EEXIST},
		{&client.APIError{StatusCode: http.StatusConflict, Code: "LOCK_CONFLICT"}, syscall.EAGAIN},
		{&client.APIError{StatusCode: http.StatusConflict, Code: "QUOTA_EXCEEDED"}, syscall.ENOSPC},
		{&client.APIError{StatusCode: http.StatusConflict, Code: "FILE_MEMORY_ARCHIVED"}, syscall.EROFS},
		{&client.APIError{StatusCode: http.StatusConflict, Code: "READ_ONLY_STORE"}, syscall.EROFS},
		{&client.APIError{StatusCode: http.StatusRequestEntityTooLarge}, syscall.EFBIG},
		// 鉴权失败(STS 临时凭证过期是典型来源)→ EACCES,与网络故障的 EIO 区分。
		{&client.APIError{StatusCode: http.StatusUnauthorized}, syscall.EACCES},
		{&client.APIError{StatusCode: http.StatusForbidden}, syscall.EACCES},
		{ErrInvalidUTF8, syscall.EILSEQ},
		{errors.New("network"), syscall.EIO},
	}
	for _, test := range tests {
		if got := errnoFor(test.err); got != test.want {
			t.Errorf("err=%v got=%v want=%v", test.err, got, test.want)
		}
	}
}

func TestServerReadOnlyHandshake(t *testing.T) {
	// 服务端 ListItems 返回 readOnly=true（file+ots 投影库）时，
	// 挂载须自动进入只读态，无需 --read-only。
	remote := &fakeRemote{memories: map[string]client.Memory{}, readOnly: true}
	filesystem := NewFileSystem(remote, Options{DirectoryTTL: time.Hour})
	if err := filesystem.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !filesystem.ReadOnly() {
		t.Fatal("server readOnly flag must flip the filesystem read-only")
	}
	if _, errno := filesystem.Create("x.md"); errno != syscall.EROFS {
		t.Fatalf("create errno=%v, want EROFS", errno)
	}
	if errno := filesystem.Mkdir("d"); errno != syscall.EROFS {
		t.Fatalf("mkdir errno=%v, want EROFS", errno)
	}
}

func TestRefreshPaginatesAtLimit100AndKeepsReadOnlyOR(t *testing.T) {
	remote := &fakeRemote{
		memories: map[string]client.Memory{},
		listPages: map[string]client.ListResult{
			"": {
				Memories:  []client.Memory{{MemoryID: "mem_a", Path: "notes/a.md", LatestVersionID: "fmver_1"}},
				NextToken: "next",
				ReadOnly:  true,
			},
			"next": {
				Memories: []client.Memory{{MemoryID: "mem_b", Path: "notes/b.md", LatestVersionID: "fmver_2"}},
			},
		},
	}
	filesystem := NewFileSystem(remote, Options{})
	if err := filesystem.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got, want := fmt.Sprint(remote.listLimits), "[100 100]"; got != want {
		t.Fatalf("list limits=%s want %s", got, want)
	}
	if got, want := fmt.Sprint(remote.listTokens), "[ next]"; got != want {
		t.Fatalf("list tokens=%s want %s", got, want)
	}
	if !filesystem.ReadOnly() {
		t.Fatal("readOnly=true on an earlier page must be retained")
	}
	if node, ok := filesystem.Index().Lookup("notes/a.md"); !ok || node.MemoryID != "mem_a" || node.LatestVersionID != "fmver_1" {
		t.Fatalf("first page node=%+v ok=%v", node, ok)
	}
	if node, ok := filesystem.Index().Lookup("notes/b.md"); !ok || node.MemoryID != "mem_b" || node.LatestVersionID != "fmver_2" {
		t.Fatalf("second page node=%+v ok=%v", node, ok)
	}
}

func TestServerReadOnlyCloseAfterReadSucceeds(t *testing.T) {
	// 回归（e2e 实测）：只读挂载上 cat/sha256sum 等纯读打开的 close 也会经
	// Flush → Commit；无脏数据必须成功返回 0，曾无条件 EROFS 导致
	// file+ots 投影库上任何读取都在关闭时报 "Read-only file system"。
	remote := &fakeRemote{memories: map[string]client.Memory{
		"a.md": {MemoryID: "m", Path: "a.md", Content: "one", ContentSHA256: "sha"},
	}, readOnly: true}
	filesystem := NewFileSystem(remote, Options{DirectoryTTL: time.Hour})
	if err := filesystem.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	handle, errno := filesystem.Open(context.Background(), "a.md")
	if errno != 0 {
		t.Fatalf("open errno=%v", errno)
	}
	if errno := filesystem.Commit(context.Background(), handle); errno != 0 {
		t.Fatalf("clean-handle close on read-only mount errno=%v, want 0", errno)
	}
	// 写拒绝语义不受影响：脏 handle 提交仍须 EROFS。
	if err := handle.Truncate(0); err != nil {
		t.Fatal(err)
	}
	if errno := filesystem.Commit(context.Background(), handle); errno != syscall.EROFS {
		t.Fatalf("dirty-handle commit on read-only mount errno=%v, want EROFS", errno)
	}
}

func TestCLIReadOnlyFlagIsFloor(t *testing.T) {
	// CLI --read-only 是地板：服务端 readOnly=false 不能把它降级为可写。
	remote := &fakeRemote{memories: map[string]client.Memory{}}
	filesystem := NewFileSystem(remote, Options{ReadOnly: true, DirectoryTTL: time.Hour})
	if err := filesystem.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !filesystem.ReadOnly() {
		t.Fatal("--read-only must not be lowered by server readOnly=false")
	}
}

func TestServerReadOnlyConcurrentRefresh(t *testing.T) {
	// -race 守卫：Refresh 写 serverReadOnly 与 FUSE goroutine 读 readOnly() 并发。
	// DirectoryTTL 用 1ns 强制每次 EnsureFresh 都触发 Refresh。
	remote := &fakeRemote{memories: map[string]client.Memory{}, readOnly: true}
	filesystem := NewFileSystem(remote, Options{DirectoryTTL: time.Nanosecond})
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_ = filesystem.EnsureFresh(context.Background())
				_ = filesystem.ReadOnly()
			}
		}()
	}
	wg.Wait()
	if !filesystem.ReadOnly() {
		t.Fatal("readOnly lost after concurrent refresh")
	}
}

func TestFileSystemKeepsLocalEmptyDirectoriesAfterFileDeletion(t *testing.T) {
	remote := &fakeRemote{memories: map[string]client.Memory{
		"a": {MemoryID: "mem_a", Path: "a", LatestVersionID: "fmver_1"},
	}}
	filesystem := NewFileSystem(remote, Options{})
	if err := filesystem.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if errno := filesystem.Mkdir("empty"); errno != 0 {
		t.Fatalf("mkdir errno=%v", errno)
	}
	if errno := filesystem.Delete(context.Background(), "a"); errno != 0 {
		t.Fatalf("delete errno=%v", errno)
	}
	if node, ok := filesystem.Index().Lookup("empty"); !ok || !node.IsDir {
		t.Fatalf("empty dir lost: node=%+v ok=%v", node, ok)
	}
}

func TestDirNodeRenameKeepsCachedFileNodeOpenableAtNewPath(t *testing.T) {
	ctx := context.Background()
	remote := &fakeRemote{memories: map[string]client.Memory{
		"a.md": {MemoryID: "mem_a", Path: "a.md", Content: "one", ContentSHA256: "sha", LatestVersionID: "fmver_1"},
	}}
	filesystem := NewFileSystem(remote, Options{})
	if err := filesystem.Refresh(ctx); err != nil {
		t.Fatal(err)
	}

	root := NewRoot(filesystem).(*dirNode)
	gofusefs.NewNodeFS(root, &gofusefs.Options{})
	file := &fileNode{filesystem: filesystem, path: "a.md"}
	inode := root.NewInode(ctx, file, gofusefs.StableAttr{Mode: fuse.S_IFREG})
	if !root.AddChild("a.md", inode, false) {
		t.Fatal("failed to add cached source inode")
	}

	if errno := root.Rename(ctx, "a.md", root, "b.md", 0); errno != 0 {
		t.Fatalf("rename errno=%v", errno)
	}
	if _, _, errno := file.Open(ctx, syscall.O_RDONLY); errno != 0 {
		t.Fatalf("open renamed cached inode errno=%v", errno)
	}
}

func TestLookupUsesPointStatWithoutList(t *testing.T) {
	remote := &fakeRemote{memories: map[string]client.Memory{
		"a.md": {MemoryID: "mem_a", Path: "a.md", ContentSizeBytes: 3, ContentSHA256: "sha-a", LatestVersionID: "fmver_1"},
	}}
	filesystem := NewFileSystem(remote, Options{MetadataTTL: time.Second})

	node, errno := filesystem.Lookup(context.Background(), "a.md")
	if errno != 0 {
		t.Fatalf("Lookup errno=%v", errno)
	}
	if node.MemoryID != "mem_a" || node.LatestVersionID != "fmver_1" || node.Size != 3 {
		t.Fatalf("Lookup node=%+v", node)
	}
	if got := remote.callCount("Stat"); got != 1 {
		t.Fatalf("Stat calls=%d want 1", got)
	}
	if got := remote.callCount("List"); got != 0 {
		t.Fatalf("List calls=%d want 0", got)
	}
}

func TestReaddirStillRefreshesWithList(t *testing.T) {
	remote := &fakeRemote{memories: map[string]client.Memory{
		"a.md": {MemoryID: "mem_a", Path: "a.md", LatestVersionID: "fmver_1"},
	}}
	filesystem := NewFileSystem(remote, Options{DirectoryTTL: time.Minute})

	if err := filesystem.EnsureFresh(context.Background()); err != nil {
		t.Fatalf("EnsureFresh err=%v", err)
	}
	if got := remote.callCount("List"); got != 1 {
		t.Fatalf("List calls=%d want 1", got)
	}
	if got := remote.callCount("Stat"); got != 0 {
		t.Fatalf("Stat calls=%d want 0", got)
	}
}

func TestMetadataPositiveAndNegativeTTLsAreIndependent(t *testing.T) {
	now := time.Date(2026, time.August, 22, 10, 0, 0, 0, time.UTC)
	remote := &fakeRemote{memories: map[string]client.Memory{
		"present.md": {MemoryID: "mem_present", Path: "present.md", LatestVersionID: "fmver_1"},
	}}
	filesystem := NewFileSystem(remote, Options{
		MetadataTTL:  5 * time.Second,
		NegativeTTL:  time.Second,
		DirectoryTTL: 10 * time.Second,
		Now:          func() time.Time { return now },
	})

	if _, errno := filesystem.Lookup(context.Background(), "present.md"); errno != 0 {
		t.Fatalf("first present Lookup errno=%v", errno)
	}
	if _, errno := filesystem.Lookup(context.Background(), "missing.md"); errno != syscall.ENOENT {
		t.Fatalf("first missing Lookup errno=%v want ENOENT", errno)
	}
	now = now.Add(2 * time.Second)
	if _, errno := filesystem.Lookup(context.Background(), "present.md"); errno != 0 {
		t.Fatalf("cached present Lookup errno=%v", errno)
	}
	if _, errno := filesystem.Lookup(context.Background(), "missing.md"); errno != syscall.ENOENT {
		t.Fatalf("expired missing Lookup errno=%v want ENOENT", errno)
	}
	if got := remote.callCount("Stat"); got != 3 {
		t.Fatalf("Stat calls=%d want 3", got)
	}
	if got := remote.callCount("List"); got != 0 {
		t.Fatalf("List calls=%d want 0", got)
	}
}

func TestLookupKnownDirectoriesDoesNotCallRemoteStat(t *testing.T) {
	remote := &fakeRemote{memories: map[string]client.Memory{
		"snapshot/a.md": {MemoryID: "mem_a", Path: "snapshot/a.md", LatestVersionID: "fmver_1"},
	}}
	filesystem := NewFileSystem(remote, Options{})
	if err := filesystem.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh err=%v", err)
	}
	if errno := filesystem.Mkdir("local"); errno != 0 {
		t.Fatalf("Mkdir errno=%v", errno)
	}
	for _, value := range []string{"snapshot", "local"} {
		node, errno := filesystem.Lookup(context.Background(), value)
		if errno != 0 || !node.IsDir {
			t.Fatalf("Lookup(%q) node=%+v errno=%v", value, node, errno)
		}
	}
	if got := remote.callCount("Stat"); got != 0 {
		t.Fatalf("directory Lookup Stat calls=%d want 0", got)
	}
}

func TestValidStat404RemovesStaleFileAndCachesNegative(t *testing.T) {
	now := time.Date(2026, time.August, 22, 10, 0, 0, 0, time.UTC)
	remote := &fakeRemote{memories: map[string]client.Memory{
		"gone.md": {MemoryID: "mem_gone", Path: "gone.md", LatestVersionID: "fmver_1"},
	}}
	filesystem := NewFileSystem(remote, Options{
		MetadataTTL:  time.Second,
		NegativeTTL:  time.Minute,
		DirectoryTTL: time.Minute,
		Now:          func() time.Time { return now },
	})
	if err := filesystem.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh err=%v", err)
	}
	now = now.Add(2 * time.Second)
	remote.mu.Lock()
	delete(remote.memories, "gone.md")
	remote.mu.Unlock()

	if _, errno := filesystem.Lookup(context.Background(), "gone.md"); errno != syscall.ENOENT {
		t.Fatalf("Lookup errno=%v want ENOENT", errno)
	}
	if node, ok := filesystem.Index().Lookup("gone.md"); ok {
		t.Fatalf("404 left stale node=%+v", node)
	}
	if _, errno := filesystem.Lookup(context.Background(), "gone.md"); errno != syscall.ENOENT {
		t.Fatalf("negative cached Lookup errno=%v want ENOENT", errno)
	}
	if got := remote.callCount("Stat"); got != 1 {
		t.Fatalf("Stat calls=%d want 1", got)
	}
}

func TestNon404StatErrorsAreNotCached(t *testing.T) {
	remote := &statErrorRemote{
		fakeRemote: &fakeRemote{memories: map[string]client.Memory{}},
		err:        &client.APIError{StatusCode: http.StatusServiceUnavailable, Code: "UNAVAILABLE"},
	}
	filesystem := NewFileSystem(remote, Options{NegativeTTL: time.Minute})
	for range 2 {
		if _, errno := filesystem.Lookup(context.Background(), "retry.md"); errno != syscall.EAGAIN {
			t.Fatalf("Lookup errno=%v want EAGAIN", errno)
		}
	}
	if got := remote.callCount("Stat"); got != 2 {
		t.Fatalf("Stat calls=%d want 2", got)
	}
}

func TestLateStatCannotOverwriteNewerOpenOrSnapshot(t *testing.T) {
	for _, newer := range []string{"Open", "Refresh"} {
		t.Run(newer, func(t *testing.T) {
			v1 := client.Memory{MemoryID: "mem_a", Path: "a.md", Content: "one", ContentSHA256: "sha-v1", LatestVersionID: "fmver_1"}
			v2 := client.Memory{MemoryID: "mem_a", Path: "a.md", Content: "two", ContentSHA256: "sha-v2", LatestVersionID: "fmver_2"}
			remote := &blockingStatRemote{fakeRemote: &fakeRemote{memories: map[string]client.Memory{"a.md": v1}}}
			filesystem := NewFileSystem(remote, Options{})
			started, finish := remote.pauseNextStats(1)
			type statResult struct {
				node  Node
				errno syscall.Errno
			}
			done := make(chan statResult, 1)
			go func() {
				node, errno := filesystem.Stat(context.Background(), "a.md")
				done <- statResult{node: node, errno: errno}
			}()
			select {
			case <-started:
			case <-time.After(time.Second):
				close(finish)
				t.Fatal("older Stat did not pause")
			}
			remote.mu.Lock()
			remote.memories["a.md"] = v2
			remote.mu.Unlock()
			if newer == "Open" {
				if _, errno := filesystem.Open(context.Background(), "a.md"); errno != 0 {
					close(finish)
					t.Fatalf("Open errno=%v", errno)
				}
			} else if err := filesystem.Refresh(context.Background()); err != nil {
				close(finish)
				t.Fatalf("Refresh err=%v", err)
			}
			close(finish)
			result := <-done
			if result.errno != 0 || result.node.LatestVersionID != "fmver_1" {
				t.Fatalf("older Stat result=%+v errno=%v", result.node, result.errno)
			}
			if node, ok := filesystem.Index().Lookup("a.md"); !ok || node.LatestVersionID != "fmver_2" || node.SHA256 != "sha-v2" {
				t.Fatalf("older Stat overwrote newer %s: node=%+v ok=%v", newer, node, ok)
			}
		})
	}
}

func TestLateStat404CannotOverwriteNewerOpenListOrMutation(t *testing.T) {
	for _, newer := range []string{"Open", "Refresh", "Create"} {
		t.Run(newer, func(t *testing.T) {
			remote := &blockingStatRemote{fakeRemote: &fakeRemote{memories: map[string]client.Memory{}}}
			filesystem := NewFileSystem(remote, Options{NegativeTTL: time.Minute})
			started, finish := remote.pauseNextStats(1)
			done := make(chan syscall.Errno, 1)
			go func() {
				_, errno := filesystem.Stat(context.Background(), "a.md")
				done <- errno
			}()
			select {
			case <-started:
			case <-time.After(time.Second):
				close(finish)
				t.Fatal("older Stat 404 did not pause")
			}
			if newer == "Create" {
				if _, errno := filesystem.Create("a.md"); errno != 0 {
					close(finish)
					t.Fatalf("Create errno=%v", errno)
				}
			} else {
				remote.mu.Lock()
				remote.memories["a.md"] = client.Memory{MemoryID: "mem_a", Path: "a.md", Content: "new", ContentSHA256: "sha-new", LatestVersionID: "fmver_2"}
				remote.mu.Unlock()
				if newer == "Open" {
					if _, errno := filesystem.Open(context.Background(), "a.md"); errno != 0 {
						close(finish)
						t.Fatalf("Open errno=%v", errno)
					}
				} else if err := filesystem.Refresh(context.Background()); err != nil {
					close(finish)
					t.Fatalf("Refresh err=%v", err)
				}
			}
			close(finish)
			if errno := <-done; errno != syscall.ENOENT {
				t.Fatalf("older Stat errno=%v want ENOENT", errno)
			}
			node, ok := filesystem.Index().Lookup("a.md")
			if !ok || node.IsDir || newer != "Create" && node.MemoryID != "mem_a" || newer == "Create" && node.MemoryID != "" {
				t.Fatalf("older 404 overwrote newer %s: node=%+v ok=%v", newer, node, ok)
			}
			filesystem.stateMu.Lock()
			_, negative := filesystem.negativeExpiry["a.md"]
			filesystem.stateMu.Unlock()
			if negative {
				t.Fatalf("older 404 installed negative cache after newer %s", newer)
			}
		})
	}
}

func TestLateStat404CannotDeleteNewerLocalDirectory(t *testing.T) {
	remote := &blockingStatRemote{fakeRemote: &fakeRemote{memories: map[string]client.Memory{}}}
	filesystem := NewFileSystem(remote, Options{})
	started, finish := remote.pauseNextStats(1)
	done := make(chan syscall.Errno, 1)
	go func() {
		_, errno := filesystem.Stat(context.Background(), "local")
		done <- errno
	}()
	<-started
	if errno := filesystem.Mkdir("local"); errno != 0 {
		close(finish)
		t.Fatalf("Mkdir errno=%v", errno)
	}
	close(finish)
	if errno := <-done; errno != syscall.ENOENT {
		t.Fatalf("older Stat errno=%v want ENOENT", errno)
	}
	if node, ok := filesystem.Index().Lookup("local"); !ok || !node.IsDir {
		t.Fatalf("older 404 deleted local directory: node=%+v ok=%v", node, ok)
	}
}

func seedMetadataCacheForTest(filesystem *FileSystem, values ...string) {
	filesystem.stateMu.Lock()
	defer filesystem.stateMu.Unlock()
	for _, value := range values {
		filesystem.positiveExpiry[value] = filesystem.opts.Now().Add(time.Hour)
		filesystem.setNegativeLocked(value, filesystem.opts.Now().Add(time.Hour))
	}
}

func assertMetadataCacheInvalidated(t *testing.T, filesystem *FileSystem, values ...string) {
	t.Helper()
	filesystem.stateMu.Lock()
	defer filesystem.stateMu.Unlock()
	for _, value := range values {
		if _, ok := filesystem.positiveExpiry[value]; ok {
			t.Errorf("positive cache for %q was not invalidated", value)
		}
		if _, ok := filesystem.negativeExpiry[value]; ok {
			t.Errorf("negative cache for %q was not invalidated", value)
		}
	}
}

func TestMutationsInvalidateRelatedPositiveAndNegativeMetadataCaches(t *testing.T) {
	t.Run("CreateAndAbandon", func(t *testing.T) {
		filesystem := NewFileSystem(&fakeRemote{memories: map[string]client.Memory{}}, Options{})
		seedMetadataCacheForTest(filesystem, "a.md")
		handle, errno := filesystem.Create("a.md")
		if errno != 0 {
			t.Fatalf("Create errno=%v", errno)
		}
		assertMetadataCacheInvalidated(t, filesystem, "a.md")
		seedMetadataCacheForTest(filesystem, "a.md")
		filesystem.AbandonCreated(handle)
		assertMetadataCacheInvalidated(t, filesystem, "a.md")
	})

	t.Run("CreateCommit", func(t *testing.T) {
		filesystem := NewFileSystem(&fakeRemote{memories: map[string]client.Memory{}}, Options{})
		handle, errno := filesystem.Create("a.md")
		if errno != 0 {
			t.Fatalf("Create errno=%v", errno)
		}
		if _, err := handle.WriteAt([]byte("a"), 0); err != nil {
			t.Fatal(err)
		}
		seedMetadataCacheForTest(filesystem, "a.md")
		if errno := filesystem.Commit(context.Background(), handle); errno != 0 {
			t.Fatalf("Commit errno=%v", errno)
		}
		assertMetadataCacheInvalidated(t, filesystem, "a.md")
	})

	t.Run("Update", func(t *testing.T) {
		remote := &fakeRemote{memories: map[string]client.Memory{
			"a.md": {MemoryID: "mem_a", Path: "a.md", Content: "a", LatestVersionID: "fmver_1"},
		}}
		filesystem := NewFileSystem(remote, Options{})
		handle, errno := filesystem.Open(context.Background(), "a.md")
		if errno != 0 {
			t.Fatalf("Open errno=%v", errno)
		}
		if _, err := handle.WriteAt([]byte("b"), 0); err != nil {
			t.Fatal(err)
		}
		seedMetadataCacheForTest(filesystem, "a.md")
		if errno := filesystem.Commit(context.Background(), handle); errno != 0 {
			t.Fatalf("Commit errno=%v", errno)
		}
		assertMetadataCacheInvalidated(t, filesystem, "a.md")
	})

	t.Run("Rename", func(t *testing.T) {
		remote := &fakeRemote{memories: map[string]client.Memory{
			"a.md": {MemoryID: "mem_a", Path: "a.md", LatestVersionID: "fmver_1"},
		}}
		filesystem := NewFileSystem(remote, Options{})
		if err := filesystem.Refresh(context.Background()); err != nil {
			t.Fatal(err)
		}
		seedMetadataCacheForTest(filesystem, "a.md", "b.md")
		if errno := filesystem.Rename(context.Background(), "a.md", "b.md", false); errno != 0 {
			t.Fatalf("Rename errno=%v", errno)
		}
		assertMetadataCacheInvalidated(t, filesystem, "a.md", "b.md")
	})

	t.Run("Delete", func(t *testing.T) {
		remote := &fakeRemote{memories: map[string]client.Memory{
			"a.md": {MemoryID: "mem_a", Path: "a.md", LatestVersionID: "fmver_1"},
		}}
		filesystem := NewFileSystem(remote, Options{})
		if err := filesystem.Refresh(context.Background()); err != nil {
			t.Fatal(err)
		}
		seedMetadataCacheForTest(filesystem, "a.md")
		if errno := filesystem.Delete(context.Background(), "a.md"); errno != 0 {
			t.Fatalf("Delete errno=%v", errno)
		}
		assertMetadataCacheInvalidated(t, filesystem, "a.md")
	})
}

func TestNegativeMetadataCacheAndObservationsAreBounded(t *testing.T) {
	const maxEntries = 4096
	remote := &fakeRemote{memories: map[string]client.Memory{}}
	filesystem := NewFileSystem(remote, Options{NegativeTTL: time.Hour})

	for index := range 10_000 {
		value := fmt.Sprintf("missing-%05d.md", index)
		if _, errno := filesystem.Lookup(context.Background(), value); errno != syscall.ENOENT {
			t.Fatalf("Lookup(%q) errno=%v want ENOENT", value, errno)
		}
	}

	filesystem.stateMu.Lock()
	negativeCount := len(filesystem.negativeExpiry)
	observationCount := len(filesystem.pointObservations)
	filesystem.stateMu.Unlock()
	if negativeCount > maxEntries {
		t.Fatalf("negative cache entries=%d want <=%d", negativeCount, maxEntries)
	}
	if observationCount > maxEntries {
		t.Fatalf("point observations=%d want <=%d after unique 404s", observationCount, maxEntries)
	}
}

func TestRefreshDropsExpiredNegativeMetadataWithoutRenewingIt(t *testing.T) {
	now := time.Date(2026, time.August, 22, 12, 0, 0, 0, time.UTC)
	remote := &fakeRemote{memories: map[string]client.Memory{}}
	filesystem := NewFileSystem(remote, Options{
		NegativeTTL: time.Second,
		Now:         func() time.Time { return now },
	})
	for index := range 8 {
		value := fmt.Sprintf("missing-%d.md", index)
		if _, errno := filesystem.Lookup(context.Background(), value); errno != syscall.ENOENT {
			t.Fatalf("Lookup(%q) errno=%v want ENOENT", value, errno)
		}
	}
	now = now.Add(2 * time.Second)

	if err := filesystem.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh err=%v", err)
	}
	filesystem.stateMu.Lock()
	negativeCount := len(filesystem.negativeExpiry)
	negativeObservations := 0
	for _, observation := range filesystem.pointObservations {
		if !observation.exists {
			negativeObservations++
		}
	}
	filesystem.stateMu.Unlock()
	if negativeCount != 0 {
		t.Fatalf("expired negative cache entries after Refresh=%d want 0", negativeCount)
	}
	if negativeObservations != 0 {
		t.Fatalf("expired negative point observations after Refresh=%d want 0", negativeObservations)
	}
}

func TestNegativeEvictionFencesPausedRefreshSnapshot(t *testing.T) {
	remote := &fakeRemote{memories: map[string]client.Memory{
		"gone.md": {MemoryID: "mem_gone", Path: "gone.md", LatestVersionID: "fmver_1"},
	}}
	filesystem := NewFileSystem(remote, Options{NegativeTTL: time.Hour})
	scheduler := &queuedRefreshScheduler{}
	filesystem.scheduleBackgroundRefresh = scheduler.schedule

	ready, resume := remote.pauseNextList()
	refreshDone := make(chan error, 1)
	go func() { refreshDone <- filesystem.Refresh(context.Background()) }()
	select {
	case <-ready:
	case <-time.After(time.Second):
		close(resume)
		t.Fatal("Refresh did not pause after capturing the stale file")
	}
	remote.mu.Lock()
	delete(remote.memories, "gone.md")
	remote.mu.Unlock()
	if _, errno := filesystem.Lookup(context.Background(), "gone.md"); errno != syscall.ENOENT {
		close(resume)
		t.Fatalf("gone Lookup errno=%v want ENOENT", errno)
	}
	for index := range maxNegativeEntries {
		value := fmt.Sprintf("evict-%05d.md", index)
		if _, errno := filesystem.Lookup(context.Background(), value); errno != syscall.ENOENT {
			close(resume)
			t.Fatalf("Lookup(%q) errno=%v want ENOENT", value, errno)
		}
	}
	filesystem.stateMu.Lock()
	_, cached := filesystem.negativeExpiry["gone.md"]
	_, observed := filesystem.pointObservations["gone.md"]
	filesystem.stateMu.Unlock()
	if cached || observed {
		close(resume)
		t.Fatalf("oldest negative was not evicted: cached=%v observed=%v", cached, observed)
	}

	close(resume)
	if err := <-refreshDone; err != nil {
		t.Fatalf("Refresh err=%v", err)
	}
	if node, exists := filesystem.Index().Lookup("gone.md"); exists {
		t.Fatalf("paused stale snapshot resurrected evicted 404 file: node=%+v", node)
	}
	if got := scheduler.count(); got != 1 {
		t.Fatalf("recovery tasks=%d want 1 after observation eviction", got)
	}
	scheduler.take(t)()
	if node, exists := filesystem.Index().Lookup("gone.md"); exists {
		t.Fatalf("recovery retained deleted file: node=%+v", node)
	}
}

func TestConcurrentColdPointStatsCoalesceByPath(t *testing.T) {
	for _, test := range []struct {
		name       string
		memory     *client.Memory
		wantErrno  syscall.Errno
		wantMemory string
	}{
		{
			name:       "positive",
			memory:     &client.Memory{MemoryID: "mem_a", Path: "a.md", LatestVersionID: "fmver_1"},
			wantMemory: "mem_a",
		},
		{name: "miss", wantErrno: syscall.ENOENT},
	} {
		t.Run(test.name, func(t *testing.T) {
			memories := map[string]client.Memory{}
			if test.memory != nil {
				memories["a.md"] = *test.memory
			}
			remote := &blockingStatRemote{fakeRemote: &fakeRemote{memories: memories}}
			filesystem := NewFileSystem(remote, Options{MetadataTTL: time.Hour, NegativeTTL: time.Hour})
			started, finish := remote.pauseNextStats(64)
			start := make(chan struct{})
			type lookupResult struct {
				node  Node
				errno syscall.Errno
			}
			results := make(chan lookupResult, 64)
			for index := range 64 {
				go func() {
					<-start
					var node Node
					var errno syscall.Errno
					if index%2 == 0 {
						node, errno = filesystem.Lookup(context.Background(), "a.md")
					} else {
						node, errno = filesystem.Stat(context.Background(), "a.md")
					}
					results <- lookupResult{node: node, errno: errno}
				}()
			}
			close(start)
			select {
			case <-started:
			case <-time.After(time.Second):
				close(finish)
				t.Fatal("cold Stat did not start")
			}
			// Give every caller a chance to reach the blocked cold path. Without
			// coalescing they each enter Remote.Stat; with coalescing they wait on
			// the one shared flight.
			time.Sleep(50 * time.Millisecond)
			close(finish)
			for range 64 {
				result := <-results
				if result.errno != test.wantErrno || result.node.MemoryID != test.wantMemory {
					t.Fatalf("result node=%+v errno=%v want memory=%q errno=%v", result.node, result.errno, test.wantMemory, test.wantErrno)
				}
			}
			if got := remote.callCount("Stat"); got != 1 {
				t.Fatalf("same-path Stat calls=%d want 1", got)
			}
		})
	}
}

func TestConcurrentColdPointStatsKeepDifferentPathsParallel(t *testing.T) {
	remote := &blockingStatRemote{fakeRemote: &fakeRemote{memories: map[string]client.Memory{
		"a.md": {MemoryID: "mem_a", Path: "a.md", LatestVersionID: "fmver_1"},
		"b.md": {MemoryID: "mem_b", Path: "b.md", LatestVersionID: "fmver_1"},
	}}}
	filesystem := NewFileSystem(remote, Options{})
	started, finish := remote.pauseNextStats(2)
	done := make(chan syscall.Errno, 2)
	for _, value := range []string{"a.md", "b.md"} {
		go func() {
			_, errno := filesystem.Lookup(context.Background(), value)
			done <- errno
		}()
	}
	seen := map[string]bool{}
	for range 2 {
		select {
		case value := <-started:
			seen[value] = true
		case <-time.After(time.Second):
			close(finish)
			t.Fatalf("different-path Stats did not overlap: started=%v", seen)
		}
	}
	close(finish)
	for range 2 {
		if errno := <-done; errno != 0 {
			t.Fatalf("Lookup errno=%v", errno)
		}
	}
	if !seen["a.md"] || !seen["b.md"] || remote.callCount("Stat") != 2 {
		t.Fatalf("different path starts=%v Stat calls=%d", seen, remote.callCount("Stat"))
	}
}

func TestPointStatDoesNotReuseFlightAcrossMutationGeneration(t *testing.T) {
	v1 := client.Memory{MemoryID: "mem_a", Path: "a.md", ContentSHA256: "sha-v1", LatestVersionID: "fmver_1"}
	v2 := client.Memory{MemoryID: "mem_a", Path: "a.md", ContentSHA256: "sha-v2", LatestVersionID: "fmver_2"}
	remote := &blockingStatRemote{fakeRemote: &fakeRemote{memories: map[string]client.Memory{"a.md": v1}}}
	filesystem := NewFileSystem(remote, Options{})
	started, finish := remote.pauseNextStats(1)
	type statResult struct {
		node  Node
		errno syscall.Errno
	}
	oldDone := make(chan statResult, 1)
	go func() {
		node, errno := filesystem.Stat(context.Background(), "a.md")
		oldDone <- statResult{node: node, errno: errno}
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		close(finish)
		t.Fatal("old-generation Stat did not start")
	}
	if errno := filesystem.Mkdir("local"); errno != 0 {
		close(finish)
		t.Fatalf("Mkdir errno=%v", errno)
	}
	remote.mu.Lock()
	remote.memories["a.md"] = v2
	remote.mu.Unlock()

	newDone := make(chan statResult, 1)
	go func() {
		node, errno := filesystem.Stat(context.Background(), "a.md")
		newDone <- statResult{node: node, errno: errno}
	}()
	var newer statResult
	select {
	case newer = <-newDone:
	case <-time.After(200 * time.Millisecond):
		close(finish)
		<-oldDone
		<-newDone
		t.Fatal("new-generation Stat reused the blocked old-generation flight")
	}
	if newer.errno != 0 || newer.node.LatestVersionID != "fmver_2" {
		close(finish)
		<-oldDone
		t.Fatalf("new-generation Stat node=%+v errno=%v", newer.node, newer.errno)
	}
	close(finish)
	older := <-oldDone
	if older.errno != 0 || older.node.LatestVersionID != "fmver_1" {
		t.Fatalf("old-generation caller node=%+v errno=%v", older.node, older.errno)
	}
	if node, exists := filesystem.Index().Lookup("a.md"); !exists || node.LatestVersionID != "fmver_2" {
		t.Fatalf("old flight rolled back newer generation: node=%+v exists=%v", node, exists)
	}
	if got := remote.callCount("Stat"); got != 2 {
		t.Fatalf("Stat calls=%d want 2 across mutation generations", got)
	}
}

func TestCancelingFlightLeaderDoesNotCancelOtherWaiter(t *testing.T) {
	remote := &blockingStatRemote{fakeRemote: &fakeRemote{memories: map[string]client.Memory{
		"a.md": {MemoryID: "mem_a", Path: "a.md", LatestVersionID: "fmver_1"},
	}}}
	filesystem := NewFileSystem(remote, Options{})
	started, finish := remote.pauseNextStats(1)
	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	leaderDone := make(chan syscall.Errno, 1)
	go func() {
		_, errno := filesystem.Lookup(leaderCtx, "a.md")
		leaderDone <- errno
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		close(finish)
		t.Fatal("leader Stat did not start")
	}
	followerDone := make(chan syscall.Errno, 1)
	go func() {
		_, errno := filesystem.Stat(context.Background(), "a.md")
		followerDone <- errno
	}()
	// Let the follower attach to the existing flight before canceling the
	// caller that happened to create it.
	time.Sleep(20 * time.Millisecond)
	cancelLeader()
	select {
	case errno := <-leaderDone:
		if errno == 0 {
			close(finish)
			<-followerDone
			t.Fatal("canceled leader returned success")
		}
	case <-time.After(200 * time.Millisecond):
		close(finish)
		<-leaderDone
		<-followerDone
		t.Fatal("canceled leader did not return promptly")
	}
	select {
	case errno := <-followerDone:
		close(finish)
		t.Fatalf("follower completed before shared Stat was released: errno=%v", errno)
	case <-time.After(50 * time.Millisecond):
	}
	close(finish)
	select {
	case errno := <-followerDone:
		if errno != 0 {
			t.Fatalf("follower errno=%v want success", errno)
		}
	case <-time.After(time.Second):
		t.Fatal("follower did not receive the shared result")
	}
	if got := remote.callCount("Stat"); got != 1 {
		t.Fatalf("Stat calls=%d want 1", got)
	}
}

func TestAllCanceledPointStatWaitersCleanUpFlight(t *testing.T) {
	remote := &blockingStatRemote{fakeRemote: &fakeRemote{memories: map[string]client.Memory{
		"a.md": {MemoryID: "mem_a", Path: "a.md", LatestVersionID: "fmver_1"},
	}}}
	filesystem := NewFileSystem(remote, Options{})
	started, finish := remote.pauseNextStats(1)
	contexts := make([]context.Context, 2)
	cancels := make([]context.CancelFunc, 2)
	for index := range contexts {
		contexts[index], cancels[index] = context.WithCancel(context.Background())
	}
	done := make(chan syscall.Errno, 2)
	go func() {
		_, errno := filesystem.Lookup(contexts[0], "a.md")
		done <- errno
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		close(finish)
		t.Fatal("shared Stat did not start")
	}
	go func() {
		_, errno := filesystem.Stat(contexts[1], "a.md")
		done <- errno
	}()
	time.Sleep(20 * time.Millisecond)
	cancels[0]()
	cancels[1]()
	for range 2 {
		select {
		case errno := <-done:
			if errno == 0 {
				close(finish)
				t.Fatal("canceled waiter returned success")
			}
		case <-time.After(200 * time.Millisecond):
			close(finish)
			t.Fatal("canceled waiter did not return promptly")
		}
	}
	filesystem.statMu.Lock()
	inFlight := len(filesystem.statFlights)
	filesystem.statMu.Unlock()
	if inFlight != 0 {
		close(finish)
		t.Fatalf("in-flight entries after all waiters canceled=%d want 0", inFlight)
	}
	// The canceled flight must not poison the key. Its pause slot is consumed,
	// so a fresh caller can issue a second Remote.Stat and succeed immediately.
	if _, errno := filesystem.Lookup(context.Background(), "a.md"); errno != 0 {
		close(finish)
		t.Fatalf("fresh Lookup after all cancellations errno=%v", errno)
	}
	close(finish)
	if got := remote.callCount("Stat"); got != 2 {
		t.Fatalf("Stat calls=%d want 2 (one canceled shared request, one fresh)", got)
	}
}

func TestPointStatWaiterCannotPromoteOldFlightPastNewerOpen(t *testing.T) {
	now := time.Date(2026, time.August, 22, 13, 0, 0, 0, time.UTC)
	v1 := client.Memory{MemoryID: "mem_a", Path: "a.md", Content: "one", ContentSHA256: "sha-v1", LatestVersionID: "fmver_1"}
	v2 := client.Memory{MemoryID: "mem_a", Path: "a.md", Content: "two", ContentSHA256: "sha-v2", LatestVersionID: "fmver_2"}
	remote := &blockingStatRemote{fakeRemote: &fakeRemote{memories: map[string]client.Memory{"a.md": v1}}}
	filesystem := NewFileSystem(remote, Options{MetadataTTL: time.Second, Now: func() time.Time { return now }})
	started, finish := remote.pauseNextStats(1)
	type statResult struct {
		node  Node
		errno syscall.Errno
	}
	leaderDone := make(chan statResult, 1)
	go func() {
		node, errno := filesystem.Stat(context.Background(), "a.md")
		leaderDone <- statResult{node: node, errno: errno}
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		close(finish)
		t.Fatal("old Stat flight did not start")
	}
	remote.mu.Lock()
	remote.memories["a.md"] = v2
	remote.mu.Unlock()
	if _, errno := filesystem.Open(context.Background(), "a.md"); errno != 0 {
		close(finish)
		t.Fatalf("newer Open errno=%v", errno)
	}
	now = now.Add(2 * time.Second)
	waiterDone := make(chan statResult, 1)
	go func() {
		node, errno := filesystem.Stat(context.Background(), "a.md")
		waiterDone <- statResult{node: node, errno: errno}
	}()
	deadline := time.Now().Add(time.Second)
	for {
		filesystem.statMu.Lock()
		waiters := 0
		for _, flight := range filesystem.statFlights {
			waiters = flight.waiters
		}
		filesystem.statMu.Unlock()
		if waiters == 2 {
			break
		}
		if time.Now().After(deadline) {
			close(finish)
			<-leaderDone
			<-waiterDone
			t.Fatalf("late waiter did not join old flight: waiters=%d", waiters)
		}
		time.Sleep(time.Millisecond)
	}
	close(finish)
	for _, result := range []statResult{<-leaderDone, <-waiterDone} {
		if result.errno != 0 || result.node.LatestVersionID != "fmver_1" {
			t.Fatalf("shared old-flight result node=%+v errno=%v", result.node, result.errno)
		}
	}
	if node, exists := filesystem.Index().Lookup("a.md"); !exists || node.LatestVersionID != "fmver_2" || node.SHA256 != "sha-v2" {
		t.Fatalf("late waiter promoted old flight over newer Open: node=%+v exists=%v", node, exists)
	}
	if got := remote.callCount("Stat"); got != 1 {
		t.Fatalf("Stat calls=%d want 1", got)
	}
}

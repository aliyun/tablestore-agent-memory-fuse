package fs

import (
	"container/list"
	"context"
	"errors"
	"net/http"
	"path"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"agent-memory-fuse/internal/client"
)

type Remote interface {
	List(ctx context.Context, pathPrefix, nextToken string, limit int) (client.ListResult, error)
	Stat(ctx context.Context, path string) (client.Memory, error)
	Read(ctx context.Context, path string) (client.Memory, error)
	Create(ctx context.Context, path string, content []byte) (client.Memory, error)
	Update(ctx context.Context, itemID string, content []byte, expectedVersionID string) (client.Memory, error)
	Rename(ctx context.Context, itemID, toPath string, overwrite bool, expectedVersionID string) (client.Memory, error)
	Delete(ctx context.Context, itemID, expectedVersionID string) error
}

const (
	// MaxFiles 是 Statfs 向内核宣告的常规文件数上限。
	MaxFiles = 2000
	// LocalFileLimitSlack 允许本地 pending create 暂时超过常规上限；最终仍以
	// 服务端 QUOTA_EXCEEDED 为准。
	LocalFileLimitSlack = 100
	MaxLocalFiles       = MaxFiles + LocalFileLimitSlack
)

const maxNegativeEntries = 4096

const (
	defaultRefreshRecoveryTimeout = 30 * time.Second
	defaultRefreshRecoveryBackoff = 100 * time.Millisecond
)

var errStaleCreatedHandle = errors.New("created handle no longer owns path")

type Options struct {
	ReadOnly               bool
	MetadataTTL            time.Duration
	NegativeTTL            time.Duration
	DirectoryTTL           time.Duration
	MutationTimeout        time.Duration
	RefreshRecoveryTimeout time.Duration
	RefreshRecoveryBackoff time.Duration
	Now                    func() time.Time
	MutationErrorReporter  func(MutationErrorEvent)
}

type pointObservation struct {
	ticket uint64
	exists bool
	memory client.Memory
}

type statFlight struct {
	done      chan struct{}
	cancel    context.CancelFunc
	waiters   int
	completed bool
	ticket    uint64
	memory    client.Memory
	err       error
}

type statFlightKey struct {
	path       string
	generation uint64
}

type mutationSuccessBucket struct {
	itemIDs []string
	paths   []string
}

type pendingCreateOwner struct {
	handle         *Handle
	creationTicket uint64
}

type FileSystem struct {
	remote Remote
	index  *PathIndex
	opts   Options
	// serverReadOnly 是最近一次成功发布的服务端只读握手标志。
	serverReadOnly atomic.Bool

	// refreshMu 只串行化远端 List 快照；stateMu 保护本地发布状态与
	// mutationGen。远端 Read 和不同路径的写操作不经过 refreshMu。
	refreshMu                sync.Mutex
	stateMu                  sync.Mutex
	mutationGen              uint64
	nextMutationTicket       uint64
	completedMutationThrough uint64
	completedMutationTickets map[uint64]struct{}
	publishedTicket          map[string]uint64
	publishedPathTicket      map[string]uint64
	successKeysByTicket      map[uint64]mutationSuccessBucket
	lastRefresh              time.Time
	localDirs                map[string]struct{}
	pendingCreated           map[string]pendingCreateOwner
	positiveExpiry           map[string]time.Time
	negativeExpiry           map[string]time.Time
	negativeOrder            *list.List
	negativeItems            map[string]*list.Element
	pathLocks                *pathLocks
	statMu                   sync.Mutex
	statFlights              map[statFlightKey]*statFlight
	lifecycleCtx             context.Context
	lifecycleCancel          context.CancelFunc
	closeOnce                sync.Once

	nextObservationTicket uint64
	lastSnapshotTicket    uint64
	observationEpoch      uint64
	pointObservations     map[string]pointObservation

	// backgroundRefreshScheduled 由 stateMu 保护。调度钩子默认为异步
	// goroutine，测试可替换成无延迟队列或同步执行器。
	backgroundRefreshScheduled bool
	scheduleBackgroundRefresh  func(func())
	// afterRefreshRecoveryUnlock 仅供同包确定性测试观察 worker 退出交接；
	// 生产构造保持 nil，且回调始终在 refreshMu/stateMu 之外运行。
	afterRefreshRecoveryUnlock func()
}

func NewFileSystem(remote Remote, opts Options) *FileSystem {
	if opts.MetadataTTL <= 0 {
		opts.MetadataTTL = 5 * time.Second
	}
	if opts.NegativeTTL <= 0 {
		opts.NegativeTTL = time.Second
	}
	if opts.DirectoryTTL <= 0 {
		opts.DirectoryTTL = 10 * time.Second
	}
	if opts.MutationTimeout <= 0 {
		opts.MutationTimeout = time.Minute
	}
	if opts.RefreshRecoveryTimeout <= 0 {
		opts.RefreshRecoveryTimeout = defaultRefreshRecoveryTimeout
	}
	if opts.RefreshRecoveryBackoff <= 0 {
		opts.RefreshRecoveryBackoff = defaultRefreshRecoveryBackoff
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	lifecycleCtx, lifecycleCancel := context.WithCancel(context.Background())
	filesystem := &FileSystem{
		remote:                   remote,
		index:                    NewPathIndex(),
		opts:                     opts,
		localDirs:                map[string]struct{}{},
		pendingCreated:           map[string]pendingCreateOwner{},
		completedMutationTickets: map[uint64]struct{}{},
		publishedTicket:          map[string]uint64{},
		publishedPathTicket:      map[string]uint64{},
		successKeysByTicket:      map[uint64]mutationSuccessBucket{},
		positiveExpiry:           map[string]time.Time{},
		negativeExpiry:           map[string]time.Time{},
		negativeOrder:            list.New(),
		negativeItems:            map[string]*list.Element{},
		pathLocks:                newPathLocks(),
		statFlights:              map[statFlightKey]*statFlight{},
		pointObservations:        make(map[string]pointObservation),
		lifecycleCtx:             lifecycleCtx,
		lifecycleCancel:          lifecycleCancel,
	}
	filesystem.scheduleBackgroundRefresh = func(refresh func()) { go refresh() }
	return filesystem
}

// Close stops lifecycle-bound background work. It is safe to call more than
// once and does not wait for a Remote implementation that ignores cancellation.
func (f *FileSystem) Close() {
	f.closeOnce.Do(func() {
		f.lifecycleCancel()
		f.stateMu.Lock()
		f.backgroundRefreshScheduled = false
		f.stateMu.Unlock()
	})
}

func (f *FileSystem) lockRefresh(ctx context.Context) error {
	if f.refreshMu.TryLock() {
		return nil
	}
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if f.refreshMu.TryLock() {
				return nil
			}
		}
	}
}

func (f *FileSystem) mutationContext(parent context.Context) (context.Context, context.CancelFunc, error) {
	if err := parent.Err(); err != nil {
		return nil, nil, err
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), f.opts.MutationTimeout)
	return ctx, cancel, nil
}

func (f *FileSystem) Refresh(ctx context.Context) error {
	if err := f.lockRefresh(ctx); err != nil {
		return err
	}
	published, err := f.refreshOnce(ctx)
	f.refreshMu.Unlock()
	if err == nil && !published {
		f.scheduleRefreshRecovery()
	}
	return err
}

// refreshOnce performs one complete paginated snapshot attempt. The caller
// holds refreshMu, but no state lock is held while waiting on List.
func (f *FileSystem) refreshOnce(ctx context.Context) (bool, error) {
	f.stateMu.Lock()
	f.purgeExpiredNegativesLocked(f.opts.Now())
	generation := f.mutationGen
	observationEpoch := f.observationEpoch
	ticket := f.nextObservationTicketLocked()
	f.stateMu.Unlock()

	var all []client.Memory
	serverReadOnly := false
	next := ""
	for {
		page, err := f.remote.List(ctx, "", next, 100)
		if err != nil {
			return false, err
		}
		all = append(all, page.Memories...)
		// 各分页取或：任一分页带 readOnly=true 即整库只读。
		serverReadOnly = serverReadOnly || page.ReadOnly
		if page.NextToken == "" {
			break
		}
		next = page.NextToken
	}

	f.stateMu.Lock()
	defer f.stateMu.Unlock()
	now := f.opts.Now()
	f.purgeExpiredNegativesLocked(now)
	if f.mutationGen != generation || f.observationEpoch != observationEpoch {
		return false, nil
	}
	// readOnly、索引、本地目录与时间戳只在 generation/observation epoch
	// 校验成功后作为同一次发布更新；raced snapshot 不得留下任何部分状态。
	f.serverReadOnly.Store(serverReadOnly)
	f.index.Rebuild(all)
	positiveExpiry := make(map[string]time.Time, len(all))
	for _, memory := range all {
		positiveExpiry[canonicalPathKey(memory.Path)] = now.Add(f.opts.MetadataTTL)
	}
	for value, observation := range f.pointObservations {
		if observation.ticket > ticket {
			if observation.exists {
				f.index.Upsert(observation.memory)
				positiveExpiry[value] = now.Add(f.opts.MetadataTTL)
				f.removeNegativeLocked(value, false)
			} else {
				f.index.Remove(value)
				delete(positiveExpiry, value)
			}
		} else {
			delete(f.pointObservations, value)
		}
	}
	for value := range f.pendingCreated {
		if _, exists := f.index.Lookup(value); !exists {
			f.index.Upsert(client.Memory{Path: value})
		}
		delete(positiveExpiry, value)
		f.removeNegativeLocked(value, false)
	}
	for dir := range f.localDirs {
		f.index.AddDir(dir)
		delete(positiveExpiry, dir)
		f.removeNegativeLocked(dir, false)
	}
	for value := range f.negativeExpiry {
		if _, exists := f.index.Lookup(value); exists {
			f.removeNegativeLocked(value, false)
		}
	}
	f.positiveExpiry = positiveExpiry
	f.lastSnapshotTicket = ticket
	f.lastRefresh = now
	return true, nil
}

func (f *FileSystem) EnsureFresh(ctx context.Context) error {
	reporter := f.opts.MutationErrorReporter
	if reporter == nil {
		if err := f.lockRefresh(ctx); err != nil {
			return err
		}
		f.stateMu.Lock()
		stale := f.lastRefresh.IsZero() || f.opts.Now().Sub(f.lastRefresh) >= f.opts.DirectoryTTL
		f.stateMu.Unlock()
		if !stale {
			f.refreshMu.Unlock()
			return nil
		}
		published, err := f.refreshOnce(ctx)
		f.refreshMu.Unlock()
		if err == nil && !published {
			f.scheduleRefreshRecovery()
		}
		return err
	}
	return f.ensureFreshWithMutationErrorReporter(ctx, reporter)
}

func (f *FileSystem) ensureFreshWithMutationErrorReporter(ctx context.Context, reporter func(MutationErrorEvent)) error {
	if err := f.lockRefresh(ctx); err != nil {
		return err
	}
	f.stateMu.Lock()
	stale := f.lastRefresh.IsZero() || f.opts.Now().Sub(f.lastRefresh) >= f.opts.DirectoryTTL
	f.stateMu.Unlock()
	if !stale {
		f.refreshMu.Unlock()
		return nil
	}
	published, err := f.refreshOnce(ctx)
	f.refreshMu.Unlock()
	if err == nil && !published {
		f.scheduleRefreshRecovery()
	}
	if err != nil {
		safelyReportRemoteError(reporter, "list", "", err, errnoFor(err))
	}
	return err
}

func (f *FileSystem) scheduleRefreshRecovery() {
	f.stateMu.Lock()
	if f.lifecycleCtx.Err() != nil {
		f.stateMu.Unlock()
		return
	}
	if f.backgroundRefreshScheduled {
		f.stateMu.Unlock()
		return
	}
	f.backgroundRefreshScheduled = true
	schedule := f.scheduleBackgroundRefresh
	f.stateMu.Unlock()

	// Synchronous test schedulers are supported: neither stateMu nor
	// refreshMu is held when the callback starts.
	schedule(f.runRefreshRecovery)
}

func (f *FileSystem) runRefreshRecovery() {
	for attempt := range 2 {
		if attempt > 0 {
			timer := time.NewTimer(f.opts.RefreshRecoveryBackoff)
			select {
			case <-timer.C:
			case <-f.lifecycleCtx.Done():
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				f.stateMu.Lock()
				f.backgroundRefreshScheduled = false
				f.stateMu.Unlock()
				return
			}
		}
		attemptCtx, cancel := context.WithTimeout(f.lifecycleCtx, f.opts.RefreshRecoveryTimeout)
		lockErr := f.lockRefresh(attemptCtx)
		var published bool
		var err error
		if lockErr == nil {
			published, err = f.refreshOnce(attemptCtx)
		} else {
			err = lockErr
		}
		exiting := err == nil && published || attempt == 1 || f.lifecycleCtx.Err() != nil
		if exiting {
			// Clear the worker reservation before releasing refreshMu. A queued
			// foreground refresh can only finish after the flag is available for
			// its own raced publication to schedule a successor.
			f.stateMu.Lock()
			f.backgroundRefreshScheduled = false
			f.stateMu.Unlock()
		}
		if lockErr == nil {
			f.refreshMu.Unlock()
		}
		cancel()
		if exiting {
			if f.afterRefreshRecoveryUnlock != nil {
				f.afterRefreshRecoveryUnlock()
			}
			return
		}
	}
}

func (f *FileSystem) Index() *PathIndex { return f.index }

// readOnly 返回生效只读态：CLI --read-only 是地板（服务端 false 不能降级），
// 服务端握手标志按最近一次 Refresh 的各分页取或。
func (f *FileSystem) readOnly() bool { return f.opts.ReadOnly || f.serverReadOnly.Load() }

func (f *FileSystem) ReadOnly() bool { return f.readOnly() }

func (f *FileSystem) Lookup(ctx context.Context, value string) (Node, syscall.Errno) {
	return f.pointStat(ctx, value)
}

func (f *FileSystem) Stat(ctx context.Context, value string) (Node, syscall.Errno) {
	return f.pointStat(ctx, value)
}

func (f *FileSystem) pointStat(ctx context.Context, value string) (Node, syscall.Errno) {
	value = canonicalPathKey(value)
	f.stateMu.Lock()
	now := f.opts.Now()
	f.purgeExpiredNegativesLocked(now)
	node, exists := f.index.Lookup(value)
	_, pending := f.pendingCreated[value]
	if exists && (node.IsDir || pending) {
		f.stateMu.Unlock()
		return node, 0
	}
	if expiry, cached := f.positiveExpiry[value]; cached && now.Before(expiry) && exists {
		f.stateMu.Unlock()
		return node, 0
	}
	delete(f.positiveExpiry, value)
	if expiry, cached := f.negativeExpiry[value]; cached && now.Before(expiry) {
		f.stateMu.Unlock()
		return Node{}, syscall.ENOENT
	}
	f.removeNegativeLocked(value, true)
	generation := f.mutationGen
	ticket := f.nextObservationTicketLocked()
	f.stateMu.Unlock()

	memory, err, observationTicket := f.coalescedRemoteStat(ctx, value, generation, ticket)
	ticket = observationTicket
	if err != nil {
		errno := errnoFor(err)
		if errno == syscall.ENOENT {
			f.stateMu.Lock()
			if f.canPublishPointLocked(value, generation, ticket) {
				if stale, present := f.index.Lookup(value); present && !stale.IsDir {
					if _, pending := f.pendingCreated[value]; !pending {
						f.index.Remove(value)
						f.restoreLocalDirsLocked()
					}
				}
				f.pointObservations[value] = pointObservation{ticket: ticket}
				delete(f.positiveExpiry, value)
				f.setNegativeLocked(value, f.opts.Now().Add(f.opts.NegativeTTL))
			}
			f.stateMu.Unlock()
		}
		return Node{}, errno
	}

	metadata := memory
	metadata.Content = ""
	observed := nodeFromMemory(metadata)
	f.stateMu.Lock()
	if f.canPublishPointLocked(value, generation, ticket) {
		metadata.Path = value
		f.index.Upsert(metadata)
		f.pointObservations[value] = pointObservation{ticket: ticket, exists: true, memory: metadata}
		f.positiveExpiry[value] = f.opts.Now().Add(f.opts.MetadataTTL)
		f.removeNegativeLocked(value, false)
	}
	f.stateMu.Unlock()
	return observed, 0
}

func (f *FileSystem) coalescedRemoteStat(ctx context.Context, value string, generation, ticket uint64) (client.Memory, error, uint64) {
	if err := ctx.Err(); err != nil {
		return client.Memory{}, err, ticket
	}
	key := statFlightKey{path: value, generation: generation}
	f.statMu.Lock()
	flight := f.statFlights[key]
	if flight == nil {
		sharedCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
		flight = &statFlight{done: make(chan struct{}), cancel: cancel, ticket: ticket}
		f.statFlights[key] = flight
		go f.runStatFlight(sharedCtx, key, flight)
	}
	flight.waiters++
	f.statMu.Unlock()

	select {
	case <-flight.done:
		return flight.memory, flight.err, flight.ticket
	case <-ctx.Done():
		f.statMu.Lock()
		if !flight.completed {
			flight.waiters--
			if flight.waiters == 0 {
				if f.statFlights[key] == flight {
					delete(f.statFlights, key)
				}
				flight.cancel()
			}
		}
		f.statMu.Unlock()
		return client.Memory{}, ctx.Err(), flight.ticket
	}
}

func (f *FileSystem) runStatFlight(ctx context.Context, key statFlightKey, flight *statFlight) {
	memory, err := f.remote.Stat(ctx, key.path)
	reporter := f.opts.MutationErrorReporter
	if reporter == nil || err == nil {
		f.statMu.Lock()
		flight.memory = memory
		flight.err = err
		flight.completed = true
		if f.statFlights[key] == flight {
			delete(f.statFlights, key)
		}
		close(flight.done)
		flight.cancel()
		f.statMu.Unlock()
		return
	}

	f.statMu.Lock()
	flight.memory = memory
	flight.err = err
	flight.completed = true
	if f.statFlights[key] == flight {
		delete(f.statFlights, key)
	}
	close(flight.done)
	flight.cancel()
	f.statMu.Unlock()

	safelyReportRemoteError(reporter, "stat", key.path, err, errnoFor(err))
}

func (f *FileSystem) canPublishPointLocked(value string, generation, ticket uint64) bool {
	latest, hasLatest := f.pointObservations[value]
	return f.mutationGen == generation && ticket >= f.lastSnapshotTicket && (!hasLatest || ticket >= latest.ticket)
}

func nodeFromMemory(memory client.Memory) Node {
	value := canonicalPathKey(memory.Path)
	return Node{
		Name:            path.Base(value),
		Path:            value,
		MemoryID:        memory.MemoryID,
		LatestVersionID: memory.LatestVersionID,
		Size:            memory.ContentSizeBytes,
		SHA256:          memory.ContentSHA256,
		UpdatedAt:       memory.UpdatedAt,
	}
}

func (f *FileSystem) Open(ctx context.Context, value string) (*Handle, syscall.Errno) {
	value = canonicalPathKey(value)
	f.stateMu.Lock()
	generation := f.mutationGen
	ticket := f.nextObservationTicketLocked()
	f.stateMu.Unlock()

	memory, err := f.remote.Read(ctx, value)
	if err != nil {
		result := errnoFor(err)
		if reporter := f.opts.MutationErrorReporter; reporter != nil {
			safelyReportRemoteError(reporter, "read", value, err, result)
		}
		return nil, result
	}
	// The handle always returns the snapshot it read. Publishing its metadata
	// is conditional so a late read cannot roll back a completed mutation.
	f.stateMu.Lock()
	observationPath := canonicalPathKey(memory.Path)
	memory.Path = normalizePathNFC(memory.Path)
	if f.canPublishPointLocked(observationPath, generation, ticket) {
		metadata := memory
		metadata.Content = ""
		f.index.Upsert(metadata)
		f.pointObservations[observationPath] = pointObservation{ticket: ticket, exists: true, memory: metadata}
		f.positiveExpiry[observationPath] = f.opts.Now().Add(f.opts.MetadataTTL)
		f.removeNegativeLocked(observationPath, false)
	}
	f.stateMu.Unlock()
	return NewHandle(memory), 0
}

func (f *FileSystem) Create(value string) (*Handle, syscall.Errno) {
	value = canonicalPathKey(value)
	unlock := f.pathLocks.Lock(value)
	defer unlock()
	if f.readOnly() {
		return nil, syscall.EROFS
	}
	handle := NewCreatedHandle(value)

	f.stateMu.Lock()
	defer f.stateMu.Unlock()
	if _, exists := f.index.Lookup(value); exists {
		return nil, syscall.EEXIST
	}
	if f.index.Count() >= MaxLocalFiles {
		return nil, syscall.ENOSPC
	}
	f.index.Upsert(client.Memory{Path: value})
	f.invalidateMetadataLocked(value)
	creationTicket := f.recordLocalPathSuccessLocked(value)
	f.pendingCreated[value] = pendingCreateOwner{handle: handle, creationTicket: creationTicket}
	f.advanceMutationLocked()
	return handle, 0
}

func (f *FileSystem) Commit(ctx context.Context, handle *Handle) syscall.Errno {
	// 纯读 handle 也会经 close 的 Flush 或显式 Fsync 走到这里：
	// 无脏数据必须先短路成功，否则只读挂载上任何读后关闭都误报 EROFS。
	if !handle.Dirty() {
		return 0
	}
	reporter := f.opts.MutationErrorReporter
	if reporter == nil {
		// Keep the default path identical to the parent mutation path: no
		// diagnostic metadata capture, and the keyed lock remains deferred
		// through errno mapping.
		handlePath := handle.Memory().Path
		unlock := f.pathLocks.Lock(handlePath)
		defer unlock()
		err := handle.Commit(ctx, func(ctx context.Context, current client.Memory, content []byte, isNew bool) (client.Memory, error) {
			if f.readOnly() {
				return client.Memory{}, syscall.EROFS
			}
			currentPath := canonicalPathKey(current.Path)
			f.stateMu.Lock()
			owner, ownsPath := f.pendingCreated[currentPath]
			if isNew && (!ownsPath || owner.handle != handle) {
				f.stateMu.Unlock()
				return client.Memory{}, errStaleCreatedHandle
			}
			mutationTicket := f.nextMutationPublishTicketLocked()
			f.stateMu.Unlock()
			defer f.completeMutationTicket(mutationTicket)
			mutationCtx, cancel, err := f.mutationContext(ctx)
			if err != nil {
				return client.Memory{}, err
			}
			defer cancel()
			var memory client.Memory
			if isNew {
				memory, err = f.remote.Create(mutationCtx, current.Path, content)
			} else {
				memory, err = f.remote.Update(mutationCtx, current.MemoryID, content, current.LatestVersionID)
			}
			if err == nil {
				memory.Path = normalizePathNFC(memory.Path)
				f.stateMu.Lock()
				f.publishCommitSuccessLocked(handle, currentPath, memory, isNew, mutationTicket)
				f.stateMu.Unlock()
			}
			return memory, err
		})
		if errors.Is(err, syscall.EROFS) {
			return syscall.EROFS
		}
		if errors.Is(err, errStaleCreatedHandle) {
			return syscall.ESTALE
		}
		if apiErr := new(client.APIError); errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusNotFound {
			// item 已被删除：旧 handle 的提交对象消失，不得静默重建文件。
			return syscall.ESTALE
		}
		return errnoFor(err)
	}
	return f.commitWithMutationErrorReporter(ctx, handle, reporter)
}

func (f *FileSystem) commitWithMutationErrorReporter(ctx context.Context, handle *Handle, reporter func(MutationErrorEvent)) syscall.Errno {
	// Memory() only holds Handle.mu long enough to copy the current identity;
	// it is released before waiting for the keyed path lock. Handle.Commit
	// re-checks dirty/authoritative state under Handle.mu after the wait.
	handlePath := handle.Memory().Path
	unlockPath := f.pathLocks.Lock(handlePath)
	defer func() {
		if unlockPath != nil {
			unlockPath()
		}
	}()
	var failedOperation, failedPath string
	err := handle.Commit(ctx, func(ctx context.Context, current client.Memory, content []byte, isNew bool) (client.Memory, error) {
		if f.readOnly() {
			return client.Memory{}, syscall.EROFS
		}
		currentPath := canonicalPathKey(current.Path)
		f.stateMu.Lock()
		owner, ownsPath := f.pendingCreated[currentPath]
		if isNew && (!ownsPath || owner.handle != handle) {
			f.stateMu.Unlock()
			return client.Memory{}, errStaleCreatedHandle
		}
		mutationTicket := f.nextMutationPublishTicketLocked()
		f.stateMu.Unlock()
		defer f.completeMutationTicket(mutationTicket)
		mutationCtx, cancel, err := f.mutationContext(ctx)
		if err != nil {
			return client.Memory{}, err
		}
		defer cancel()
		var memory client.Memory
		if isNew {
			memory, err = f.remote.Create(mutationCtx, current.Path, content)
		} else {
			memory, err = f.remote.Update(mutationCtx, current.MemoryID, content, current.LatestVersionID)
		}
		if err == nil {
			memory.Path = normalizePathNFC(memory.Path)
			f.stateMu.Lock()
			f.publishCommitSuccessLocked(handle, currentPath, memory, isNew, mutationTicket)
			f.stateMu.Unlock()
		} else if isNew {
			failedOperation, failedPath = "create", current.Path
		} else {
			failedOperation, failedPath = "update", current.Path
		}
		return memory, err
	})
	// Handle.Commit has released Handle.mu, and the keyed mutation lock must be
	// released before invoking a synchronous reporter that may inspect this path.
	unlockPath()
	unlockPath = nil

	var result syscall.Errno
	if errors.Is(err, syscall.EROFS) {
		result = syscall.EROFS
	} else if errors.Is(err, errStaleCreatedHandle) {
		result = syscall.ESTALE
	} else if apiErr := new(client.APIError); errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusNotFound {
		// item 已被删除：旧 handle 的提交对象消失，不得静默重建文件。
		result = syscall.ESTALE
	} else {
		result = errnoFor(err)
	}
	if failedOperation != "" {
		safelyReportMutationError(reporter, newMutationErrorEvent(failedOperation, failedPath, err, result))
	}
	return result
}

func (f *FileSystem) publishCommitSuccessLocked(handle *Handle, currentPath string, memory client.Memory, isNew bool, mutationTicket uint64) {
	memoryPath := canonicalPathKey(memory.Path)
	memory.Path = normalizePathNFC(memory.Path)
	affectedPaths := []string{currentPath, memoryPath}
	canPublish := f.canPublishMutationLocked(memory.MemoryID, mutationTicket, affectedPaths...)
	f.recordMutationSuccessLocked(memory.MemoryID, mutationTicket, affectedPaths...)

	if !isNew {
		f.clearPendingCreatedThroughLocked(mutationTicket, affectedPaths...)
		if !canPublish {
			return
		}
		f.index.Replace(currentPath, memory, f.localDirPathsLocked())
		f.invalidateMetadataLocked(currentPath, memoryPath)
		f.advanceMutationLocked()
		return
	}

	removedPlaceholder := false
	seenPaths := make(map[string]struct{}, 2)
	for _, value := range affectedPaths {
		if value == "" {
			continue
		}
		if _, seen := seenPaths[value]; seen {
			continue
		}
		seenPaths[value] = struct{}{}
		owner, exists := f.pendingCreated[value]
		if !exists || owner.handle != handle {
			continue
		}
		delete(f.pendingCreated, value)
		if node, exists := f.index.Lookup(value); exists && !node.IsDir && node.MemoryID == "" {
			f.index.Remove(value)
			removedPlaceholder = true
		}
	}
	if removedPlaceholder {
		f.restoreLocalDirsLocked()
	}
	if !canPublish {
		return
	}
	f.index.Upsert(memory)
	f.invalidateMetadataLocked(currentPath, memoryPath)
	f.advanceMutationLocked()
}

func (f *FileSystem) Rename(ctx context.Context, from, to string, overwrite bool) syscall.Errno {
	from, to = canonicalPathKey(from), canonicalPathKey(to)
	reporter := f.opts.MutationErrorReporter
	if reporter != nil {
		return f.renameWithMutationErrorReporter(ctx, from, to, overwrite, reporter)
	}
	unlock := f.pathLocks.Lock(from, to)
	defer unlock()
	if f.readOnly() {
		return syscall.EROFS
	}
	f.stateMu.Lock()
	node, ok := f.index.Lookup(from)
	if !ok {
		f.stateMu.Unlock()
		return syscall.ENOENT
	}
	if node.IsDir {
		f.stateMu.Unlock()
		return syscall.ENOTSUP
	}
	if target, exists := f.index.Lookup(to); exists && target.IsDir {
		f.stateMu.Unlock()
		return syscall.EISDIR
	}
	if handled, result := f.renamePendingCreatedLocked(from, to, node); handled {
		f.stateMu.Unlock()
		return result
	}
	mutationTicket := f.nextMutationPublishTicketLocked()
	f.stateMu.Unlock()
	defer f.completeMutationTicket(mutationTicket)
	// 目标占用（PATH_EXISTS）→ EEXIST、资源版本争用（PRECONDITION_FAILED）→ ESTALE 均由
	// errnoFor 按服务端 409 细分 code 区分，无需再猜裸 409。
	mutationCtx, cancel, err := f.mutationContext(ctx)
	if err != nil {
		return errnoFor(err)
	}
	defer cancel()
	memory, err := f.remote.Rename(mutationCtx, node.MemoryID, to, overwrite, node.LatestVersionID)
	if err != nil {
		return errnoFor(err)
	}
	memory.Path = normalizePathNFC(memory.Path)
	f.stateMu.Lock()
	affectedPaths := []string{from, to, memory.Path}
	canPublish := f.canPublishMutationLocked(node.MemoryID, mutationTicket, affectedPaths...)
	f.recordMutationSuccessLocked(node.MemoryID, mutationTicket, affectedPaths...)
	f.clearPendingCreatedThroughLocked(mutationTicket, affectedPaths...)
	if canPublish {
		f.index.Replace(from, memory, f.localDirPathsLocked())
		f.invalidateMetadataLocked(affectedPaths...)
		f.advanceMutationLocked()
	}
	f.stateMu.Unlock()
	return 0
}

func (f *FileSystem) renameWithMutationErrorReporter(ctx context.Context, from, to string, overwrite bool, reporter func(MutationErrorEvent)) syscall.Errno {
	unlock := f.pathLocks.Lock(from, to)
	defer func() {
		if unlock != nil {
			unlock()
		}
	}()
	if f.readOnly() {
		return syscall.EROFS
	}
	f.stateMu.Lock()
	node, ok := f.index.Lookup(from)
	if !ok {
		f.stateMu.Unlock()
		return syscall.ENOENT
	}
	if node.IsDir {
		f.stateMu.Unlock()
		return syscall.ENOTSUP
	}
	if target, exists := f.index.Lookup(to); exists && target.IsDir {
		f.stateMu.Unlock()
		return syscall.EISDIR
	}
	if handled, result := f.renamePendingCreatedLocked(from, to, node); handled {
		f.stateMu.Unlock()
		return result
	}
	mutationTicket := f.nextMutationPublishTicketLocked()
	f.stateMu.Unlock()
	defer f.completeMutationTicket(mutationTicket)
	mutationCtx, cancel, err := f.mutationContext(ctx)
	if err != nil {
		return errnoFor(err)
	}
	defer cancel()
	memory, err := f.remote.Rename(mutationCtx, node.MemoryID, to, overwrite, node.LatestVersionID)
	if err != nil {
		result := errnoFor(err)
		unlock()
		unlock = nil
		safelyReportRemoteError(reporter, "rename", from, err, result)
		return result
	}
	memory.Path = normalizePathNFC(memory.Path)
	f.stateMu.Lock()
	affectedPaths := []string{from, to, memory.Path}
	canPublish := f.canPublishMutationLocked(node.MemoryID, mutationTicket, affectedPaths...)
	f.recordMutationSuccessLocked(node.MemoryID, mutationTicket, affectedPaths...)
	f.clearPendingCreatedThroughLocked(mutationTicket, affectedPaths...)
	if canPublish {
		f.index.Replace(from, memory, f.localDirPathsLocked())
		f.invalidateMetadataLocked(affectedPaths...)
		f.advanceMutationLocked()
	}
	f.stateMu.Unlock()
	return 0
}

func (f *FileSystem) Delete(ctx context.Context, value string) syscall.Errno {
	value = canonicalPathKey(value)
	reporter := f.opts.MutationErrorReporter
	if reporter != nil {
		return f.deleteWithMutationErrorReporter(ctx, value, reporter)
	}
	unlock := f.pathLocks.Lock(value)
	defer unlock()
	if f.readOnly() {
		return syscall.EROFS
	}
	f.stateMu.Lock()
	node, ok := f.index.Lookup(value)
	if !ok {
		f.stateMu.Unlock()
		return syscall.ENOENT
	}
	if node.IsDir {
		f.stateMu.Unlock()
		return syscall.EISDIR
	}
	if f.deletePendingCreatedLocked(value, node) {
		f.stateMu.Unlock()
		return 0
	}
	mutationTicket := f.nextMutationPublishTicketLocked()
	f.stateMu.Unlock()
	defer f.completeMutationTicket(mutationTicket)
	mutationCtx, cancel, err := f.mutationContext(ctx)
	if err != nil {
		return errnoFor(err)
	}
	defer cancel()
	if err := f.remote.Delete(mutationCtx, node.MemoryID, node.LatestVersionID); err != nil {
		return errnoFor(err)
	}
	f.stateMu.Lock()
	canPublish := f.canPublishMutationLocked(node.MemoryID, mutationTicket, value)
	f.recordMutationSuccessLocked(node.MemoryID, mutationTicket, value)
	f.clearPendingCreatedThroughLocked(mutationTicket, value)
	if canPublish {
		f.index.RemoveIdentity(value, node.MemoryID, f.localDirPathsLocked())
		f.invalidateMetadataLocked(value)
		f.advanceMutationLocked()
	}
	f.stateMu.Unlock()
	return 0
}

func (f *FileSystem) deleteWithMutationErrorReporter(ctx context.Context, value string, reporter func(MutationErrorEvent)) syscall.Errno {
	value = canonicalPathKey(value)
	unlock := f.pathLocks.Lock(value)
	defer func() {
		if unlock != nil {
			unlock()
		}
	}()
	if f.readOnly() {
		return syscall.EROFS
	}
	f.stateMu.Lock()
	node, ok := f.index.Lookup(value)
	if !ok {
		f.stateMu.Unlock()
		return syscall.ENOENT
	}
	if node.IsDir {
		f.stateMu.Unlock()
		return syscall.EISDIR
	}
	if f.deletePendingCreatedLocked(value, node) {
		f.stateMu.Unlock()
		return 0
	}
	mutationTicket := f.nextMutationPublishTicketLocked()
	f.stateMu.Unlock()
	defer f.completeMutationTicket(mutationTicket)
	mutationCtx, cancel, err := f.mutationContext(ctx)
	if err != nil {
		return errnoFor(err)
	}
	defer cancel()
	if err := f.remote.Delete(mutationCtx, node.MemoryID, node.LatestVersionID); err != nil {
		result := errnoFor(err)
		unlock()
		unlock = nil
		safelyReportRemoteError(reporter, "delete", value, err, result)
		return result
	}
	f.stateMu.Lock()
	canPublish := f.canPublishMutationLocked(node.MemoryID, mutationTicket, value)
	f.recordMutationSuccessLocked(node.MemoryID, mutationTicket, value)
	f.clearPendingCreatedThroughLocked(mutationTicket, value)
	if canPublish {
		f.index.RemoveIdentity(value, node.MemoryID, f.localDirPathsLocked())
		f.invalidateMetadataLocked(value)
		f.advanceMutationLocked()
	}
	f.stateMu.Unlock()
	return 0
}

func (f *FileSystem) Mkdir(value string) syscall.Errno {
	value = canonicalPathKey(value)
	unlock := f.pathLocks.Lock(value)
	defer unlock()
	if f.readOnly() {
		return syscall.EROFS
	}
	f.stateMu.Lock()
	defer f.stateMu.Unlock()
	if value == "" {
		return syscall.EEXIST
	}
	if _, exists := f.index.Lookup(value); exists {
		return syscall.EEXIST
	}
	f.localDirs[value] = struct{}{}
	f.index.AddDir(value)
	f.invalidateMetadataLocked(value)
	f.recordLocalPathSuccessLocked(value)
	f.advanceMutationLocked()
	return 0
}

func (f *FileSystem) Rmdir(value string) syscall.Errno {
	value = canonicalPathKey(value)
	unlock := f.pathLocks.Lock(value)
	defer unlock()
	if f.readOnly() {
		return syscall.EROFS
	}
	f.stateMu.Lock()
	defer f.stateMu.Unlock()
	node, ok := f.index.Lookup(value)
	if !ok || !node.IsDir {
		return syscall.ENOENT
	}
	if len(f.index.Children(value)) > 0 {
		return syscall.ENOTEMPTY
	}
	delete(f.localDirs, value)
	f.index.Remove(value)
	f.restoreLocalDirsLocked()
	f.invalidateMetadataLocked(value)
	f.recordLocalPathSuccessLocked(value)
	f.advanceMutationLocked()
	return 0
}

func (f *FileSystem) nextObservationTicketLocked() uint64 {
	f.nextObservationTicket++
	return f.nextObservationTicket
}

func (f *FileSystem) nextMutationPublishTicketLocked() uint64 {
	f.nextMutationTicket++
	return f.nextMutationTicket
}

func (f *FileSystem) canPublishMutationLocked(itemID string, ticket uint64, paths ...string) bool {
	if f.publishedTicket[itemID] > ticket {
		return false
	}
	for _, value := range paths {
		if f.publishedPathTicket[canonicalPathKey(value)] > ticket {
			return false
		}
	}
	return true
}

func (f *FileSystem) recordMutationSuccessLocked(itemID string, ticket uint64, paths ...string) {
	bucket := f.successKeysByTicket[ticket]
	if itemID != "" && ticket > f.publishedTicket[itemID] {
		f.publishedTicket[itemID] = ticket
		bucket.itemIDs = append(bucket.itemIDs, itemID)
	}
	seenPaths := make(map[string]struct{}, len(paths))
	for _, value := range paths {
		value = canonicalPathKey(value)
		if value == "" {
			continue
		}
		if _, seen := seenPaths[value]; seen {
			continue
		}
		seenPaths[value] = struct{}{}
		if ticket > f.publishedPathTicket[value] {
			f.publishedPathTicket[value] = ticket
			bucket.paths = append(bucket.paths, value)
		}
	}
	if len(bucket.itemIDs) != 0 || len(bucket.paths) != 0 {
		f.successKeysByTicket[ticket] = bucket
	}
}

func (f *FileSystem) recordLocalPathSuccessLocked(value string) uint64 {
	ticket := f.nextMutationPublishTicketLocked()
	f.recordMutationSuccessLocked("", ticket, value)
	f.completeMutationTicketLocked(ticket)
	return ticket
}

// deletePendingCreatedLocked removes a placeholder that has not acquired a
// stable remote item ID. The dirty handle intentionally keeps its created
// state; without the pending owner its next Commit returns ESTALE instead of
// recreating an unlinked name. stateMu and the path lock must be held.
func (f *FileSystem) deletePendingCreatedLocked(value string, node Node) bool {
	if node.MemoryID != "" {
		return false
	}
	delete(f.pendingCreated, value)
	f.index.Remove(value)
	f.restoreLocalDirsLocked()
	f.invalidateMetadataLocked(value)
	f.recordLocalPathSuccessLocked(value)
	f.advanceMutationLocked()
	return true
}

// renamePendingCreatedLocked moves a local-only placeholder and its owning
// handle. There is no Item API operation that can atomically overwrite a
// remote target with a source that has not been created remotely, so an
// occupied target is rejected without sending an empty item ID. stateMu and
// both path locks must be held.
func (f *FileSystem) renamePendingCreatedLocked(from, to string, node Node) (bool, syscall.Errno) {
	if node.MemoryID != "" {
		return false, 0
	}
	if _, exists := f.index.Lookup(to); exists {
		return true, syscall.EEXIST
	}
	owner, exists := f.pendingCreated[from]
	if !exists || !owner.handle.RenameCreated(from, to) {
		return true, syscall.ESTALE
	}
	ticket := f.nextMutationPublishTicketLocked()
	f.recordMutationSuccessLocked("", ticket, from, to)
	f.completeMutationTicketLocked(ticket)
	f.index.Remove(from)
	f.index.Upsert(client.Memory{Path: to})
	delete(f.pendingCreated, from)
	owner.creationTicket = ticket
	f.pendingCreated[to] = owner
	f.restoreLocalDirsLocked()
	f.invalidateMetadataLocked(from, to)
	f.advanceMutationLocked()
	return true, 0
}

func (f *FileSystem) clearPendingCreatedThroughLocked(mutationTicket uint64, paths ...string) {
	removedPlaceholder := false
	seenPaths := make(map[string]struct{}, len(paths))
	for _, value := range paths {
		value = canonicalPathKey(value)
		if value == "" {
			continue
		}
		if _, seen := seenPaths[value]; seen {
			continue
		}
		seenPaths[value] = struct{}{}
		owner, exists := f.pendingCreated[value]
		if !exists || owner.creationTicket > mutationTicket {
			continue
		}
		delete(f.pendingCreated, value)
		if node, exists := f.index.Lookup(value); exists && !node.IsDir && node.MemoryID == "" {
			f.index.Remove(value)
			removedPlaceholder = true
		}
	}
	if removedPlaceholder {
		f.restoreLocalDirsLocked()
	}
}

func (f *FileSystem) completeMutationTicket(ticket uint64) {
	f.stateMu.Lock()
	f.completeMutationTicketLocked(ticket)
	f.stateMu.Unlock()
}

func (f *FileSystem) completeMutationTicketLocked(ticket uint64) {
	if ticket <= f.completedMutationThrough {
		return
	}
	f.completedMutationTickets[ticket] = struct{}{}
	for {
		next := f.completedMutationThrough + 1
		if _, completed := f.completedMutationTickets[next]; !completed {
			return
		}
		delete(f.completedMutationTickets, next)
		bucket := f.successKeysByTicket[next]
		for _, itemID := range bucket.itemIDs {
			if f.publishedTicket[itemID] == next {
				delete(f.publishedTicket, itemID)
			}
		}
		for _, value := range bucket.paths {
			if f.publishedPathTicket[value] == next {
				delete(f.publishedPathTicket, value)
			}
		}
		delete(f.successKeysByTicket, next)
		f.completedMutationThrough = next
	}
}

func (f *FileSystem) advanceMutationLocked() {
	f.mutationGen++
	clear(f.pointObservations)
}

func (f *FileSystem) invalidateMetadataLocked(values ...string) {
	for _, value := range values {
		value = canonicalPathKey(value)
		delete(f.positiveExpiry, value)
		f.removeNegativeLocked(value, false)
	}
}

func (f *FileSystem) setNegativeLocked(value string, expiry time.Time) {
	if item := f.negativeItems[value]; item != nil {
		f.negativeOrder.Remove(item)
	}
	f.negativeExpiry[value] = expiry
	f.negativeItems[value] = f.negativeOrder.PushBack(value)
	for len(f.negativeExpiry) > maxNegativeEntries {
		oldest := f.negativeOrder.Front()
		f.removeNegativeLocked(oldest.Value.(string), true)
	}
}

func (f *FileSystem) purgeExpiredNegativesLocked(now time.Time) {
	for oldest := f.negativeOrder.Front(); oldest != nil; oldest = f.negativeOrder.Front() {
		value := oldest.Value.(string)
		if expiry := f.negativeExpiry[value]; now.Before(expiry) {
			return
		}
		f.removeNegativeLocked(value, true)
	}
}

func (f *FileSystem) removeNegativeLocked(value string, removeObservation bool) {
	delete(f.negativeExpiry, value)
	if item := f.negativeItems[value]; item != nil {
		f.negativeOrder.Remove(item)
		delete(f.negativeItems, value)
	}
	if removeObservation {
		if observation, exists := f.pointObservations[value]; exists && !observation.exists {
			delete(f.pointObservations, value)
			f.observationEpoch++
		}
	}
}

// AbandonCreated releases only filesystem-local state for an uncommitted
// created handle. It never submits remote work and intentionally leaves the
// handle dirty so Release cannot become an implicit retry path.
func (f *FileSystem) AbandonCreated(handle *Handle) {
	memory := handle.Memory()
	if memory.MemoryID != "" {
		return
	}
	value := canonicalPathKey(memory.Path)
	unlock := f.pathLocks.Lock(value)
	defer unlock()

	f.stateMu.Lock()
	defer f.stateMu.Unlock()
	owner, exists := f.pendingCreated[value]
	if !exists || owner.handle != handle {
		return
	}
	delete(f.pendingCreated, value)
	if node, exists := f.index.Lookup(value); exists && !node.IsDir && node.MemoryID == "" {
		f.index.Remove(value)
		f.restoreLocalDirsLocked()
	}
	f.invalidateMetadataLocked(value)
	f.recordLocalPathSuccessLocked(value)
	f.advanceMutationLocked()
}

func (f *FileSystem) restoreLocalDirsLocked() {
	for _, dir := range f.localDirPathsLocked() {
		f.index.AddDir(dir)
	}
}

func (f *FileSystem) localDirPathsLocked() []string {
	dirs := make([]string, 0, len(f.localDirs))
	for dir := range f.localDirs {
		dirs = append(dirs, dir)
	}
	return dirs
}

func joinPath(parent, name string) string {
	if parent == "" {
		return canonicalPathKey(name)
	}
	return canonicalPathKey(path.Join(parent, name))
}

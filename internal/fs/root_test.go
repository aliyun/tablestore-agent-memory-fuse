package fs

import (
	"context"
	"syscall"
	"testing"
	"time"

	"agent-memory-fuse/internal/client"

	gofusefs "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

type lockConflictOnceRemote struct {
	*fakeRemote
	updateCalls int
}

func (r *lockConflictOnceRemote) Update(ctx context.Context, itemID string, content []byte, expectedVersionID string) (client.Memory, error) {
	r.updateCalls++
	if r.updateCalls == 1 {
		return client.Memory{}, &client.APIError{StatusCode: 409, Code: "LOCK_CONFLICT"}
	}
	return r.fakeRemote.Update(ctx, itemID, content, expectedVersionID)
}

func TestReleaseDoesNotRetryFailedFlush(t *testing.T) {
	ctx := context.Background()
	remote := &lockConflictOnceRemote{fakeRemote: &fakeRemote{memories: map[string]client.Memory{
		"a.md": {
			MemoryID:        "mem_a",
			Path:            "a.md",
			Content:         "lock-ok",
			LatestVersionID: "fmver_1",
		},
	}}}
	filesystem := NewFileSystem(remote, Options{})
	if err := filesystem.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	handle, errno := filesystem.Open(ctx, "a.md")
	if errno != 0 {
		t.Fatalf("open errno=%v", errno)
	}
	file := &fileHandle{filesystem: filesystem, handle: handle, flags: syscall.O_WRONLY}
	if _, errno := file.Write(ctx, []byte("lock-fail"), 0); errno != 0 {
		t.Fatalf("write errno=%v", errno)
	}

	if errno := file.Flush(ctx); errno != syscall.EAGAIN {
		t.Fatalf("flush errno=%v want EAGAIN", errno)
	}
	if !handle.Dirty() {
		t.Fatal("failed flush must leave the handle dirty for an explicit retry")
	}
	filesystem.stateMu.Lock()
	beforeReleaseGeneration := filesystem.mutationGen
	filesystem.stateMu.Unlock()
	if errno := file.Release(ctx); errno != 0 {
		t.Fatalf("release errno=%v", errno)
	}
	filesystem.stateMu.Lock()
	afterReleaseGeneration := filesystem.mutationGen
	filesystem.stateMu.Unlock()
	if afterReleaseGeneration != beforeReleaseGeneration {
		t.Fatalf("non-created Release advanced mutationGen: before=%d after=%d", beforeReleaseGeneration, afterReleaseGeneration)
	}
	if remote.updateCalls != 1 {
		t.Fatalf("Update calls=%d want 1; Release must not retry the failed Flush", remote.updateCalls)
	}
	if got := remote.memories["a.md"].Content; got != "lock-ok" {
		t.Fatalf("remote content=%q want %q", got, "lock-ok")
	}
	if !handle.Dirty() {
		t.Fatal("Release must not clear dirty state; retries belong to a still-live dup fd or an explicit Fsync/Flush before Release")
	}
}

func TestReleaseAbandonsFailedCreatedHandleWithoutRemoteRetry(t *testing.T) {
	ctx := context.Background()
	remote := &fakeRemote{
		memories:  map[string]client.Memory{},
		createErr: &client.APIError{StatusCode: 503, Code: "UNAVAILABLE"},
	}
	filesystem := NewFileSystem(remote, Options{})
	handle, errno := filesystem.Create("a.md")
	if errno != 0 {
		t.Fatalf("Create errno=%v", errno)
	}
	file := &fileHandle{filesystem: filesystem, handle: handle, flags: syscall.O_WRONLY}
	if errno := file.Flush(ctx); errno != syscall.EAGAIN {
		t.Fatalf("failed created Flush errno=%v want EAGAIN", errno)
	}
	filesystem.stateMu.Lock()
	beforeReleaseGeneration := filesystem.mutationGen
	filesystem.stateMu.Unlock()
	if errno := file.Release(ctx); errno != 0 {
		t.Fatalf("Release errno=%v", errno)
	}
	filesystem.stateMu.Lock()
	afterReleaseGeneration := filesystem.mutationGen
	filesystem.stateMu.Unlock()
	if afterReleaseGeneration != beforeReleaseGeneration+1 {
		t.Fatalf("created Release mutationGen=%d want %d", afterReleaseGeneration, beforeReleaseGeneration+1)
	}
	remote.mu.Lock()
	createCalls := 0
	for _, call := range remote.calls {
		if call == "Create" {
			createCalls++
		}
	}
	remote.mu.Unlock()
	if createCalls != 1 {
		t.Fatalf("Create calls=%d want 1; Release must not retry", createCalls)
	}
	if !handle.Dirty() {
		t.Fatal("Release must not clear the abandoned handle's dirty state")
	}
	filesystem.stateMu.Lock()
	_, pending := filesystem.pendingCreated["a.md"]
	filesystem.stateMu.Unlock()
	if pending {
		t.Fatal("Release left the failed create pending marker")
	}
	if node, ok := filesystem.Index().Lookup("a.md"); ok {
		t.Fatalf("Release left the empty placeholder: node=%+v", node)
	}
	if err := filesystem.Refresh(ctx); err != nil {
		t.Fatalf("Refresh after Release err=%v", err)
	}
	if node, ok := filesystem.Index().Lookup("a.md"); ok {
		t.Fatalf("Refresh revived the abandoned placeholder: node=%+v", node)
	}
	if _, errno := filesystem.Create("a.md"); errno != 0 {
		t.Fatalf("same-path Create after Release errno=%v", errno)
	}
}

func TestReleaseClearsAmbiguousPendingWithoutDeletingAuthoritativeNode(t *testing.T) {
	ctx := context.Background()
	remote := &ambiguousCreateRemote{fakeRemote: &fakeRemote{memories: map[string]client.Memory{}}}
	filesystem := NewFileSystem(remote, Options{})
	handle, errno := filesystem.Create("a.md")
	if errno != 0 {
		t.Fatalf("Create errno=%v", errno)
	}
	file := &fileHandle{filesystem: filesystem, handle: handle, flags: syscall.O_WRONLY}
	if errno := file.Flush(ctx); errno != syscall.EAGAIN {
		t.Fatalf("ambiguous created Flush errno=%v want EAGAIN", errno)
	}
	if err := filesystem.Refresh(ctx); err != nil {
		t.Fatalf("authoritative Refresh err=%v", err)
	}
	if node, ok := filesystem.Index().Lookup("a.md"); !ok || node.MemoryID == "" || node.LatestVersionID == "" {
		t.Fatalf("authoritative node hidden by pending placeholder: node=%+v ok=%v", node, ok)
	}
	filesystem.stateMu.Lock()
	beforeReleaseGeneration := filesystem.mutationGen
	filesystem.stateMu.Unlock()
	if errno := file.Release(ctx); errno != 0 {
		t.Fatalf("Release errno=%v", errno)
	}
	filesystem.stateMu.Lock()
	afterReleaseGeneration := filesystem.mutationGen
	filesystem.stateMu.Unlock()
	if afterReleaseGeneration != beforeReleaseGeneration+1 {
		t.Fatalf("ambiguous Release mutationGen=%d want %d", afterReleaseGeneration, beforeReleaseGeneration+1)
	}
	remote.mu.Lock()
	createCalls := 0
	for _, call := range remote.calls {
		if call == "Create" {
			createCalls++
		}
	}
	remote.mu.Unlock()
	if createCalls != 1 {
		t.Fatalf("Create calls=%d want 1; Release must not retry ambiguous Create", createCalls)
	}
	if !handle.Dirty() {
		t.Fatal("Release must not clear the ambiguous handle's dirty state")
	}
	filesystem.stateMu.Lock()
	_, pending := filesystem.pendingCreated["a.md"]
	filesystem.stateMu.Unlock()
	if pending {
		t.Fatal("Release left the ambiguous pending marker")
	}
	if node, ok := filesystem.Index().Lookup("a.md"); !ok || node.MemoryID == "" {
		t.Fatalf("Release deleted authoritative node: node=%+v ok=%v", node, ok)
	}
	if err := filesystem.Refresh(ctx); err != nil {
		t.Fatalf("post-Release Refresh err=%v", err)
	}
	if node, ok := filesystem.Index().Lookup("a.md"); !ok || node.MemoryID == "" {
		t.Fatalf("post-Release authoritative node=%+v ok=%v", node, ok)
	}
}

func setupReplacementPendingCreate(t *testing.T) (*FileSystem, *ambiguousCreateOnceRemote, *Handle, *Handle) {
	t.Helper()
	ctx := context.Background()
	remote := &ambiguousCreateOnceRemote{fakeRemote: &fakeRemote{memories: map[string]client.Memory{}}}
	filesystem := NewFileSystem(remote, Options{})

	oldHandle, errno := filesystem.Create("a.md")
	if errno != 0 {
		t.Fatalf("old Create errno=%v", errno)
	}
	if _, err := oldHandle.WriteAt([]byte("old"), 0); err != nil {
		t.Fatal(err)
	}
	if errno := filesystem.Commit(ctx, oldHandle); errno != syscall.EAGAIN {
		t.Fatalf("ambiguous old Commit errno=%v want EAGAIN", errno)
	}
	if err := filesystem.Refresh(ctx); err != nil {
		t.Fatalf("authoritative Refresh err=%v", err)
	}
	if node, ok := filesystem.Index().Lookup("a.md"); !ok || node.MemoryID == "" {
		t.Fatalf("authoritative node after ambiguous Create=%+v ok=%v", node, ok)
	}
	if errno := filesystem.Delete(ctx, "a.md"); errno != 0 {
		t.Fatalf("authoritative Delete errno=%v", errno)
	}

	newHandle, errno := filesystem.Create("a.md")
	if errno != 0 {
		t.Fatalf("replacement Create errno=%v", errno)
	}
	if _, err := newHandle.WriteAt([]byte("new"), 0); err != nil {
		t.Fatal(err)
	}
	if node, ok := filesystem.Index().Lookup("a.md"); !ok || node.IsDir || node.MemoryID != "" {
		t.Fatalf("replacement pending placeholder=%+v ok=%v", node, ok)
	}
	filesystem.stateMu.Lock()
	_, pending := filesystem.pendingCreated["a.md"]
	filesystem.stateMu.Unlock()
	if !pending {
		t.Fatal("replacement Create did not register a pending marker")
	}
	return filesystem, remote, oldHandle, newHandle
}

func TestReleaseDoesNotAbandonReplacementPendingCreate(t *testing.T) {
	ctx := context.Background()
	filesystem, remote, oldHandle, newHandle := setupReplacementPendingCreate(t)
	oldFile := &fileHandle{filesystem: filesystem, handle: oldHandle, flags: syscall.O_WRONLY}

	filesystem.stateMu.Lock()
	beforeReleaseGeneration := filesystem.mutationGen
	filesystem.stateMu.Unlock()
	if errno := oldFile.Release(ctx); errno != 0 {
		t.Fatalf("old Release errno=%v", errno)
	}
	filesystem.stateMu.Lock()
	afterReleaseGeneration := filesystem.mutationGen
	_, pending := filesystem.pendingCreated["a.md"]
	filesystem.stateMu.Unlock()
	if afterReleaseGeneration != beforeReleaseGeneration {
		t.Fatalf("stale Release advanced mutationGen: before=%d after=%d", beforeReleaseGeneration, afterReleaseGeneration)
	}
	if !pending {
		t.Fatal("stale Release removed replacement pending marker")
	}
	if node, ok := filesystem.Index().Lookup("a.md"); !ok || node.IsDir || node.MemoryID != "" {
		t.Fatalf("stale Release removed replacement placeholder: node=%+v ok=%v", node, ok)
	}
	if got := remote.callCount("Create"); got != 1 {
		t.Fatalf("Create calls after stale Release=%d want 1", got)
	}

	if errno := filesystem.Commit(ctx, newHandle); errno != 0 {
		t.Fatalf("replacement Commit errno=%v", errno)
	}
	if got := remote.callCount("Create"); got != 2 {
		t.Fatalf("Create calls after replacement Commit=%d want 2", got)
	}
	remote.mu.Lock()
	content := remote.memories["a.md"].Content
	remote.mu.Unlock()
	if content != "new" {
		t.Fatalf("replacement remote content=%q want %q", content, "new")
	}
}

func TestLateCreatedHandleFlushCannotStealReplacementPendingCreate(t *testing.T) {
	ctx := context.Background()
	filesystem, remote, oldHandle, newHandle := setupReplacementPendingCreate(t)
	oldFile := &fileHandle{filesystem: filesystem, handle: oldHandle, flags: syscall.O_WRONLY}
	createCallsBeforeRetry := remote.callCount("Create")

	if errno := oldFile.Flush(ctx); errno != syscall.ESTALE {
		t.Fatalf("late old Flush errno=%v want ESTALE", errno)
	}
	if got := remote.callCount("Create"); got != createCallsBeforeRetry {
		t.Fatalf("late old Flush made remote Create calls: before=%d after=%d", createCallsBeforeRetry, got)
	}
	if !oldHandle.Dirty() {
		t.Fatal("stale old Flush cleared dirty state")
	}
	filesystem.stateMu.Lock()
	_, pending := filesystem.pendingCreated["a.md"]
	filesystem.stateMu.Unlock()
	if !pending {
		t.Fatal("late old Flush removed replacement pending marker")
	}
	if node, ok := filesystem.Index().Lookup("a.md"); !ok || node.IsDir || node.MemoryID != "" {
		t.Fatalf("late old Flush replaced pending placeholder: node=%+v ok=%v", node, ok)
	}

	if errno := filesystem.Commit(ctx, newHandle); errno != 0 {
		t.Fatalf("replacement Commit errno=%v", errno)
	}
	if got := remote.callCount("Create"); got != createCallsBeforeRetry+1 {
		t.Fatalf("Create calls after replacement Commit=%d want %d", got, createCallsBeforeRetry+1)
	}
	remote.mu.Lock()
	content := remote.memories["a.md"].Content
	remote.mu.Unlock()
	if content != "new" {
		t.Fatalf("replacement remote content=%q want %q", content, "new")
	}
}

// touch 的 utimens 只带时间戳不带 size——Setattr 必须作 no-op 成功返回
// （服务端 createdAt/updatedAt 是时间权威，客户端不持久化任意 mtime），
// 否则 touch 在挂载点直接失败（e2e 命令矩阵回归）。
func TestSetattrTimeOnlyIsNoop(t *testing.T) {
	remote := &fakeRemote{memories: map[string]client.Memory{"a.md": {MemoryID: "m", Path: "a.md", Content: "one", ContentSizeBytes: 3, ContentSHA256: "s"}}}
	filesystem := NewFileSystem(remote, Options{})
	if err := filesystem.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	node := &fileNode{filesystem: filesystem, path: "a.md"}

	in := &fuse.SetAttrIn{}
	in.Valid = fuse.FATTR_ATIME | fuse.FATTR_MTIME

	// 带 handle（futimens(fd) 路径）：从 handle 填 attr。
	handle, errno := filesystem.Open(context.Background(), "a.md")
	if errno != 0 {
		t.Fatalf("open errno=%v", errno)
	}
	var out fuse.AttrOut
	if errno := node.Setattr(context.Background(), &fileHandle{filesystem: filesystem, handle: handle}, in, &out); errno != 0 {
		t.Fatalf("time-only setattr with handle: errno=%v", errno)
	}
	if out.Attr.Size != 3 {
		t.Fatalf("attr size=%d want 3", out.Attr.Size)
	}

	// 无 handle（utimensat 路径）：退到 Getattr 语义。
	if errno := node.Setattr(context.Background(), nil, in, &out); errno != 0 {
		t.Fatalf("time-only setattr without handle: errno=%v", errno)
	}
	if out.Attr.Size != 3 {
		t.Fatalf("attr size=%d want 3", out.Attr.Size)
	}
}

func TestHandleAttrTimeoutUsesMetadataTTL(t *testing.T) {
	const metadataTTL = 7 * time.Second
	remote := &fakeRemote{memories: map[string]client.Memory{
		"a.md": {MemoryID: "mem_a", Path: "a.md", Content: "one", ContentSizeBytes: 3, LatestVersionID: "fmver_1"},
	}}
	filesystem := NewFileSystem(remote, Options{MetadataTTL: metadataTTL})
	handle, errno := filesystem.Open(context.Background(), "a.md")
	if errno != 0 {
		t.Fatalf("Open errno=%v", errno)
	}
	file := &fileHandle{filesystem: filesystem, handle: handle}
	node := &fileNode{filesystem: filesystem, path: "a.md"}

	tests := []struct {
		name string
		attr func(*fuse.AttrOut) syscall.Errno
	}{
		{
			name: "file handle getattr",
			attr: func(out *fuse.AttrOut) syscall.Errno {
				return file.Getattr(context.Background(), out)
			},
		},
		{
			name: "setattr with file handle",
			attr: func(out *fuse.AttrOut) syscall.Errno {
				input := &fuse.SetAttrIn{}
				input.Valid = fuse.FATTR_ATIME | fuse.FATTR_MTIME
				return node.Setattr(context.Background(), file, input, out)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var out fuse.AttrOut
			if errno := test.attr(&out); errno != 0 {
				t.Fatalf("attr errno=%v", errno)
			}
			if got := out.Timeout(); got != metadataTTL {
				t.Fatalf("AttrOut.Timeout()=%v want %v", got, metadataTTL)
			}
		})
	}
}

func TestDirNodeLookupUsesPointLookupWithoutList(t *testing.T) {
	remote := &fakeRemote{memories: map[string]client.Memory{
		"a.md": {MemoryID: "mem_a", Path: "a.md", ContentSizeBytes: 7, LatestVersionID: "fmver_1"},
	}}
	filesystem := NewFileSystem(remote, Options{MetadataTTL: time.Minute})
	root := NewRoot(filesystem).(*dirNode)
	gofusefs.NewNodeFS(root, &gofusefs.Options{})

	var out fuse.EntryOut
	if _, errno := root.Lookup(context.Background(), "a.md", &out); errno != 0 {
		t.Fatalf("Lookup errno=%v", errno)
	}
	if got := remote.callCount("Stat"); got != 1 {
		t.Fatalf("Stat calls=%d want 1", got)
	}
	if got := remote.callCount("List"); got != 0 {
		t.Fatalf("List calls=%d want 0", got)
	}
}

func TestFileNodeGetattrUsesPointStatWithoutList(t *testing.T) {
	remote := &fakeRemote{memories: map[string]client.Memory{
		"a.md": {MemoryID: "mem_a", Path: "a.md", ContentSizeBytes: 9, LatestVersionID: "fmver_1"},
	}}
	filesystem := NewFileSystem(remote, Options{MetadataTTL: time.Minute})
	node := &fileNode{filesystem: filesystem, path: "a.md"}

	var out fuse.AttrOut
	if errno := node.Getattr(context.Background(), nil, &out); errno != 0 {
		t.Fatalf("Getattr errno=%v", errno)
	}
	if out.Attr.Size != 9 {
		t.Fatalf("Getattr size=%d want 9", out.Attr.Size)
	}
	if got := remote.callCount("Stat"); got != 1 {
		t.Fatalf("Stat calls=%d want 1", got)
	}
	if got := remote.callCount("List"); got != 0 {
		t.Fatalf("List calls=%d want 0", got)
	}
}

func TestDirNodeReaddirUsesListExclusively(t *testing.T) {
	remote := &fakeRemote{memories: map[string]client.Memory{
		"a.md": {MemoryID: "mem_a", Path: "a.md", LatestVersionID: "fmver_1"},
	}}
	filesystem := NewFileSystem(remote, Options{DirectoryTTL: time.Minute})
	root := NewRoot(filesystem).(*dirNode)

	stream, errno := root.Readdir(context.Background())
	if errno != 0 {
		t.Fatalf("Readdir errno=%v", errno)
	}
	stream.Close()
	if got := remote.callCount("List"); got != 1 {
		t.Fatalf("List calls=%d want 1", got)
	}
	if got := remote.callCount("Stat"); got != 0 {
		t.Fatalf("Stat calls=%d want 0", got)
	}
}

func TestDirNodeLookupNFDAndReaddirUseOneNFCEntry(t *testing.T) {
	const nfd = "cafe\u0301.md"
	const nfc = "caf\u00e9.md"
	remote := &fakeRemote{memories: map[string]client.Memory{
		nfc: {MemoryID: "mem_unicode", Path: nfc, LatestVersionID: "fmver_1"},
	}}
	filesystem := NewFileSystem(remote, Options{})
	root := NewRoot(filesystem).(*dirNode)
	gofusefs.NewNodeFS(root, &gofusefs.Options{})

	var out fuse.EntryOut
	if _, errno := root.Lookup(context.Background(), nfd, &out); errno != 0 {
		t.Fatalf("NFD Lookup errno=%v", errno)
	}
	stream, errno := root.Readdir(context.Background())
	if errno != 0 {
		t.Fatalf("Readdir errno=%v", errno)
	}
	defer stream.Close()
	var names []string
	for stream.HasNext() {
		entry, nextErrno := stream.Next()
		if nextErrno != 0 {
			t.Fatalf("Readdir Next errno=%v", nextErrno)
		}
		names = append(names, entry.Name)
	}
	if len(names) != 1 || names[0] != nfc {
		t.Fatalf("Readdir names=%q want [%q]", names, nfc)
	}
}

func TestDirNodeLookupKnownSnapshotAndLocalDirectoriesDoesNotStat(t *testing.T) {
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
	root := NewRoot(filesystem).(*dirNode)
	gofusefs.NewNodeFS(root, &gofusefs.Options{})

	for _, name := range []string{"snapshot", "local"} {
		var out fuse.EntryOut
		inode, errno := root.Lookup(context.Background(), name, &out)
		if errno != 0 || inode == nil || out.Attr.Mode&fuse.S_IFDIR == 0 {
			t.Fatalf("Lookup(%q) inode=%v mode=%#o errno=%v", name, inode, out.Attr.Mode, errno)
		}
	}
	if got := remote.callCount("Stat"); got != 0 {
		t.Fatalf("known directory Stat calls=%d want 0", got)
	}
}

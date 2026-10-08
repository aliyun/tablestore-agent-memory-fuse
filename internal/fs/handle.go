package fs

import (
	"context"
	"errors"
	"io"
	"sync"
	"unicode/utf8"

	"agent-memory-fuse/internal/client"
)

const MaxContentBytes = 102400

var (
	ErrTooLarge    = errors.New("file exceeds 100 KiB")
	ErrInvalidUTF8 = errors.New("file is not valid UTF-8")
)

// CommitFunc 提交 handle 内容。isNew=true 表示该文件尚未在服务端创建
// （必须走 AddItem）；否则按稳定 item ID 更新并携带当前资源版本做并发前置条件。
type CommitFunc func(ctx context.Context, memory client.Memory, content []byte, isNew bool) (client.Memory, error)

type Handle struct {
	mu         sync.Mutex
	memory     client.Memory
	content    []byte
	generation uint64
	committed  uint64
	created    bool // 本地新建、远端尚未落地；首次成功提交后清除
}

func NewHandle(memory client.Memory) *Handle {
	return &Handle{memory: memory, content: []byte(memory.Content)}
}

// NewCreatedHandle 为本地新建（O_CREAT）文件建 handle：远端不存在，
// generation 置 1 使 touch（零写入）也会提交一次空内容 Create。
func NewCreatedHandle(path string) *Handle {
	h := NewHandle(client.Memory{Path: path})
	h.created = true
	h.generation = 1
	return h
}

// RenameCreated moves an uncommitted create to a new local path. The caller
// owns the filesystem path locks, while Handle.mu serializes this path update
// with writes and the first Commit snapshot.
func (h *Handle) RenameCreated(from, to string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.created || h.memory.MemoryID != "" || h.memory.Path != from {
		return false
	}
	h.memory.Path = to
	return true
}

func (h *Handle) ReadAt(destination []byte, offset int64) (int, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if offset < 0 {
		return 0, errors.New("negative offset")
	}
	if offset >= int64(len(h.content)) {
		return 0, io.EOF
	}
	n := copy(destination, h.content[offset:])
	if n < len(destination) {
		return n, io.EOF
	}
	return n, nil
}

func (h *Handle) WriteAt(source []byte, offset int64) (int, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if offset < 0 {
		return 0, errors.New("negative offset")
	}
	end := int(offset) + len(source)
	if end > MaxContentBytes {
		return 0, ErrTooLarge
	}
	if end > len(h.content) {
		h.content = append(h.content, make([]byte, end-len(h.content))...)
	}
	copy(h.content[int(offset):], source)
	h.generation++
	return len(source), nil
}

// Append chooses the tail offset and mutates the buffer under one lock. This
// prevents concurrent O_APPEND requests on the same handle from observing the
// same pre-write length and overwriting one another.
func (h *Handle) Append(source []byte) (int, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	end := len(h.content) + len(source)
	if end > MaxContentBytes {
		return 0, ErrTooLarge
	}
	h.content = append(h.content, source...)
	h.generation++
	return len(source), nil
}

func (h *Handle) Truncate(size int64) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if size < 0 {
		return errors.New("negative size")
	}
	if size > MaxContentBytes {
		return ErrTooLarge
	}
	if int(size) < len(h.content) {
		h.content = h.content[:size]
	} else if int(size) > len(h.content) {
		h.content = append(h.content, make([]byte, int(size)-len(h.content))...)
	}
	h.generation++
	return nil
}

func (h *Handle) Bytes() []byte {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]byte(nil), h.content...)
}
func (h *Handle) Memory() client.Memory { h.mu.Lock(); defer h.mu.Unlock(); return h.memory }

// Dirty 报告 handle 是否有未提交的本地修改（含本地新建未落地）。
func (h *Handle) Dirty() bool { h.mu.Lock(); defer h.mu.Unlock(); return h.generation != h.committed }

func (h *Handle) Commit(ctx context.Context, commit CommitFunc) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.generation == h.committed {
		return nil
	}
	if !utf8.Valid(h.content) {
		return ErrInvalidUTF8
	}
	memory, err := commit(ctx, h.memory, append([]byte(nil), h.content...), h.created)
	if err != nil {
		return err
	}
	h.memory = memory
	h.content = []byte(memory.Content)
	h.committed = h.generation
	h.created = false
	return nil
}

package fs

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"agent-memory-fuse/internal/client"
)

func TestHandleByteWritesTruncateAndSingleGenerationCommit(t *testing.T) {
	h := NewHandle(client.Memory{MemoryID: "mem_a", Path: "a", Content: "hello", ContentSHA256: "sha", LatestVersionID: "fmver_1"})
	if _, err := h.WriteAt([]byte("XY"), 1); err != nil {
		t.Fatal(err)
	}
	if err := h.Truncate(4); err != nil {
		t.Fatal(err)
	}
	if got := string(h.Bytes()); got != "hXYl" {
		t.Fatalf("content=%q", got)
	}
	commits := 0
	commit := func(_ context.Context, memory client.Memory, content []byte, isNew bool) (client.Memory, error) {
		commits++
		if memory.MemoryID != "mem_a" || memory.Path != "a" || memory.LatestVersionID != "fmver_1" || string(content) != "hXYl" || isNew {
			t.Fatalf("memory=%+v content=%q isNew=%v", memory, content, isNew)
		}
		return client.Memory{MemoryID: "mem_a", Path: memory.Path, Content: string(content), ContentSHA256: "sha2", LatestVersionID: "fmver_2"}, nil
	}
	if err := h.Commit(context.Background(), commit); err != nil {
		t.Fatal(err)
	}
	if err := h.Commit(context.Background(), commit); err != nil {
		t.Fatal(err)
	}
	if commits != 1 {
		t.Fatalf("commits=%d", commits)
	}
	if memory := h.Memory(); memory.MemoryID != "mem_a" || memory.LatestVersionID != "fmver_2" {
		t.Fatalf("memory=%+v", memory)
	}
}

func TestHandleRejectsOversizeAndInvalidUTF8(t *testing.T) {
	h := NewHandle(client.Memory{Path: "a"})
	if _, err := h.WriteAt(make([]byte, MaxContentBytes+1), 0); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("oversize err=%v", err)
	}
	if _, err := h.WriteAt([]byte{0xff}, 0); err != nil {
		t.Fatal(err)
	}
	err := h.Commit(context.Background(), func(context.Context, client.Memory, []byte, bool) (client.Memory, error) {
		return client.Memory{}, nil
	})
	if !errors.Is(err, ErrInvalidUTF8) {
		t.Fatalf("utf8 err=%v", err)
	}
}

func TestCreatedHandleCommitsAsNewOnceThenAsUpdate(t *testing.T) {
	h := NewCreatedHandle("a")
	var news []bool
	commit := func(_ context.Context, memory client.Memory, _ []byte, isNew bool) (client.Memory, error) {
		news = append(news, isNew)
		if len(news) == 1 && (memory.MemoryID != "" || memory.LatestVersionID != "" || memory.Path != "a") {
			t.Fatalf("new memory snapshot=%+v", memory)
		}
		return client.Memory{MemoryID: "mem_a", Path: memory.Path, ContentSHA256: "sha-1", LatestVersionID: "fmver_1"}, nil
	}
	// touch 语义：即便无写入，新建 handle 的首次 Commit 也必须提交（空内容）。
	if err := h.Commit(context.Background(), commit); err != nil {
		t.Fatal(err)
	}
	if _, err := h.WriteAt([]byte("x"), 0); err != nil {
		t.Fatal(err)
	}
	if err := h.Commit(context.Background(), commit); err != nil {
		t.Fatal(err)
	}
	if len(news) != 2 || !news[0] || news[1] {
		t.Fatalf("isNew sequence=%v want [true false]", news)
	}
}

func TestCreatedHandleStaysNewAfterFailedCommit(t *testing.T) {
	h := NewCreatedHandle("a")
	boom := errors.New("boom")
	err := h.Commit(context.Background(), func(_ context.Context, memory client.Memory, _ []byte, isNew bool) (client.Memory, error) {
		if !isNew || memory.MemoryID != "" || memory.LatestVersionID != "" {
			t.Fatalf("failed create snapshot=%+v isNew=%v", memory, isNew)
		}
		return client.Memory{}, boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("err=%v", err)
	}
	var news []bool
	err = h.Commit(context.Background(), func(_ context.Context, memory client.Memory, _ []byte, isNew bool) (client.Memory, error) {
		news = append(news, isNew)
		if memory.MemoryID != "" || memory.LatestVersionID != "" {
			t.Fatalf("retry snapshot=%+v", memory)
		}
		return client.Memory{MemoryID: "mem_a", Path: "a", ContentSHA256: "sha-1", LatestVersionID: "fmver_1"}, nil
	})
	if err != nil || len(news) != 1 || !news[0] {
		t.Fatalf("err=%v news=%v want retry as new", err, news)
	}
}

func TestCreatedHandleRenameChangesOnlyUncommittedOwnerPath(t *testing.T) {
	h := NewCreatedHandle("a.md")
	if !h.RenameCreated("a.md", "b.md") {
		t.Fatal("created handle rename was rejected")
	}
	if got := h.Memory().Path; got != "b.md" {
		t.Fatalf("path=%q want b.md", got)
	}
	if h.RenameCreated("a.md", "c.md") {
		t.Fatal("stale source path renamed created handle")
	}
	if err := h.Commit(context.Background(), func(_ context.Context, memory client.Memory, content []byte, isNew bool) (client.Memory, error) {
		if !isNew || memory.Path != "b.md" {
			t.Fatalf("memory=%+v isNew=%v", memory, isNew)
		}
		return client.Memory{MemoryID: "mem_b", Path: memory.Path, Content: string(content), LatestVersionID: "fmver_1"}, nil
	}); err != nil {
		t.Fatal(err)
	}
	if h.RenameCreated("b.md", "c.md") {
		t.Fatal("committed handle was renamed as a pending create")
	}
}

func TestHandleAppendSerializesOffsetSelection(t *testing.T) {
	h := NewHandle(client.Memory{Path: "append.md"})
	const workers = 64
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		token := fmt.Sprintf("%04d|", i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if _, err := h.Append([]byte(token)); err != nil {
				t.Errorf("Append: %v", err)
			}
		}()
	}
	close(start)
	wg.Wait()
	content := string(h.Bytes())
	if len(content) != workers*5 {
		t.Fatalf("length=%d want=%d", len(content), workers*5)
	}
	for i := 0; i < workers; i++ {
		token := fmt.Sprintf("%04d|", i)
		if count := strings.Count(content, token); count != 1 {
			t.Fatalf("token=%q count=%d", token, count)
		}
	}
}

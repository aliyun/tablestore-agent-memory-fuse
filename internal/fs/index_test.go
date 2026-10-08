package fs

import (
	"testing"

	"agent-memory-fuse/internal/client"
)

func TestPathIndexSynthesizesDirectories(t *testing.T) {
	index := NewPathIndex()
	index.Rebuild([]client.Memory{
		{MemoryID: "mem_a", Path: "notes/daily/a.md", LatestVersionID: "fmver_1"},
		{MemoryID: "mem_b", Path: "notes/b.md", LatestVersionID: "fmver_2"},
		{MemoryID: "mem_root", Path: "root.md", LatestVersionID: "fmver_3"},
	})
	if node, ok := index.Lookup("notes"); !ok || !node.IsDir {
		t.Fatalf("notes=%+v ok=%v", node, ok)
	}
	if node, ok := index.Lookup("notes/daily/a.md"); !ok || node.IsDir || node.MemoryID != "mem_a" || node.LatestVersionID != "fmver_1" {
		t.Fatalf("file=%+v ok=%v", node, ok)
	}
	children := index.Children("notes")
	if len(children) != 2 || children[0].Name != "b.md" || children[1].Name != "daily" {
		t.Fatalf("children=%+v", children)
	}
	index.Remove("notes/b.md")
	if node, ok := index.Lookup("notes/daily/a.md"); !ok || node.MemoryID != "mem_a" || node.LatestVersionID != "fmver_1" {
		t.Fatalf("survivor identity lost after remove rebuild: node=%+v ok=%v", node, ok)
	}
	index.Remove("notes/daily/a.md")
	if _, ok := index.Lookup("notes/daily"); ok {
		t.Fatal("empty implicit directory must disappear")
	}
}

func TestPathIndexNormalizesServerLeadingSlash(t *testing.T) {
	// 服务端返回规范化 "/notes/a.md"，索引键必须与 FUSE 侧 "notes/a.md" 一致。
	index := NewPathIndex()
	index.Upsert(client.Memory{MemoryID: "mem_a", Path: "/notes/a.md", LatestVersionID: "fmver_1"})
	node, ok := index.Lookup("notes/a.md")
	if !ok || node.MemoryID != "mem_a" || node.LatestVersionID != "fmver_1" || node.Path != "notes/a.md" {
		t.Fatalf("node=%+v ok=%v", node, ok)
	}
	children := index.Children("")
	if len(children) != 1 || children[0].Name != "notes" || !children[0].IsDir {
		t.Fatalf("children=%+v", children)
	}
}

func TestPathIndexRemoveNormalizesLeadingSlashAndPreservesOtherIdentity(t *testing.T) {
	index := NewPathIndex()
	index.Rebuild([]client.Memory{
		{MemoryID: "mem_a", Path: "/notes/a.md", LatestVersionID: "fmver_1"},
		{MemoryID: "mem_b", Path: "/notes/b.md", LatestVersionID: "fmver_2"},
	})

	index.Remove("/notes/a.md")

	if _, ok := index.Lookup("notes/a.md"); ok {
		t.Fatal("removed service path remains in relative index")
	}
	if node, ok := index.Lookup("notes/b.md"); !ok || node.MemoryID != "mem_b" || node.LatestVersionID != "fmver_2" {
		t.Fatalf("unrelated node=%+v ok=%v", node, ok)
	}
}

func TestPathIndexReplaceAtomicallyRebuildsRenameOverwriteState(t *testing.T) {
	index := NewPathIndex()
	index.Rebuild([]client.Memory{
		{MemoryID: "mem_a", Path: "a.md", LatestVersionID: "fmver_1"},
		{MemoryID: "mem_b", Path: "b.md", LatestVersionID: "fmver_2"},
		{MemoryID: "mem_c", Path: "nested/c.md", LatestVersionID: "fmver_3"},
	})
	index.AddDir("empty")

	index.Replace("a.md", client.Memory{MemoryID: "mem_a", Path: "b.md", LatestVersionID: "fmver_4"}, []string{"empty"})

	if _, ok := index.Lookup("a.md"); ok {
		t.Fatal("rename source remains in index")
	}
	if node, ok := index.Lookup("b.md"); !ok || node.MemoryID != "mem_a" || node.LatestVersionID != "fmver_4" {
		t.Fatalf("rename target=%+v ok=%v", node, ok)
	}
	if node, ok := index.Lookup("nested/c.md"); !ok || node.MemoryID != "mem_c" || node.LatestVersionID != "fmver_3" {
		t.Fatalf("unrelated node=%+v ok=%v", node, ok)
	}
	if node, ok := index.Lookup("empty"); !ok || !node.IsDir {
		t.Fatalf("local directory=%+v ok=%v", node, ok)
	}
}

func TestPathIndexReplaceRemovesEveryExistingPathForAuthoritativeIdentity(t *testing.T) {
	index := NewPathIndex()
	index.Rebuild([]client.Memory{
		{MemoryID: "mem_a", Path: "a.md", LatestVersionID: "opaque-old-a"},
		{MemoryID: "mem_a", Path: "stale/duplicate.md", LatestVersionID: "opaque-old-duplicate"},
		{MemoryID: "mem_other", Path: "other.md", LatestVersionID: "opaque-other"},
	})
	index.AddDir("local-empty")

	index.Replace("a.md", client.Memory{MemoryID: "mem_a", Path: "/authoritative.md", LatestVersionID: "opaque-new"}, []string{"local-empty"})

	for _, stalePath := range []string{"a.md", "stale/duplicate.md"} {
		if _, ok := index.Lookup(stalePath); ok {
			t.Fatalf("stale identity path %q remains after Replace", stalePath)
		}
	}
	if node, ok := index.Lookup("authoritative.md"); !ok || node.MemoryID != "mem_a" || node.LatestVersionID != "opaque-new" {
		t.Fatalf("authoritative node=%+v ok=%v", node, ok)
	}
	if node, ok := index.Lookup("other.md"); !ok || node.MemoryID != "mem_other" {
		t.Fatalf("unrelated node=%+v ok=%v", node, ok)
	}
	if node, ok := index.Lookup("local-empty"); !ok || !node.IsDir {
		t.Fatalf("local dir=%+v ok=%v", node, ok)
	}
}

func TestPathIndexCountsFilesOnly(t *testing.T) {
	index := NewPathIndex()
	if index.Count() != 0 {
		t.Fatalf("count=%d want 0", index.Count())
	}
	index.Rebuild([]client.Memory{{MemoryID: "m1", Path: "notes/daily/a.md"}, {MemoryID: "m2", Path: "notes/b.md"}})
	index.AddDir("empty")
	if index.Count() != 2 {
		t.Fatalf("count=%d want 2 (directories excluded)", index.Count())
	}
	index.Remove("notes/b.md")
	if index.Count() != 1 {
		t.Fatalf("count=%d want 1", index.Count())
	}
}

func TestPathIndexUsesOneNFCIdentityForCanonicalEquivalentPaths(t *testing.T) {
	index := NewPathIndex()
	index.Upsert(client.Memory{MemoryID: "mem_unicode", Path: "caf\u00e9.md", LatestVersionID: "fmver_1"})

	nfdNode, nfdOK := index.Lookup("cafe\u0301.md")
	nfcNode, nfcOK := index.Lookup("caf\u00e9.md")
	if !nfdOK || !nfcOK || nfdNode != nfcNode || nfcNode.Path != "caf\u00e9.md" {
		t.Fatalf("NFD node=%+v ok=%v NFC node=%+v ok=%v", nfdNode, nfdOK, nfcNode, nfcOK)
	}
	children := index.Children("")
	if len(children) != 1 || children[0].Name != "caf\u00e9.md" {
		t.Fatalf("children=%+v want one NFC entry", children)
	}
}

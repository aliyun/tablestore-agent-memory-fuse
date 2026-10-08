package fs

import (
	"path"
	"sort"
	"sync"
	"time"

	"agent-memory-fuse/internal/client"
)

type Node struct {
	Name            string
	Path            string
	IsDir           bool
	MemoryID        string
	LatestVersionID string
	Size            int64
	SHA256          string
	UpdatedAt       time.Time
}

type PathIndex struct {
	mu    sync.RWMutex
	nodes map[string]Node
}

func NewPathIndex() *PathIndex {
	return &PathIndex{nodes: map[string]Node{"": {Path: "", IsDir: true}}}
}

func (i *PathIndex) Rebuild(memories []client.Memory) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.nodes = map[string]Node{"": {Path: "", IsDir: true}}
	for _, memory := range memories {
		i.upsertLocked(memory)
	}
}

func (i *PathIndex) Upsert(memory client.Memory) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.upsertLocked(memory)
}

func (i *PathIndex) AddDir(value string) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.addDirLocked(value)
}

func (i *PathIndex) addDirLocked(value string) {
	value = canonicalPathKey(value)
	for value != "" && value != "." {
		i.nodes[value] = Node{Name: path.Base(value), Path: value, IsDir: true}
		value = path.Dir(value)
		if value == "." {
			value = ""
		}
	}
}

// Replace atomically removes the old path and every stale file node carrying
// the authoritative memory identity, applies the returned memory at its new
// path, rebuilds implicit directories, and restores filesystem-local dirs.
func (i *PathIndex) Replace(value string, memory client.Memory, localDirs []string) {
	i.mu.Lock()
	defer i.mu.Unlock()
	delete(i.nodes, canonicalPathKey(value))
	files := make([]Node, 0)
	for _, node := range i.nodes {
		if !node.IsDir && node.MemoryID != memory.MemoryID {
			files = append(files, node)
		}
	}
	i.nodes = map[string]Node{"": {Path: "", IsDir: true}}
	for _, node := range files {
		i.upsertLocked(client.Memory{
			MemoryID:         node.MemoryID,
			Path:             node.Path,
			ContentSizeBytes: node.Size,
			ContentSHA256:    node.SHA256,
			LatestVersionID:  node.LatestVersionID,
			UpdatedAt:        node.UpdatedAt,
		})
	}
	i.upsertLocked(memory)
	for _, dir := range localDirs {
		i.addDirLocked(dir)
	}
}

func (i *PathIndex) upsertLocked(memory client.Memory) {
	// 服务端返回规范化的 "/a/b.md"；索引统一用去前导斜杠的相对形态。
	value := canonicalPathKey(memory.Path)
	i.nodes[value] = Node{
		Name:            path.Base(value),
		Path:            value,
		MemoryID:        memory.MemoryID,
		LatestVersionID: memory.LatestVersionID,
		Size:            memory.ContentSizeBytes,
		SHA256:          memory.ContentSHA256,
		UpdatedAt:       memory.UpdatedAt,
	}
	dir := path.Dir(value)
	for dir != "." && dir != "/" && dir != "" {
		i.nodes[dir] = Node{Name: path.Base(dir), Path: dir, IsDir: true}
		dir = path.Dir(dir)
	}
}

// Count 返回文件节点数（不含目录），用于本地配额预检与 Statfs。
func (i *PathIndex) Count() int {
	i.mu.RLock()
	defer i.mu.RUnlock()
	count := 0
	for _, node := range i.nodes {
		if !node.IsDir {
			count++
		}
	}
	return count
}

func (i *PathIndex) Remove(value string) {
	i.mu.Lock()
	defer i.mu.Unlock()
	value = canonicalPathKey(value)
	delete(i.nodes, value)
	files := make([]Node, 0)
	for _, node := range i.nodes {
		if !node.IsDir {
			files = append(files, node)
		}
	}
	i.nodes = map[string]Node{"": {Path: "", IsDir: true}}
	for _, node := range files {
		i.upsertLocked(client.Memory{
			MemoryID:         node.MemoryID,
			Path:             node.Path,
			ContentSizeBytes: node.Size,
			ContentSHA256:    node.SHA256,
			LatestVersionID:  node.LatestVersionID,
			UpdatedAt:        node.UpdatedAt,
		})
	}
}

// RemoveIdentity atomically removes the addressed path and every stale file
// node carrying the same stable memory identity. This is used for an
// authoritative Delete after the item has moved across paths.
func (i *PathIndex) RemoveIdentity(value, memoryID string, localDirs []string) {
	i.mu.Lock()
	defer i.mu.Unlock()
	delete(i.nodes, canonicalPathKey(value))
	files := make([]Node, 0)
	for _, node := range i.nodes {
		if !node.IsDir && node.MemoryID != memoryID {
			files = append(files, node)
		}
	}
	i.nodes = map[string]Node{"": {Path: "", IsDir: true}}
	for _, node := range files {
		i.upsertLocked(client.Memory{
			MemoryID:         node.MemoryID,
			Path:             node.Path,
			ContentSizeBytes: node.Size,
			ContentSHA256:    node.SHA256,
			LatestVersionID:  node.LatestVersionID,
			UpdatedAt:        node.UpdatedAt,
		})
	}
	for _, dir := range localDirs {
		i.addDirLocked(dir)
	}
}

func (i *PathIndex) Lookup(value string) (Node, bool) {
	i.mu.RLock()
	defer i.mu.RUnlock()
	node, ok := i.nodes[canonicalPathKey(value)]
	return node, ok
}

func (i *PathIndex) Children(dir string) []Node {
	i.mu.RLock()
	defer i.mu.RUnlock()
	dir = canonicalPathKey(dir)
	out := make([]Node, 0)
	for value, node := range i.nodes {
		if value == "" || value == dir {
			continue
		}
		parent := path.Dir(value)
		if parent == "." {
			parent = ""
		}
		if parent == dir {
			out = append(out, node)
		}
	}
	sort.Slice(out, func(a, b int) bool { return out[a].Name < out[b].Name })
	return out
}

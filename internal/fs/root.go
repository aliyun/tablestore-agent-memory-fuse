package fs

import (
	"context"
	"io"
	"path"
	"sync"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

const renameNoReplace = 1

type dirNode struct {
	fs.Inode
	filesystem *FileSystem
	path       string
}

type fileNode struct {
	fs.Inode
	filesystem *FileSystem
	pathMu     sync.RWMutex
	path       string
}

type fileHandle struct {
	filesystem *FileSystem
	handle     *Handle
	flags      uint32
}

func NewRoot(filesystem *FileSystem) fs.InodeEmbedder {
	return &dirNode{filesystem: filesystem}
}

func (d *dirNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	value := joinPath(d.path, name)
	node, errno := d.filesystem.Lookup(ctx, value)
	if errno != 0 {
		return nil, errno
	}
	fillEntry(out, node, d.filesystem.opts.MetadataTTL, d.filesystem.readOnly())
	if node.IsDir {
		child := &dirNode{filesystem: d.filesystem, path: value}
		return d.NewInode(ctx, child, fs.StableAttr{Mode: fuse.S_IFDIR}), 0
	}
	child := &fileNode{filesystem: d.filesystem, path: value}
	return d.NewInode(ctx, child, fs.StableAttr{Mode: fuse.S_IFREG}), 0
}

func (d *dirNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	if err := d.filesystem.EnsureFresh(ctx); err != nil {
		return nil, errnoFor(err)
	}
	children := d.filesystem.index.Children(d.path)
	entries := make([]fuse.DirEntry, 0, len(children))
	for _, child := range children {
		mode := uint32(fuse.S_IFREG)
		if child.IsDir {
			mode = fuse.S_IFDIR
		}
		entries = append(entries, fuse.DirEntry{Name: child.Name, Mode: mode})
	}
	return fs.NewListDirStream(entries), 0
}

func (d *dirNode) Create(ctx context.Context, name string, flags uint32, _ uint32, out *fuse.EntryOut) (*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	value := joinPath(d.path, name)
	handle, errno := d.filesystem.Create(value)
	if errno != 0 {
		return nil, nil, 0, errno
	}
	node := &fileNode{filesystem: d.filesystem, path: value}
	inode := d.NewInode(ctx, node, fs.StableAttr{Mode: fuse.S_IFREG})
	fillEntry(out, Node{Name: name, Path: value}, d.filesystem.opts.MetadataTTL, d.filesystem.readOnly())
	return inode, &fileHandle{filesystem: d.filesystem, handle: handle, flags: flags}, fuse.FOPEN_DIRECT_IO, 0
}

func (d *dirNode) Mkdir(ctx context.Context, name string, _ uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	value := joinPath(d.path, name)
	if errno := d.filesystem.Mkdir(value); errno != 0 {
		return nil, errno
	}
	node := &dirNode{filesystem: d.filesystem, path: value}
	inode := d.NewInode(ctx, node, fs.StableAttr{Mode: fuse.S_IFDIR})
	fillEntry(out, Node{Name: name, Path: value, IsDir: true}, d.filesystem.opts.MetadataTTL, d.filesystem.readOnly())
	return inode, 0
}

func (d *dirNode) Unlink(ctx context.Context, name string) syscall.Errno {
	return d.filesystem.Delete(ctx, joinPath(d.path, name))
}
func (d *dirNode) Rmdir(_ context.Context, name string) syscall.Errno {
	return d.filesystem.Rmdir(joinPath(d.path, name))
}

func (d *dirNode) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	parent, ok := newParent.(*dirNode)
	if !ok {
		return syscall.EXDEV
	}
	from, to := joinPath(d.path, name), joinPath(parent.path, newName)
	child := d.GetChild(name)
	if errno := d.filesystem.Rename(ctx, from, to, flags&renameNoReplace == 0); errno != 0 {
		return errno
	}
	if child != nil {
		if file, ok := child.Operations().(*fileNode); ok {
			file.setPath(to)
		}
	}
	return 0
}

func (d *dirNode) Getattr(_ context.Context, _ fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Attr.Mode = fuse.S_IFDIR | 0o755
	if d.filesystem.readOnly() {
		out.Attr.Mode = fuse.S_IFDIR | 0o555
	}
	out.Attr.Nlink = 2
	out.SetTimeout(d.filesystem.opts.MetadataTTL)
	return 0
}

func (d *dirNode) Statfs(_ context.Context, out *fuse.StatfsOut) syscall.Errno {
	out.Bsize, out.Frsize, out.Blocks, out.Bfree, out.Bavail = 4096, 4096, 50000, 50000, 50000
	out.Files, out.NameLen = MaxFiles, 4096
	if count := d.filesystem.index.Count(); count < MaxFiles {
		out.Ffree = MaxFiles - uint64(count)
	} else {
		out.Ffree = 0
	}
	return 0
}

func (n *fileNode) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	if n.filesystem.readOnly() && flags&(syscall.O_WRONLY|syscall.O_RDWR|syscall.O_TRUNC) != 0 {
		return nil, 0, syscall.EROFS
	}
	handle, errno := n.filesystem.Open(ctx, n.currentPath())
	if errno != 0 {
		return nil, 0, errno
	}
	if flags&syscall.O_TRUNC != 0 {
		if err := handle.Truncate(0); err != nil {
			return nil, 0, errnoFor(err)
		}
	}
	return &fileHandle{filesystem: n.filesystem, handle: handle, flags: flags}, fuse.FOPEN_DIRECT_IO, 0
}

func (n *fileNode) Getattr(ctx context.Context, _ fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	node, errno := n.filesystem.Stat(ctx, n.currentPath())
	if errno != 0 {
		return errno
	}
	fillAttr(&out.Attr, node, n.filesystem.readOnly())
	out.SetTimeout(n.filesystem.opts.MetadataTTL)
	return 0
}

func (n *fileNode) Setattr(ctx context.Context, file fs.FileHandle, input *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	if n.filesystem.readOnly() {
		return syscall.EROFS
	}
	size, ok := input.GetSize()
	if !ok {
		// 不带 size 的 setattr（touch 的 utimens、chmod 等）：内容无变化，
		// 时间权威在服务端 createdAt/updatedAt，客户端作 no-op 成功返回。
		if handle, ok := file.(*fileHandle); ok {
			fillHandleAttr(out, handle.handle, n.filesystem.opts.MetadataTTL, n.filesystem.readOnly())
			return 0
		}
		return n.Getattr(ctx, file, out)
	}
	if handle, ok := file.(*fileHandle); ok {
		if err := handle.handle.Truncate(int64(size)); err != nil {
			return errnoFor(err)
		}
		fillHandleAttr(out, handle.handle, n.filesystem.opts.MetadataTTL, n.filesystem.readOnly())
		return 0
	}
	handle, errno := n.filesystem.Open(ctx, n.currentPath())
	if errno != 0 {
		return errno
	}
	if err := handle.Truncate(int64(size)); err != nil {
		return errnoFor(err)
	}
	if errno := n.filesystem.Commit(ctx, handle); errno != 0 {
		return errno
	}
	fillHandleAttr(out, handle, n.filesystem.opts.MetadataTTL, n.filesystem.readOnly())
	return 0
}

func (f *fileHandle) Read(_ context.Context, destination []byte, offset int64) (fuse.ReadResult, syscall.Errno) {
	n, err := f.handle.ReadAt(destination, offset)
	if err != nil && !errorsIsEOF(err) {
		return nil, syscall.EIO
	}
	return fuse.ReadResultData(destination[:n]), 0
}

func (f *fileHandle) Write(_ context.Context, data []byte, offset int64) (uint32, syscall.Errno) {
	if f.filesystem.readOnly() {
		return 0, syscall.EROFS
	}
	if f.flags&syscall.O_APPEND != 0 {
		n, err := f.handle.Append(data)
		return uint32(n), errnoFor(err)
	}
	n, err := f.handle.WriteAt(data, offset)
	return uint32(n), errnoFor(err)
}

func (f *fileHandle) Flush(ctx context.Context) syscall.Errno {
	return f.filesystem.Commit(ctx, f.handle)
}
func (f *fileHandle) Fsync(ctx context.Context, _ uint32) syscall.Errno {
	return f.filesystem.Commit(ctx, f.handle)
}
func (f *fileHandle) Release(_ context.Context) syscall.Errno {
	// The kernel ignores Release errors, so Release must not submit. Before
	// Release, an explicit Fsync or Flush from a still-live dup fd can retry a
	// dirty handle; retrying here could silently commit after close failed.
	f.filesystem.AbandonCreated(f.handle)
	return 0
}
func (f *fileHandle) Getattr(_ context.Context, out *fuse.AttrOut) syscall.Errno {
	fillHandleAttr(out, f.handle, f.filesystem.opts.MetadataTTL, f.filesystem.readOnly())
	return 0
}

func (n *fileNode) currentPath() string {
	n.pathMu.RLock()
	defer n.pathMu.RUnlock()
	return n.path
}

func (n *fileNode) setPath(value string) {
	n.pathMu.Lock()
	n.path = value
	n.pathMu.Unlock()
}

func fillEntry(out *fuse.EntryOut, node Node, ttl time.Duration, readOnly bool) {
	fillAttr(&out.Attr, node, readOnly)
	out.SetEntryTimeout(ttl)
	out.SetAttrTimeout(ttl)
}
func fillAttr(out *fuse.Attr, node Node, readOnly bool) {
	if node.IsDir {
		out.Mode = fuse.S_IFDIR | 0o755
		out.Nlink = 2
	} else {
		out.Mode = fuse.S_IFREG | 0o644
		out.Nlink = 1
		out.Size = uint64(node.Size)
	}
	if readOnly {
		out.Mode &^= 0o222
	}
	if !node.UpdatedAt.IsZero() {
		out.Mtime = uint64(node.UpdatedAt.Unix())
		out.Ctime = out.Mtime
	}
}
func fillHandleAttr(out *fuse.AttrOut, handle *Handle, ttl time.Duration, readOnly bool) {
	memory := handle.Memory()
	fillAttr(&out.Attr, Node{Path: memory.Path, Size: int64(len(handle.Bytes())), UpdatedAt: memory.UpdatedAt}, readOnly)
	out.SetTimeout(ttl)
}
func errorsIsEOF(err error) bool { return err == io.EOF }

var _ fs.NodeLookuper = (*dirNode)(nil)
var _ fs.NodeReaddirer = (*dirNode)(nil)
var _ fs.NodeCreater = (*dirNode)(nil)
var _ fs.NodeRenamer = (*dirNode)(nil)
var _ fs.NodeUnlinker = (*dirNode)(nil)
var _ fs.NodeRmdirer = (*dirNode)(nil)
var _ fs.NodeMkdirer = (*dirNode)(nil)
var _ fs.NodeOpener = (*fileNode)(nil)
var _ fs.NodeSetattrer = (*fileNode)(nil)
var _ fs.FileReader = (*fileHandle)(nil)
var _ fs.FileWriter = (*fileHandle)(nil)
var _ fs.FileFlusher = (*fileHandle)(nil)
var _ fs.FileFsyncer = (*fileHandle)(nil)
var _ fs.FileReleaser = (*fileHandle)(nil)

var _ = path.Clean

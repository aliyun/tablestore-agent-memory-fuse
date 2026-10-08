//go:build linux

// fusemnt 在无 fusermount 的容器里代做 FUSE mount(2)：打开 /dev/fuse、
// 挂载到目标目录，然后把 fd 以 /dev/fd/3 传给子进程（go-fuse 的魔法挂载点，
// 见 fuse.NewServer 文档）。需在拥有挂载点 mount namespace 的 user namespace
// 内以 mapped root 运行（如 unshare -U -r -m）。
//
// 用法: fusemnt <mountpoint> <cmd> [args...]   # 自动在末尾追加 /dev/fd/3
package main

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"syscall"
)

func main() {
	if len(os.Args) < 3 {
		log.Fatal("usage: fusemnt <mountpoint> <cmd> [args...]")
	}
	target := os.Args[1]
	f, err := os.OpenFile("/dev/fuse", os.O_RDWR, 0)
	if err != nil {
		log.Fatalf("open /dev/fuse: %v", err)
	}
	var st syscall.Stat_t
	if err := syscall.Stat(target, &st); err != nil {
		log.Fatalf("stat %s: %v", target, err)
	}
	// 与 go-fuse mountDirect 相同的选项；fd 号取挂载时本进程的真实值。
	opts := fmt.Sprintf("fd=%d,rootmode=%o,user_id=%d,group_id=%d,max_read=131072",
		f.Fd(), st.Mode&syscall.S_IFMT, os.Geteuid(), os.Getegid())
	if err := syscall.Mount("file-memory", target, "fuse.file-memory",
		syscall.MS_NOSUID|syscall.MS_NODEV, opts); err != nil {
		log.Fatalf("mount(2) on %s: %v", target, err)
	}
	log.Printf("fusemnt: %s mounted, spawning %v with /dev/fd/3", target, os.Args[2:])
	cmd := exec.Command(os.Args[2], append(os.Args[3:], "/dev/fd/3")...)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, os.Stdout, os.Stderr
	cmd.ExtraFiles = []*os.File{f} // 子进程 fd 3
	if err := cmd.Run(); err != nil {
		log.Fatalf("child exited: %v", err)
	}
}

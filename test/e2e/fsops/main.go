// fsops 是文件系统语义探针：逐系统调用执行并打印各步骤的 errno 名，
// 避免 shell 重定向吞掉 close(2)（即 agent-memory-fuse flush 提交）阶段的错误。
// 每步输出 "step=RESULT"，RESULT 为 OK 或 errno 名（EFBIG、ESTALE...）。
package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"
)

var errnoNames = map[syscall.Errno]string{
	syscall.EPERM: "EPERM", syscall.ENOENT: "ENOENT", syscall.EIO: "EIO",
	syscall.EACCES: "EACCES", syscall.EEXIST: "EEXIST", syscall.ENOTDIR: "ENOTDIR",
	syscall.EISDIR: "EISDIR", syscall.EINVAL: "EINVAL", syscall.EFBIG: "EFBIG",
	syscall.ENOSPC: "ENOSPC", syscall.EROFS: "EROFS", syscall.EAGAIN: "EAGAIN",
	syscall.ENOTEMPTY: "ENOTEMPTY", syscall.EILSEQ: "EILSEQ", syscall.ESTALE: "ESTALE",
	syscall.ENOTSUP: "ENOTSUP", syscall.ENODATA: "ENODATA", syscall.ENAMETOOLONG: "ENAMETOOLONG",
}

func name(err error) string {
	if err == nil {
		return "OK"
	}
	var errno syscall.Errno
	if errors.As(err, &errno) {
		if n, ok := errnoNames[errno]; ok {
			return n
		}
		return fmt.Sprintf("ERRNO_%d", int(errno))
	}
	return "ERR:" + err.Error()
}

// content 语法：size:N（N 字节 'a'）、badutf8、utf8:任意字符串、@文件路径
func decodeContent(spec string) []byte {
	switch {
	case strings.HasPrefix(spec, "size:"):
		n, _ := strconv.Atoi(spec[5:])
		return []byte(strings.Repeat("a", n))
	case spec == "badutf8":
		return []byte{'h', 'i', 0xff, 0xfe, '!'}
	case strings.HasPrefix(spec, "utf8:"):
		return []byte(spec[5:])
	case strings.HasPrefix(spec, "@"):
		data, err := os.ReadFile(spec[1:])
		if err != nil {
			panic(err)
		}
		return data
	default:
		return []byte(spec)
	}
}

func main() {
	op, args := os.Args[1], os.Args[2:]
	switch op {
	case "write": // fsops write <path> <contentSpec> [sleep-ms-before-close]
		fd, err := os.OpenFile(args[0], os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
		fmt.Printf("open=%s ", name(err))
		if err != nil {
			fmt.Println()
			os.Exit(1)
		}
		_, err = fd.Write(decodeContent(args[1]))
		fmt.Printf("write=%s ", name(err))
		if len(args) > 2 {
			ms, _ := strconv.Atoi(args[2])
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}
		fmt.Printf("close=%s\n", name(fd.Close()))
	case "write-excl": // fsops write-excl <path> <contentSpec> [sleep-ms-before-close]
		fd, err := os.OpenFile(args[0], os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
		fmt.Printf("open=%s ", name(err))
		if err != nil {
			fmt.Println()
			os.Exit(1)
		}
		_, err = fd.Write(decodeContent(args[1]))
		fmt.Printf("write=%s ", name(err))
		if len(args) > 2 {
			ms, _ := strconv.Atoi(args[2])
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}
		fmt.Printf("close=%s\n", name(fd.Close()))
	case "read": // 打印 read=OK content=<内容>（内容经 %q）
		data, err := os.ReadFile(args[0])
		fmt.Printf("read=%s content=%q\n", name(err), string(data))
	case "stat":
		st, err := os.Stat(args[0])
		if err != nil {
			fmt.Printf("stat=%s\n", name(err))
			return
		}
		fmt.Printf("stat=OK size=%d mode=%s mtime=%d\n", st.Size(), st.Mode(), st.ModTime().Unix())
	case "ls":
		entries, err := os.ReadDir(args[0])
		fmt.Printf("ls=%s n=%d [", name(err), len(entries))
		for i, e := range entries {
			if i > 0 {
				fmt.Print(" ")
			}
			suffix := ""
			if e.IsDir() {
				suffix = "/"
			}
			fmt.Print(e.Name() + suffix)
		}
		fmt.Println("]")
	case "rename":
		fmt.Printf("rename=%s\n", name(os.Rename(args[0], args[1])))
	case "rm":
		fmt.Printf("rm=%s\n", name(os.Remove(args[0])))
	case "mkdir":
		fmt.Printf("mkdir=%s\n", name(os.Mkdir(args[0], 0755)))
	case "truncate":
		n, _ := strconv.Atoi(args[1])
		fmt.Printf("truncate=%s\n", name(os.Truncate(args[0], int64(n))))
	case "touch":
		nowTime := time.Now()
		fmt.Printf("touch=%s\n", name(os.Chtimes(args[0], nowTime, nowTime)))
	case "append": // 读改写路径：O_WRONLY|O_APPEND
		fd, err := os.OpenFile(args[0], os.O_WRONLY|os.O_APPEND, 0644)
		fmt.Printf("open=%s ", name(err))
		if err != nil {
			fmt.Println()
			os.Exit(1)
		}
		_, err = fd.Write(decodeContent(args[1]))
		fmt.Printf("write=%s close=%s\n", name(err), name(fd.Close()))
	case "stale": // 制造 ESTALE：打开+写入后暂停，等外部改服务端状态再 close
		// 用法: fsops stale <path> <content> <控制命令写入的通知文件>
		fd, err := os.OpenFile(args[0], os.O_WRONLY|os.O_TRUNC, 0644)
		fmt.Printf("open=%s ", name(err))
		if err != nil {
			fmt.Println()
			os.Exit(1)
		}
		_, err = fd.Write(decodeContent(args[1]))
		fmt.Printf("write=%s ", name(err))
		for i := 0; i < 100; i++ { // 最多等 10s，通知文件出现即继续
			if _, err := os.Stat(args[2]); err == nil {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		fmt.Printf("close=%s\n", name(fd.Close()))
	case "copyout": // 验证大文件读：读全文写到本地路径
		data, err := os.ReadFile(args[0])
		fmt.Printf("read=%s size=%d ", name(err), len(data))
		if err == nil {
			err = os.WriteFile(args[1], data, 0644)
		}
		fmt.Printf("out=%s\n", name(err))
	case "fsync":
		fd, err := os.OpenFile(args[0], os.O_WRONLY, 0644)
		fmt.Printf("open=%s ", name(err))
		if err != nil {
			fmt.Println()
			os.Exit(1)
		}
		_, err = fd.Write(decodeContent(args[1]))
		fmt.Printf("write=%s ", name(err))
		fmt.Printf("fsync=%s ", name(fd.Sync()))
		fmt.Printf("close=%s\n", name(fd.Close()))
	default:
		fmt.Fprintln(os.Stderr, "unknown op "+op)
		os.Exit(2)
	}
	_ = io.Discard
}

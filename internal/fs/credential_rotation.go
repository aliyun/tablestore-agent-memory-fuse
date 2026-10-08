package fs

import (
	"context"
	"errors"
	"syscall"

	"agent-memory-fuse/internal/client"
	"agent-memory-fuse/internal/credential"
)

type credentialRotator interface {
	UpdateSTSCredentials(credential.STS) error
}

// Setxattr 把挂载根上的私有 xattr 当作一次性控制消息。数据只在本次
// FUSE 请求中传递，不进入 path index、远端存储或可读取的 xattr 集合。
func (d *dirNode) Setxattr(_ context.Context, attr string, data []byte, _ uint32) syscall.Errno {
	if d.path != "" || attr != credential.RotationXattr {
		return syscall.ENOTSUP
	}
	credentials, err := credential.ParseSTS(data)
	if err != nil {
		return syscall.EINVAL
	}
	rotator, ok := d.filesystem.remote.(credentialRotator)
	if !ok {
		return syscall.EOPNOTSUPP
	}
	if err := rotator.UpdateSTSCredentials(credentials); err != nil {
		if errors.Is(err, client.ErrSTSRotationUnsupported) {
			return syscall.EOPNOTSUPP
		}
		return syscall.EIO
	}
	return 0
}

func (d *dirNode) Getxattr(_ context.Context, _ string, _ []byte) (uint32, syscall.Errno) {
	return 0, syscall.ENODATA
}

func (d *dirNode) Listxattr(_ context.Context, _ []byte) (uint32, syscall.Errno) {
	return 0, 0
}

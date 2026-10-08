//go:build linux || darwin

package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"syscall"
	"time"

	"agent-memory-fuse/internal/credential"

	"golang.org/x/sys/unix"
)

func openCredentialFile(path string) (*os.File, error) {
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if err != nil {
		return nil, fmt.Errorf("credential path must be a directly referenced regular file: %w", err)
	}
	file := os.NewFile(uintptr(fd), path)
	if file == nil {
		_ = unix.Close(fd)
		return nil, errors.New("credential file is unavailable")
	}
	closeOnError := true
	defer func() {
		if closeOnError {
			_ = file.Close()
		}
	}()

	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("inspect credential file: %w", err)
	}
	if !info.Mode().IsRegular() {
		return nil, errors.New("credential path must be a regular file")
	}
	if info.Mode().Perm()&0o077 != 0 {
		return nil, errors.New("credential file must not be readable, writable, or executable by group or others")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || int(stat.Uid) != os.Geteuid() {
		return nil, errors.New("credential file must be owned by the current effective user")
	}
	if err := unix.SetNonblock(fd, false); err != nil {
		return nil, fmt.Errorf("prepare credential file: %w", err)
	}

	closeOnError = false
	return file, nil
}

func readCredentialPayloadWithTimeout(input io.Reader, timeout time.Duration) ([]byte, error) {
	file, ok := input.(*os.File)
	if !ok {
		return readCredentialPayload(input)
	}
	deadline := time.Now().Add(timeout)
	payload := make([]byte, 0, 1024)
	buffer := make([]byte, 4096)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil, errors.New("timed out waiting for a complete credential on stdin")
		}
		milliseconds := int((remaining + time.Millisecond - 1) / time.Millisecond)
		pollFDs := []unix.PollFd{{Fd: int32(file.Fd()), Events: unix.POLLIN | unix.POLLHUP}}
		ready, err := unix.Poll(pollFDs, milliseconds)
		if err == unix.EINTR {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("wait for credential stdin: %w", err)
		}
		if ready == 0 {
			return nil, errors.New("timed out waiting for a complete credential on stdin")
		}
		if pollFDs[0].Revents&(unix.POLLERR|unix.POLLNVAL) != 0 {
			return nil, errors.New("credential stdin became unavailable")
		}

		count, readErr := file.Read(buffer)
		if count > 0 {
			payload = append(payload, buffer[:count]...)
			if len(payload) > credential.MaxRotationPayloadBytes {
				return nil, errors.New("credential payload exceeds 64 KiB")
			}
		}
		if readErr == io.EOF {
			return payload, nil
		}
		if readErr != nil {
			return nil, fmt.Errorf("read credential payload: %w", readErr)
		}
		if count == 0 && pollFDs[0].Revents&unix.POLLHUP != 0 {
			return payload, nil
		}
	}
}

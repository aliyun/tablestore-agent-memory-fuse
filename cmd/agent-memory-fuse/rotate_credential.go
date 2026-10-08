package main

import (
	"errors"
	"flag"
	"fmt"
	"io"

	"agent-memory-fuse/internal/credential"

	"golang.org/x/sys/unix"
)

type setxattrFunc func(path, attr string, data []byte, flags int) error

func parseRotateCredentialArgs(args []string) (string, error) {
	flags := flag.NewFlagSet("agent-memory-fuse rotate-credential", flag.ContinueOnError)
	var mountpoint string
	flags.StringVar(&mountpoint, "mountpoint", "", "existing agent-memory-fuse mountpoint")
	if err := flags.Parse(args); err != nil {
		return "", err
	}
	if mountpoint == "" || flags.NArg() != 0 {
		return "", errors.New("usage: agent-memory-fuse rotate-credential --mountpoint MOUNTPOINT")
	}
	return mountpoint, nil
}

func runRotateCredential(args []string, input io.Reader, setxattr setxattrFunc) error {
	mountpoint, err := parseRotateCredentialArgs(args)
	if err != nil {
		return err
	}
	credentials, err := credential.DecodeSTS(input)
	if err != nil {
		return fmt.Errorf("read new STS credential from stdin: %w", err)
	}
	payload, err := credential.EncodeSTS(credentials)
	if err != nil {
		return fmt.Errorf("encode new STS credential: %w", err)
	}
	if err := setxattr(mountpoint, credential.RotationXattr, payload, 0); err != nil {
		return fmt.Errorf("rotate STS credential through mountpoint: %w", err)
	}
	return nil
}

func systemSetxattr(path, attr string, data []byte, flags int) error {
	return unix.Setxattr(path, attr, data, flags)
}

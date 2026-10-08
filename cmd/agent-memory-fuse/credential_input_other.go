//go:build !linux && !darwin

package main

import (
	"errors"
	"io"
	"os"
	"time"
)

func openCredentialFile(string) (*os.File, error) {
	return nil, errors.New("credential files are supported only on Linux and macOS")
}

func readCredentialPayloadWithTimeout(io.Reader, time.Duration) ([]byte, error) {
	return nil, errors.New("bounded credential stdin is supported only on Linux and macOS")
}

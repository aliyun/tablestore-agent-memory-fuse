package fs

import (
	"context"
	"fmt"
	"syscall"
	"testing"
)

func TestErrnoForFUSEInterruptIsEINTR(t *testing.T) {
	err := fmt.Errorf("remote read stopped: %w", context.Canceled)
	if got := errnoFor(err); got != syscall.EINTR {
		t.Fatalf("errno=%v want EINTR", got)
	}
}

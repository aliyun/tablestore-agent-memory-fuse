package fs

import (
	"context"
	"errors"
	"syscall"
	"testing"

	"agent-memory-fuse/internal/client"
	"agent-memory-fuse/internal/credential"
)

type rotatingRemote struct {
	*fakeRemote
	credentials credential.STS
	err         error
	calls       int
}

func (r *rotatingRemote) UpdateSTSCredentials(credentials credential.STS) error {
	r.calls++
	r.credentials = credentials
	return r.err
}

func TestRootCredentialRotationXattr(t *testing.T) {
	remote := &rotatingRemote{fakeRemote: &fakeRemote{memories: map[string]client.Memory{}}}
	filesystem := NewFileSystem(remote, Options{ReadOnly: true})
	root := &dirNode{filesystem: filesystem}
	payload := []byte(`{"accessKeyId":"new-ak","accessKeySecret":"new-sk","stsToken":"new-token"}`)
	if errno := root.Setxattr(context.Background(), credential.RotationXattr, payload, 0); errno != 0 {
		t.Fatalf("errno=%v", errno)
	}
	want := credential.STS{AccessKeyID: "new-ak", AccessKeySecret: "new-sk", Token: "new-token"}
	if remote.calls != 1 || remote.credentials != want {
		t.Fatalf("calls=%d credentials=%+v want=%+v", remote.calls, remote.credentials, want)
	}
}

func TestCredentialRotationXattrRejectsInvalidRequests(t *testing.T) {
	remote := &rotatingRemote{fakeRemote: &fakeRemote{memories: map[string]client.Memory{}}}
	filesystem := NewFileSystem(remote, Options{})
	tests := []struct {
		name string
		node *dirNode
		attr string
		data []byte
		want syscall.Errno
	}{
		{"unknown attr", &dirNode{filesystem: filesystem}, "user.other", []byte(`{}`), syscall.ENOTSUP},
		{"subdirectory", &dirNode{filesystem: filesystem, path: "child"}, credential.RotationXattr, []byte(`{}`), syscall.ENOTSUP},
		{"malformed", &dirNode{filesystem: filesystem}, credential.RotationXattr, []byte(`{`), syscall.EINVAL},
		{"oversized", &dirNode{filesystem: filesystem}, credential.RotationXattr, make([]byte, credential.MaxRotationPayloadBytes+1), syscall.EINVAL},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := test.node.Setxattr(context.Background(), test.attr, test.data, 0); got != test.want {
				t.Fatalf("errno=%v want %v", got, test.want)
			}
		})
	}
	if remote.calls != 0 {
		t.Fatalf("unexpected rotation calls=%d", remote.calls)
	}
}

func TestCredentialRotationXattrMapsCapabilityErrors(t *testing.T) {
	payload := []byte(`{"accessKeyId":"new-ak","accessKeySecret":"new-sk","stsToken":"new-token"}`)
	nonRotating := NewFileSystem(&fakeRemote{memories: map[string]client.Memory{}}, Options{})
	if got := (&dirNode{filesystem: nonRotating}).Setxattr(context.Background(), credential.RotationXattr, payload, 0); got != syscall.EOPNOTSUPP {
		t.Fatalf("non-rotating errno=%v", got)
	}

	unsupported := &rotatingRemote{fakeRemote: &fakeRemote{memories: map[string]client.Memory{}}, err: client.ErrSTSRotationUnsupported}
	if got := (&dirNode{filesystem: NewFileSystem(unsupported, Options{})}).Setxattr(context.Background(), credential.RotationXattr, payload, 0); got != syscall.EOPNOTSUPP {
		t.Fatalf("unsupported errno=%v", got)
	}

	failed := &rotatingRemote{fakeRemote: &fakeRemote{memories: map[string]client.Memory{}}, err: errors.New("client construction failed")}
	if got := (&dirNode{filesystem: NewFileSystem(failed, Options{})}).Setxattr(context.Background(), credential.RotationXattr, payload, 0); got != syscall.EIO {
		t.Fatalf("failed errno=%v", got)
	}
}

func TestCredentialRotationXattrCannotBeReadOrListed(t *testing.T) {
	root := &dirNode{filesystem: NewFileSystem(&fakeRemote{memories: map[string]client.Memory{}}, Options{})}
	if size, errno := root.Getxattr(context.Background(), credential.RotationXattr, make([]byte, 1024)); size != 0 || errno != syscall.ENODATA {
		t.Fatalf("getxattr size=%d errno=%v", size, errno)
	}
	if size, errno := root.Listxattr(context.Background(), make([]byte, 1024)); size != 0 || errno != 0 {
		t.Fatalf("listxattr size=%d errno=%v", size, errno)
	}
}

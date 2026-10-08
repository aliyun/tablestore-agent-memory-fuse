package main

import (
	"errors"
	"strings"
	"testing"

	"agent-memory-fuse/internal/credential"
)

func TestRunRotateCredentialReadsStdinAndSetsControlXattr(t *testing.T) {
	var gotPath, gotAttr string
	var gotData []byte
	setxattr := func(path, attr string, data []byte, flags int) error {
		gotPath, gotAttr, gotData = path, attr, append([]byte(nil), data...)
		if flags != 0 {
			t.Fatalf("flags=%d", flags)
		}
		return nil
	}
	input := strings.NewReader(`{
		"accessKeyId":"new-ak",
		"accessKeySecret":"new-sk",
		"stsToken":"new-token"
	}`)
	if err := runRotateCredential([]string{"--mountpoint", "/data/memory"}, input, setxattr); err != nil {
		t.Fatal(err)
	}
	if gotPath != "/data/memory" || gotAttr != credential.RotationXattr {
		t.Fatalf("path=%q attr=%q", gotPath, gotAttr)
	}
	want := `{"accessKeyId":"new-ak","accessKeySecret":"new-sk","stsToken":"new-token"}`
	if string(gotData) != want {
		t.Fatalf("payload=%q want %q", gotData, want)
	}
}

func TestRunRotateCredentialRejectsBadInputWithoutCallingSetxattr(t *testing.T) {
	called := false
	err := runRotateCredential([]string{"--mountpoint", "/data/memory"}, strings.NewReader(`{"accessKeyId":"ak"}`), func(string, string, []byte, int) error {
		called = true
		return nil
	})
	if err == nil || called {
		t.Fatalf("err=%v called=%v", err, called)
	}
}

func TestRunRotateCredentialErrorsDoNotLeakCredentials(t *testing.T) {
	const input = `{"accessKeyId":"secret-ak","accessKeySecret":"secret-sk","stsToken":"secret-token"}`
	err := runRotateCredential([]string{"--mountpoint", "/data/memory"}, strings.NewReader(input), func(string, string, []byte, int) error {
		return errors.New("xattr rejected")
	})
	if err == nil {
		t.Fatal("expected error")
	}
	for _, secret := range []string{"secret-ak", "secret-sk", "secret-token"} {
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("error leaked %q: %v", secret, err)
		}
	}
}

func TestParseRotateCredentialArgsRequiresMountpoint(t *testing.T) {
	for _, args := range [][]string{nil, {"--mountpoint", ""}, {"--mountpoint", "/m", "extra"}} {
		if _, err := parseRotateCredentialArgs(args); err == nil {
			t.Fatalf("args=%v: expected error", args)
		}
	}
}

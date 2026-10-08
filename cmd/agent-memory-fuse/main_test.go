package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"agent-memory-fuse/internal/client"
	memoryfs "agent-memory-fuse/internal/fs"

	"github.com/hanwen/go-fuse/v2/fuse"
)

func validMountArgs(credentialArgs ...string) []string {
	args := []string{"--endpoint", "https://example", "--instance", "inst", "--region", "cn-hangzhou", "--store", "store", "--scope", "app#tenant#agent#run", "--session", "session"}
	args = append(args, credentialArgs...)
	return append(args, "/mnt/memory")
}

func TestParseMountArgs(t *testing.T) {
	args := []string{"--endpoint", "https://example", "--instance", "inst", "--credential-stdin", "--store", "store", "--scope", "app#tenant#agent#run", "--session", "session", "/mnt/memory"}
	config, mountpoint, err := parseMountArgsWithInput(args, strings.NewReader("apikey:key"))
	if err != nil {
		t.Fatal(err)
	}
	if mountpoint != "/mnt/memory" || config.APIKey != "key" || config.Scope.TenantID != "tenant" {
		t.Fatalf("config=%+v mount=%q", config, mountpoint)
	}

	config, _, err = parseMountArgsWithInput(validMountArgs("--credential-stdin"), strings.NewReader("aksk:access:secret"))
	if err != nil || config.AccessKeyID != "access" || config.AccessKeySecret != "secret" {
		t.Fatalf("config=%+v err=%v", config, err)
	}

	// STS token 是不透明末段,自身可以含冒号。
	config, _, err = parseMountArgsWithInput(validMountArgs("--credential-stdin"), strings.NewReader("sts:access:secret:tok:en="))
	if err != nil || config.AccessKeyID != "access" || config.AccessKeySecret != "secret" || config.STSToken != "tok:en=" {
		t.Fatalf("config=%+v err=%v", config, err)
	}
}

func TestParseMountArgsDefaultsOmittedScope(t *testing.T) {
	args := []string{"--endpoint", "https://example", "--instance", "inst", "--credential-stdin", "--store", "store", "--session", "session", "/mnt/memory"}
	config, _, err := parseMountArgsWithInput(args, strings.NewReader("apikey:key"))
	if err != nil {
		t.Fatal(err)
	}
	want := client.Scope{AppID: "__default__", TenantID: "__default__", AgentID: "__default__", RunID: "__default__"}
	if config.Scope != want {
		t.Fatalf("scope=%+v want %+v", config.Scope, want)
	}
}

func TestParseMountArgsMutationTimeoutDefault(t *testing.T) {
	config, _, err := parseMountArgsWithInput(validMountArgs("--credential-stdin"), strings.NewReader("apikey:key"))
	if err != nil {
		t.Fatal(err)
	}
	if got := config.MutationTimeout; got != time.Minute {
		t.Fatalf("MutationTimeout=%v want %v", got, time.Minute)
	}
}

func TestParseMountArgsCredentialStdinTimeoutDefaultAndOverride(t *testing.T) {
	defaults, _, err := parseMountArgsWithInput(validMountArgs("--credential-stdin"), strings.NewReader("apikey:key"))
	if err != nil {
		t.Fatal(err)
	}
	if defaults.CredentialStdinTimeout != 30*time.Second {
		t.Fatalf("CredentialStdinTimeout=%v want 30s", defaults.CredentialStdinTimeout)
	}

	args := validMountArgs("--credential-stdin")
	args = append(args[:len(args)-1], "--credential-stdin-timeout", "7s", args[len(args)-1])
	custom, _, err := parseMountArgsWithInput(args, strings.NewReader("apikey:key"))
	if err != nil {
		t.Fatal(err)
	}
	if custom.CredentialStdinTimeout != 7*time.Second {
		t.Fatalf("CredentialStdinTimeout=%v want 7s", custom.CredentialStdinTimeout)
	}
}

func TestParseMountArgsRejectsNonPositiveCredentialStdinTimeout(t *testing.T) {
	for _, value := range []string{"0s", "-1s"} {
		args := validMountArgs("--credential-stdin")
		args = append(args[:len(args)-1], "--credential-stdin-timeout", value, args[len(args)-1])
		if _, _, err := parseMountArgsWithInput(args, strings.NewReader("apikey:key")); err == nil || !strings.Contains(err.Error(), "credential-stdin-timeout must be positive") {
			t.Errorf("credential-stdin-timeout=%s err=%v", value, err)
		}
	}
}

func TestParseMountArgsMutationTimeoutOverride(t *testing.T) {
	args := validMountArgs("--credential-stdin")
	args = append(args[:len(args)-1], "--mutation-timeout", "17s", args[len(args)-1])
	config, _, err := parseMountArgsWithInput(args, strings.NewReader("apikey:key"))
	if err != nil {
		t.Fatal(err)
	}
	if got := config.MutationTimeout; got != 17*time.Second {
		t.Fatalf("MutationTimeout=%v want 17s", got)
	}
}

func TestParseMountArgsRejectsNonPositiveMutationTimeout(t *testing.T) {
	for _, value := range []string{"0s", "-1s"} {
		args := validMountArgs("--credential-stdin")
		args = append(args[:len(args)-1], "--mutation-timeout", value, args[len(args)-1])
		if _, _, err := parseMountArgsWithInput(args, strings.NewReader("apikey:key")); err == nil || !strings.Contains(err.Error(), "mutation-timeout must be positive") {
			t.Errorf("mutation-timeout=%s err=%v", value, err)
		}
	}
}

func TestParseMountArgsRemoteHTTPOptIn(t *testing.T) {
	args := []string{"--endpoint", "http://192.0.2.1", "--instance", "inst", "--region", "cn-hangzhou", "--credential-stdin", "--store", "store", "--session", "session", "/mnt/memory"}
	config, _, err := parseMountArgsWithInput(args, strings.NewReader("aksk:access:secret"))
	if err != nil {
		t.Fatal(err)
	}
	if config.Config.AllowInsecureHTTP {
		t.Fatal("remote HTTP opt-in must be disabled by default")
	}

	args = append(args[:len(args)-1], "--allow-insecure-http", args[len(args)-1])
	config, _, err = parseMountArgsWithInput(args, strings.NewReader("aksk:access:secret"))
	if err != nil {
		t.Fatal(err)
	}
	if !config.Config.AllowInsecureHTTP {
		t.Fatalf("parsed opt-in was not passed to client.Config: %+v", config)
	}
}

func TestParseMountArgsRejectsMalformedSTSCredential(t *testing.T) {
	for _, value := range []string{"sts:access:secret", "sts:access::token", "sts::secret:token", "sts:access:secret:"} {
		if _, _, err := parseMountArgsWithInput(validMountArgs("--credential-stdin"), strings.NewReader(value)); err == nil {
			t.Errorf("credential %q must be rejected", value)
		}
	}
}

func TestParseMountArgsRequiresRegionForSignedCredentials(t *testing.T) {
	args := []string{"--endpoint", "https://example", "--instance", "inst", "--credential-stdin", "--store", "store", "--session", "session", "/mnt/memory"}
	if _, _, err := parseMountArgsWithInput(args, strings.NewReader("aksk:access:secret")); err == nil || !strings.Contains(err.Error(), "region is required") {
		t.Fatalf("err=%v", err)
	}
}

func TestParseMountArgsCredentialFile(t *testing.T) {
	path := t.TempDir() + "/credential"
	if err := os.WriteFile(path, []byte("aksk:file-ak:file-sk\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	config, mountpoint, err := parseMountArgs(validMountArgs("--credential-file", path))
	if err != nil {
		t.Fatal(err)
	}
	if mountpoint != "/mnt/memory" || config.AccessKeyID != "file-ak" || config.AccessKeySecret != "file-sk" {
		t.Fatalf("config=%+v mountpoint=%q", config, mountpoint)
	}
}

func TestParseMountArgsCredentialFileRejectsUnsafePaths(t *testing.T) {
	directory := t.TempDir()
	securePath := filepath.Join(directory, "secure")
	if err := os.WriteFile(securePath, []byte("apikey:key"), 0o600); err != nil {
		t.Fatal(err)
	}

	insecurePath := filepath.Join(directory, "insecure")
	if err := os.WriteFile(insecurePath, []byte("apikey:key"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := parseMountArgs(validMountArgs("--credential-file", insecurePath)); err == nil || !strings.Contains(err.Error(), "group or others") {
		t.Fatalf("insecure permissions err=%v", err)
	}

	symlinkPath := filepath.Join(directory, "credential-link")
	if err := os.Symlink(securePath, symlinkPath); err != nil {
		t.Fatal(err)
	}
	if _, _, err := parseMountArgs(validMountArgs("--credential-file", symlinkPath)); err == nil || !strings.Contains(err.Error(), "directly referenced regular file") {
		t.Fatalf("symlink err=%v", err)
	}

	if _, _, err := parseMountArgs(validMountArgs("--credential-file", directory)); err == nil || !strings.Contains(err.Error(), "regular file") {
		t.Fatalf("directory err=%v", err)
	}

	missingPath := filepath.Join(directory, "missing")
	if _, _, err := parseMountArgs(validMountArgs("--credential-file", missingPath)); err == nil || !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing credential cause was not preserved: %v", err)
	}
}

func TestParseMountArgsCredentialStdin(t *testing.T) {
	input := strings.NewReader(`{"accessKeyId":"stdin-ak","accessKeySecret":"stdin-sk","stsToken":"stdin-token"}`)
	config, mountpoint, err := parseMountArgsWithInput(validMountArgs("--credential-stdin"), input)
	if err != nil {
		t.Fatal(err)
	}
	if mountpoint != "/mnt/memory" || config.AccessKeyID != "stdin-ak" || config.AccessKeySecret != "stdin-sk" || config.STSToken != "stdin-token" {
		t.Fatalf("config=%+v mountpoint=%q", config, mountpoint)
	}
}

func TestParseMountArgsCredentialStdinJSONAKSK(t *testing.T) {
	input := strings.NewReader(`{"accessKeyId":"stdin-ak","accessKeySecret":"stdin-sk","stsToken":""}`)
	config, _, err := parseMountArgsWithInput(validMountArgs("--credential-stdin"), input)
	if err != nil {
		t.Fatal(err)
	}
	if config.AccessKeyID != "stdin-ak" || config.AccessKeySecret != "stdin-sk" || config.STSToken != "" {
		t.Fatalf("config=%+v", config)
	}
}

func TestParseMountArgsCredentialStdinPipeCompletes(t *testing.T) {
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	go func() {
		_, _ = writer.Write([]byte("apikey:pipe-key"))
		_ = writer.Close()
	}()

	config, _, err := parseMountArgsWithInput(validMountArgs("--credential-stdin"), reader)
	if err != nil {
		t.Fatal(err)
	}
	if config.APIKey != "pipe-key" {
		t.Fatalf("APIKey=%q", config.APIKey)
	}
}

func TestParseMountArgsCredentialStdinPipeTimesOut(t *testing.T) {
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	defer writer.Close()
	const partialSecret = "partial-secret-must-not-leak"
	if _, err := writer.Write([]byte("apikey:" + partialSecret)); err != nil {
		t.Fatal(err)
	}

	args := validMountArgs("--credential-stdin")
	args = append(args[:len(args)-1], "--credential-stdin-timeout", "25ms", args[len(args)-1])
	started := time.Now()
	_, _, err = parseMountArgsWithInput(args, reader)
	if err == nil || !strings.Contains(err.Error(), "timed out waiting for a complete credential") {
		t.Fatalf("err=%v", err)
	}
	if strings.Contains(err.Error(), partialSecret) {
		t.Fatal("timeout error leaked partial credential")
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("stdin timeout took %v", elapsed)
	}
}

func TestParseMountArgsCredentialSourcesAreMutuallyExclusive(t *testing.T) {
	path := t.TempDir() + "/credential"
	if err := os.WriteFile(path, []byte("apikey:file-key"), 0o600); err != nil {
		t.Fatal(err)
	}
	tests := [][]string{
		validMountArgs(),
		validMountArgs("--credential-file", path, "--credential-stdin"),
	}
	for _, args := range tests {
		if _, _, err := parseMountArgsWithInput(args, strings.NewReader("apikey:stdin-key")); err == nil || !strings.Contains(err.Error(), "exactly one of") {
			t.Errorf("args=%v err=%v", args, err)
		}
	}
}

func TestParseMountArgsCredentialInputErrorsDoNotLeakPayload(t *testing.T) {
	secret := strings.Repeat("s", 64*1024+1)
	_, _, err := parseMountArgsWithInput(validMountArgs("--credential-stdin"), strings.NewReader(secret))
	if err == nil {
		t.Fatal("expected oversized credential error")
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatal("error leaked credential payload")
	}
}

func TestParseMountArgsRejectsInvalidScope(t *testing.T) {
	for _, scope := range []string{"", "only#two", "app#tenant#*#run", "app#tenant#agent/run"} {
		args := []string{"--endpoint", "x", "--instance", "i", "--credential-stdin", "--store", "s", "--session", "x", "--scope", scope, "/mnt"}
		if _, _, err := parseMountArgsWithInput(args, strings.NewReader("apikey:k")); err == nil {
			t.Errorf("scope %q: expected invalid scope", scope)
		}
	}
}

// 显式 scope 的空段(含只有空白的段)必须被拒，不能退化成省略 --scope 时的
// CLI 四段默认值，否则环境变量漏值会静默挂到另一个命名空间。
func TestParseMountArgsRejectsEmptyScopeField(t *testing.T) {
	for _, scope := range []string{"app#tenant#agent#", "#tenant#agent#run", "app##agent#run", "app#tenant#agent#   "} {
		args := []string{"--endpoint", "https://example", "--instance", "i", "--credential-stdin", "--store", "s", "--session", "x", "--scope", scope, "/mnt"}
		_, _, err := parseMountArgsWithInput(args, strings.NewReader("apikey:k"))
		if err == nil || !strings.Contains(err.Error(), "must not be empty") {
			t.Errorf("scope %q: err=%v", scope, err)
		}
	}
}

// 校验的值必须等于发出去的值:scope 各段两侧空白被归一化,不能把 "run " 原样发走。
func TestParseMountArgsTrimsScopeFields(t *testing.T) {
	args := []string{"--endpoint", "https://example", "--instance", "i", "--credential-stdin", "--store", "s", "--session", "x", "--scope", " app # tenant #agent#run ", "/mnt"}
	config, _, err := parseMountArgsWithInput(args, strings.NewReader("apikey:k"))
	if err != nil {
		t.Fatal(err)
	}
	want := client.Scope{AppID: "app", TenantID: "tenant", AgentID: "agent", RunID: "run"}
	if config.Scope != want {
		t.Fatalf("scope=%+v want %+v", config.Scope, want)
	}
}

func TestParseUmountArgs(t *testing.T) {
	mountpoint, err := parseUmountArgs([]string{"/mnt/memory"})
	if err != nil || mountpoint != "/mnt/memory" {
		t.Fatalf("mountpoint=%q err=%v", mountpoint, err)
	}
	if _, err := parseUmountArgs(nil); err == nil {
		t.Fatal("expected error for missing mountpoint")
	}
	if _, err := parseUmountArgs([]string{"/a", "/b"}); err == nil {
		t.Fatal("expected error for extra arguments")
	}
}

func TestUmountCommandCandidatesFusermount3ThenFusermount(t *testing.T) {
	commands := umountCommands("/mnt/memory")
	if len(commands) != 2 {
		t.Fatalf("commands=%v", commands)
	}
	want := [][]string{{"fusermount3", "-u", "/mnt/memory"}, {"fusermount", "-u", "/mnt/memory"}}
	for i := range want {
		if len(commands[i]) != 3 || commands[i][0] != want[i][0] || commands[i][1] != want[i][1] || commands[i][2] != want[i][2] {
			t.Fatalf("commands[%d]=%v want %v", i, commands[i], want[i])
		}
	}
}

func TestIsUmountBusy(t *testing.T) {
	for _, message := range []string{"Device or resource busy", "fusermount: target is busy"} {
		if !isUmountBusy(message) {
			t.Errorf("message %q must be recognized as busy", message)
		}
	}
	if isUmountBusy("permission denied") {
		t.Fatal("permission error must not be recognized as busy")
	}
}

func TestRunRejectsUnknownSubcommand(t *testing.T) {
	if err := run([]string{"bogus"}); err == nil {
		t.Fatal("expected usage error")
	}
	if err := run(nil); err == nil {
		t.Fatal("expected usage error")
	}
}

func TestRunUsageNamesAgentMemoryFuse(t *testing.T) {
	for _, args := range [][]string{nil, {"bogus"}} {
		err := run(args)
		if err == nil || !strings.HasPrefix(err.Error(), "usage: agent-memory-fuse ") {
			t.Fatalf("args=%v err=%v", args, err)
		}
	}
}

func TestFormatVersionIncludesBuildProvenance(t *testing.T) {
	originalVersion, originalCommit, originalEpoch := version, gitCommit, sourceEpoch
	t.Cleanup(func() {
		version, gitCommit, sourceEpoch = originalVersion, originalCommit, originalEpoch
	})
	version, gitCommit, sourceEpoch = "v0.1.0-beta.1", "abc123", "1788192000"
	if got, want := formatVersion(), "agent-memory-fuse v0.1.0-beta.1 (commit=abc123 source_epoch=1788192000)"; got != want {
		t.Fatalf("formatVersion()=%q want %q", got, want)
	}
}

func TestMountOptionsAdvertiseAtomicTruncate(t *testing.T) {
	options := mountOptions(time.Second, time.Second, false)
	if options.MountOptions.ExtraCapabilities&fuse.CAP_ATOMIC_O_TRUNC == 0 {
		t.Fatal("mount must advertise CAP_ATOMIC_O_TRUNC when Open handles O_TRUNC")
	}
}

func TestMountOptionsUseMountProcessOwner(t *testing.T) {
	options := mountOptions(time.Second, time.Second, false)
	if options.UID != uint32(os.Getuid()) || options.GID != uint32(os.Getgid()) {
		t.Fatalf("owner=%d:%d want %d:%d", options.UID, options.GID, os.Getuid(), os.Getgid())
	}
}

func TestMountOptionsAllowOther(t *testing.T) {
	if mountOptions(time.Second, time.Second, false).MountOptions.AllowOther {
		t.Fatal("allow-other must stay off by default")
	}
	if !mountOptions(time.Second, time.Second, true).MountOptions.AllowOther {
		t.Fatal("allow-other must be forwarded to FUSE mount options")
	}
}

func TestParseMountArgsAllowOtherDefaultsFalse(t *testing.T) {
	config, _, err := parseMountArgsWithInput(validMountArgs("--credential-stdin"), strings.NewReader("apikey:key"))
	if err != nil {
		t.Fatalf("parse defaults: %v", err)
	}
	if config.AllowOther {
		t.Fatal("allow-other must default to false")
	}
	args := validMountArgs("--credential-stdin", "--allow-other")
	config, _, err = parseMountArgsWithInput(args, strings.NewReader("apikey:key"))
	if err != nil {
		t.Fatalf("parse --allow-other: %v", err)
	}
	if !config.AllowOther {
		t.Fatal("--allow-other must set config.AllowOther")
	}
}

func TestParseMountCacheTTLs(t *testing.T) {
	defaults, _, err := parseMountArgsWithInput(validMountArgs("--credential-stdin"), strings.NewReader("apikey:key"))
	if err != nil {
		t.Fatalf("parse defaults: %v", err)
	}
	if defaults.MetadataTTL != 5*time.Second || defaults.NegativeTTL != time.Second || defaults.DirectoryTTL != 10*time.Second {
		t.Fatalf("default TTLs=(%v,%v,%v) want (5s,1s,10s)", defaults.MetadataTTL, defaults.NegativeTTL, defaults.DirectoryTTL)
	}

	args := validMountArgs("--credential-stdin")
	mountpoint := args[len(args)-1]
	args = append(args[:len(args)-1],
		"--metadata-ttl", "7s",
		"--negative-ttl", "2s",
		"--directory-ttl", "13s",
		mountpoint,
	)
	custom, _, err := parseMountArgsWithInput(args, strings.NewReader("apikey:key"))
	if err != nil {
		t.Fatalf("parse custom TTLs: %v", err)
	}
	if custom.MetadataTTL != 7*time.Second || custom.NegativeTTL != 2*time.Second || custom.DirectoryTTL != 13*time.Second {
		t.Fatalf("custom TTLs=(%v,%v,%v) want (7s,2s,13s)", custom.MetadataTTL, custom.NegativeTTL, custom.DirectoryTTL)
	}

	for _, test := range []struct {
		flag  string
		value string
	}{
		{flag: "--metadata-ttl", value: "0s"},
		{flag: "--metadata-ttl", value: "-1s"},
		{flag: "--negative-ttl", value: "0s"},
		{flag: "--negative-ttl", value: "-1s"},
		{flag: "--directory-ttl", value: "0s"},
		{flag: "--directory-ttl", value: "-1s"},
	} {
		t.Run(test.flag+"="+test.value, func(t *testing.T) {
			args := validMountArgs("--credential-stdin")
			mountpoint := args[len(args)-1]
			args = append(args[:len(args)-1], test.flag, test.value, mountpoint)
			if _, _, err := parseMountArgsWithInput(args, strings.NewReader("apikey:key")); err == nil || !strings.Contains(err.Error(), "must be positive") {
				t.Fatalf("err=%v want positive TTL validation", err)
			}
		})
	}
}

func TestMountOptionsUseMetadataAndNegativeTTLs(t *testing.T) {
	options := mountOptions(7*time.Second, 2*time.Second, false)
	if options.EntryTimeout == nil || *options.EntryTimeout != 7*time.Second {
		t.Fatalf("EntryTimeout=%v want 7s", options.EntryTimeout)
	}
	if options.AttrTimeout == nil || *options.AttrTimeout != 7*time.Second {
		t.Fatalf("AttrTimeout=%v want 7s", options.AttrTimeout)
	}
	if options.NegativeTimeout == nil || *options.NegativeTimeout != 2*time.Second {
		t.Fatalf("NegativeTimeout=%v want 2s", options.NegativeTimeout)
	}
}

type mutationLogRemote struct {
	memoryfs.Remote
	err        error
	createPath string
}

func (r *mutationLogRemote) Create(_ context.Context, path string, _ []byte) (client.Memory, error) {
	r.createPath = path
	return client.Memory{}, r.err
}

func TestReportMutationErrorLogsOnlySafeEventFieldsEndToEnd(t *testing.T) {
	const (
		rawPath       = "private/path-sentinel.md"
		rawMessage    = "raw-message-sentinel"
		credentialRaw = "credential-sentinel"
		endpointRaw   = "https://endpoint-sentinel"
		contentRaw    = "content-sentinel"
		wrappedRaw    = "wrapped-client-detail-sentinel"
	)
	remote := &mutationLogRemote{err: fmt.Errorf("%s: %w", wrappedRaw, &client.APIError{
		StatusCode: http.StatusInternalServerError,
		Code:       "INTERNAL_ERROR",
		RequestID:  "req-safe",
		Message:    strings.Join([]string{rawMessage, rawPath, credentialRaw, endpointRaw, contentRaw}, " "),
	})}
	filesystem := memoryfs.NewFileSystem(remote, memoryfs.Options{MutationErrorReporter: reportMutationError})
	handle, errno := filesystem.Create("/" + rawPath + "/")
	if errno != 0 {
		t.Fatalf("Create errno=%v", errno)
	}

	oldWriter, oldFlags, oldPrefix := log.Writer(), log.Flags(), log.Prefix()
	t.Cleanup(func() {
		log.SetOutput(oldWriter)
		log.SetFlags(oldFlags)
		log.SetPrefix(oldPrefix)
	})
	var output bytes.Buffer
	log.SetOutput(&output)
	log.SetFlags(0)
	log.SetPrefix("")

	if errno := filesystem.Commit(context.Background(), handle); errno != syscall.EIO {
		t.Fatalf("Commit errno=%v want EIO", errno)
	}
	if remote.createPath != rawPath {
		t.Fatalf("remote Create path=%q want normalized %q", remote.createPath, rawPath)
	}

	const prefix = "file memory mutation error"
	logged := output.String()
	if strings.Count(logged, "\n") != 2 || !strings.HasSuffix(logged, "\n") {
		t.Fatalf("logger output must be exactly two lines: %q", logged)
	}
	lines := strings.Split(strings.TrimSuffix(logged, "\n"), "\n")
	if lines[0] != prefix {
		t.Fatalf("first line must be the fixed prefix: %q", lines[0])
	}
	if !strings.HasPrefix(lines[1], "mutation_error json: {") {
		t.Fatalf("second line must be tagged JSON 'mutation_error json: {...}': %q", lines[1])
	}
	var fields map[string]any
	if err := json.Unmarshal([]byte(strings.TrimPrefix(lines[1], "mutation_error json: ")), &fields); err != nil {
		t.Fatalf("invalid event JSON: %v; line=%q", err, lines[1])
	}
	wantFields := []string{"event", "operation", "class", "reason", "path_sha256", "category", "errno", "http_status", "service_code", "request_id", "message"}
	if len(fields) != len(wantFields) {
		t.Fatalf("fields=%v want exactly %v", fields, wantFields)
	}
	for _, field := range wantFields {
		if _, ok := fields[field]; !ok {
			t.Fatalf("missing field %q in %v", field, fields)
		}
	}
	wantHash := sha256.Sum256([]byte("agent-memory-fuse/mutation-path/v1\x00" + rawPath))
	if fields["event"] != "mutation_error" || fields["class"] != "retryable" || fields["reason"] != "server_error" || fields["operation"] != "create" || fields["path_sha256"] != hex.EncodeToString(wantHash[:]) || fields["category"] != "service" || int(fields["errno"].(float64)) != int(syscall.EIO) || int(fields["http_status"].(float64)) != http.StatusInternalServerError || fields["service_code"] != "INTERNAL_ERROR" || fields["request_id"] != "req-safe" || fields["message"] != "service request failed" {
		t.Fatalf("unsafe or incorrect event fields: %v", fields)
	}
	for _, forbidden := range []string{rawPath, rawMessage, credentialRaw, endpointRaw, contentRaw, wrappedRaw} {
		if strings.Contains(logged, forbidden) {
			t.Fatalf("formatted log leaked %q: %s", forbidden, logged)
		}
	}
}

func TestReportMutationErrorDropsUnsafeServiceIdentifiers(t *testing.T) {
	const (
		serviceCodeSentinel = "private/path-sentinel.md"
		requestIDSentinel   = "credential=secret-sentinel"
	)
	oldWriter, oldFlags, oldPrefix := log.Writer(), log.Flags(), log.Prefix()
	t.Cleanup(func() {
		log.SetOutput(oldWriter)
		log.SetFlags(oldFlags)
		log.SetPrefix(oldPrefix)
	})
	var output bytes.Buffer
	log.SetOutput(&output)
	log.SetFlags(0)
	log.SetPrefix("")

	reportMutationError(memoryfs.MutationErrorEvent{
		Operation:   "create",
		PathSHA256:  strings.Repeat("a", 64),
		Category:    "service",
		Errno:       int(syscall.EIO),
		HTTPStatus:  http.StatusInternalServerError,
		ServiceCode: serviceCodeSentinel,
		RequestID:   requestIDSentinel,
		Message:     "service request failed",
	})

	logged := output.String()
	if strings.Count(logged, "\n") != 2 || !strings.HasSuffix(logged, "\n") {
		t.Fatalf("logger output must be exactly two lines: %q", logged)
	}
	lines := strings.Split(strings.TrimSuffix(logged, "\n"), "\n")
	if lines[0] != mutationErrorLogPrefix || !strings.HasPrefix(lines[1], "mutation_error json: {") {
		t.Fatalf("unexpected line format: %q", logged)
	}
	if strings.Contains(logged, serviceCodeSentinel) || strings.Contains(logged, requestIDSentinel) {
		t.Fatalf("logger leaked unsafe service identifiers: %s", logged)
	}
	var fields map[string]any
	if err := json.Unmarshal([]byte(strings.TrimPrefix(lines[1], "mutation_error json: ")), &fields); err != nil {
		t.Fatalf("invalid event JSON: %v; line=%q", err, lines[1])
	}
	if _, exists := fields["service_code"]; exists {
		t.Fatalf("unsafe service_code was not omitted: %v", fields)
	}
	if _, exists := fields["request_id"]; exists {
		t.Fatalf("unsafe request_id was not omitted: %v", fields)
	}
	// Directly constructed events without classification fall back to unknown.
	if fields["class"] != "unknown" || fields["reason"] != "unknown" {
		t.Fatalf("unclassified event must fall back to unknown: %v", fields)
	}
}

func TestFormatMutationErrorLogIncludesOnlySafeSDKKind(t *testing.T) {
	event := memoryfs.MutationErrorEvent{
		Operation:  "read",
		PathSHA256: strings.Repeat("a", 64),
		Category:   "client",
		Errno:      int(syscall.EIO),
		SDKKind:    "transport_other",
		Message:    "client-side error",
	}
	line, err := formatMutationErrorLog(event)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	jsonLine := strings.TrimPrefix(strings.SplitN(line, "\n", 2)[1], memoryfs.MutationErrorEventName+" json: ")
	if err := json.Unmarshal([]byte(jsonLine), &fields); err != nil {
		t.Fatal(err)
	}
	if fields["sdk_kind"] != "transport_other" {
		t.Fatalf("sdk_kind=%v want transport_other", fields["sdk_kind"])
	}

	event.SDKKind = "credential-sentinel"
	line, err = formatMutationErrorLog(event)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(line, "credential-sentinel") || strings.Contains(line, "sdk_kind") {
		t.Fatalf("unsafe sdk_kind was not omitted: %s", line)
	}
}

var _ memoryfs.Remote = (*mutationLogRemote)(nil)

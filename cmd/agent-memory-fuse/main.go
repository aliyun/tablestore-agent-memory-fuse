package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"agent-memory-fuse/internal/client"
	"agent-memory-fuse/internal/credential"
	memoryfs "agent-memory-fuse/internal/fs"

	fs "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

type mountConfig struct {
	client.Config
	ReadOnly               bool
	AllowOther             bool
	MetadataTTL            time.Duration
	NegativeTTL            time.Duration
	DirectoryTTL           time.Duration
	MutationTimeout        time.Duration
	CredentialStdinTimeout time.Duration
}

const mutationErrorLogPrefix = "file memory mutation error"

var (
	version     = "dev"
	gitCommit   = "unknown"
	sourceEpoch = "unknown"
)

type mutationErrorLogRecord struct {
	Event       string `json:"event"`
	Operation   string `json:"operation"`
	Class       string `json:"class"`
	Reason      string `json:"reason"`
	PathSHA256  string `json:"path_sha256"`
	Category    string `json:"category"`
	Errno       int    `json:"errno"`
	HTTPStatus  int    `json:"http_status,omitempty"`
	ServiceCode string `json:"service_code,omitempty"`
	RequestID   string `json:"request_id,omitempty"`
	SDKKind     string `json:"sdk_kind,omitempty"`
	Message     string `json:"message"`
}

func formatMutationErrorLog(event memoryfs.MutationErrorEvent) (string, error) {
	event = memoryfs.SanitizeMutationErrorIdentifiers(event)
	payload, err := json.Marshal(mutationErrorLogRecord{
		Event:       memoryfs.MutationErrorEventName,
		Operation:   event.Operation,
		Class:       event.Class,
		Reason:      event.Reason,
		PathSHA256:  event.PathSHA256,
		Category:    event.Category,
		Errno:       event.Errno,
		HTTPStatus:  event.HTTPStatus,
		ServiceCode: event.ServiceCode,
		RequestID:   event.RequestID,
		SDKKind:     event.SDKKind,
		Message:     event.Message,
	})
	if err != nil {
		return "", err
	}
	// Two-line output shared with the mount failure event: human anchor line,
	// then a tagged JSON line "<event> json: {...}" for machine collectors.
	return mutationErrorLogPrefix + "\n" + memoryfs.MutationErrorEventName + " json: " + string(payload), nil
}

func reportMutationError(event memoryfs.MutationErrorEvent) {
	line, err := formatMutationErrorLog(event)
	if err != nil {
		// The record contains only strings and integers, so this branch is
		// defensive. Keep even its fallback fixed and free of raw data.
		log.Print(mutationErrorLogPrefix + "\n" + memoryfs.MutationErrorEventName + ` json: {"event":"mutation_error","class":"unknown","reason":"unknown","message":"client-side error"}`)
		return
	}
	log.Print(line)
}

func parseMountArgs(args []string) (mountConfig, string, error) {
	return parseMountArgsWithInput(args, os.Stdin)
}

func parseMountArgsWithInput(args []string, input io.Reader) (mountConfig, string, error) {
	flags := flag.NewFlagSet("agent-memory-fuse mount", flag.ContinueOnError)
	var config mountConfig
	var credentialFile, scope string
	var credentialStdin bool
	flags.StringVar(&config.Endpoint, "endpoint", "", "HTTPS memory service gateway URL (loopback HTTP allowed; remote HTTP requires explicit test opt-in)")
	flags.BoolVar(&config.Config.AllowInsecureHTTP, "allow-insecure-http", false, "allow non-loopback HTTP endpoint for explicit testing only")
	flags.StringVar(&config.Instance, "instance", "", "TableStore instance name")
	flags.StringVar(&config.Region, "region", "", "TableStore region (required for AK/SK and STS v4 signing)")
	flags.StringVar(&credentialFile, "credential-file", "", "read credential from file")
	flags.BoolVar(&credentialStdin, "credential-stdin", false, "read credential from standard input")
	flags.DurationVar(&config.CredentialStdinTimeout, "credential-stdin-timeout", 30*time.Second, "maximum time to wait for a complete credential on standard input")
	flags.StringVar(&config.StoreName, "store", "", "file memory store name")
	flags.StringVar(&scope, "scope", "", "app#tenant#agent#run exact scope")
	flags.StringVar(&config.SessionID, "session", "", "session ID used for version attribution")
	flags.BoolVar(&config.ReadOnly, "read-only", false, "mount read-only")
	flags.BoolVar(&config.AllowOther, "allow-other", false, "allow other users (including root) to access the mountpoint")
	flags.DurationVar(&config.MetadataTTL, "metadata-ttl", 5*time.Second, "positive file metadata cache TTL")
	flags.DurationVar(&config.NegativeTTL, "negative-ttl", time.Second, "missing path lookup cache TTL")
	flags.DurationVar(&config.DirectoryTTL, "directory-ttl", 10*time.Second, "directory snapshot cache TTL")
	flags.DurationVar(&config.MutationTimeout, "mutation-timeout", time.Minute, "positive wall-clock timeout for each remote mutation")
	if err := flags.Parse(args); err != nil {
		return mountConfig{}, "", err
	}
	scopeProvided := false
	flags.Visit(func(parsed *flag.Flag) {
		if parsed.Name == "scope" {
			scopeProvided = true
		}
	})
	if config.MetadataTTL <= 0 || config.NegativeTTL <= 0 || config.DirectoryTTL <= 0 {
		return mountConfig{}, "", errors.New("metadata-ttl, negative-ttl and directory-ttl must be positive")
	}
	if config.MutationTimeout <= 0 {
		return mountConfig{}, "", errors.New("mutation-timeout must be positive")
	}
	if config.CredentialStdinTimeout <= 0 {
		return mountConfig{}, "", errors.New("credential-stdin-timeout must be positive")
	}
	if flags.NArg() != 1 {
		return mountConfig{}, "", errors.New("exactly one mountpoint is required")
	}
	if (credentialFile == "" && !credentialStdin) || (credentialFile != "" && credentialStdin) {
		return mountConfig{}, "", errors.New("exactly one of credential-file and credential-stdin is required")
	}
	if credentialFile != "" {
		file, err := openCredentialFile(credentialFile)
		if err != nil {
			return mountConfig{}, "", fmt.Errorf("open credential file: %w", err)
		}
		defer file.Close()
		if err := applyCredentialReader(&config, file); err != nil {
			return mountConfig{}, "", fmt.Errorf("read credential file: %w", err)
		}
	} else {
		if input == nil {
			return mountConfig{}, "", errors.New("credential stdin is unavailable")
		}
		if err := applyCredentialReaderWithTimeout(&config, input, config.CredentialStdinTimeout); err != nil {
			return mountConfig{}, "", fmt.Errorf("read credential from stdin: %w", err)
		}
	}

	if !scopeProvided {
		config.Scope = client.Scope{AppID: "__default__", TenantID: "__default__", AgentID: "__default__", RunID: "__default__"}
	} else {
		parts := strings.Split(scope, "#")
		if len(parts) != 4 {
			return mountConfig{}, "", errors.New("scope must contain four '#' separated fields")
		}
		for i, part := range parts {
			part = strings.TrimSpace(part)
			// 显式 scope 必须完整；否则环境变量漏值会静默挂到另一个命名空间。
			if part == "" {
				return mountConfig{}, "", errors.New("scope fields must not be empty")
			}
			if part == "*" || strings.Contains(part, "/") {
				return mountConfig{}, "", errors.New("scope must be exact")
			}
			parts[i] = part
		}
		config.Scope = client.Scope{AppID: parts[0], TenantID: parts[1], AgentID: parts[2], RunID: parts[3]}
	}
	if config.Endpoint == "" || config.Instance == "" || config.StoreName == "" || config.SessionID == "" {
		return mountConfig{}, "", errors.New("endpoint, instance, store and session are required")
	}
	// 与 client.New 一致地先归一化,否则 --region "  " 会溜过这里、改由 client 层
	// 用另一套措辞报错。
	config.Region = strings.TrimSpace(config.Region)
	if config.APIKey == "" && config.Region == "" {
		return mountConfig{}, "", errors.New("region is required for AK/SK and STS credentials")
	}
	return config, flags.Arg(0), nil
}

func applyCredentialReader(config *mountConfig, input io.Reader) error {
	payload, err := readCredentialPayload(input)
	if err != nil {
		return errors.New("read credential payload")
	}
	return applyCredentialPayload(config, payload)
}

func applyCredentialReaderWithTimeout(config *mountConfig, input io.Reader, timeout time.Duration) error {
	payload, err := readCredentialPayloadWithTimeout(input, timeout)
	if err != nil {
		return err
	}
	return applyCredentialPayload(config, payload)
}

func readCredentialPayload(input io.Reader) ([]byte, error) {
	return io.ReadAll(io.LimitReader(input, credential.MaxRotationPayloadBytes+1))
}

func applyCredentialPayload(config *mountConfig, payload []byte) error {
	if len(payload) > credential.MaxRotationPayloadBytes {
		return errors.New("credential payload exceeds 64 KiB")
	}
	value := strings.TrimSpace(string(payload))
	if value == "" {
		return errors.New("credential payload is empty")
	}
	if strings.HasPrefix(value, "{") {
		credentials, err := credential.ParseAccessKeyCredentials([]byte(value))
		if err != nil {
			return err
		}
		config.AccessKeyID = credentials.AccessKeyID
		config.AccessKeySecret = credentials.AccessKeySecret
		config.STSToken = credentials.Token
		return nil
	}
	return applyCredentialValue(config, value)
}

func applyCredentialValue(config *mountConfig, value string) error {
	switch {
	case strings.HasPrefix(value, "apikey:"):
		config.APIKey = strings.TrimPrefix(value, "apikey:")
	case strings.HasPrefix(value, "aksk:"):
		parts := strings.SplitN(strings.TrimPrefix(value, "aksk:"), ":", 2)
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			return errors.New("aksk credential must be aksk:<id>:<secret>")
		}
		config.AccessKeyID, config.AccessKeySecret = parts[0], parts[1]
	case strings.HasPrefix(value, "sts:"):
		// token 放末段且用 SplitN 截断:STS token 是不透明字符串,自身可能含冒号。
		parts := strings.SplitN(strings.TrimPrefix(value, "sts:"), ":", 3)
		if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
			return errors.New("sts credential must be sts:<id>:<secret>:<token>")
		}
		config.AccessKeyID, config.AccessKeySecret, config.STSToken = parts[0], parts[1], parts[2]
	case value != "":
		config.APIKey = value
	default:
		return errors.New("credential is required")
	}
	return nil
}

func run(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: agent-memory-fuse mount|umount|rotate-credential|version [options]")
	}
	switch args[0] {
	case "mount":
		return runMount(args[1:])
	case "umount":
		mountpoint, err := parseUmountArgs(args[1:])
		if err != nil {
			return err
		}
		return runUmount(mountpoint)
	case "rotate-credential":
		return runRotateCredential(args[1:], os.Stdin, systemSetxattr)
	case "version":
		if len(args) != 1 {
			return errors.New("usage: agent-memory-fuse version")
		}
		fmt.Fprintln(os.Stdout, formatVersion())
		return nil
	default:
		return errors.New("usage: agent-memory-fuse mount|umount|rotate-credential|version [options]")
	}
}

func formatVersion() string {
	return fmt.Sprintf("agent-memory-fuse %s (commit=%s source_epoch=%s)", version, gitCommit, sourceEpoch)
}

func parseUmountArgs(args []string) (string, error) {
	flags := flag.NewFlagSet("agent-memory-fuse umount", flag.ContinueOnError)
	if err := flags.Parse(args); err != nil {
		return "", err
	}
	if flags.NArg() != 1 {
		return "", errors.New("usage: agent-memory-fuse umount MOUNTPOINT")
	}
	return flags.Arg(0), nil
}

// umountCommands 返回按序尝试的卸载命令：优先 fusermount3，回退 fusermount。
func umountCommands(mountpoint string) [][]string {
	return [][]string{
		{"fusermount3", "-u", mountpoint},
		{"fusermount", "-u", mountpoint},
	}
}

func runUmount(mountpoint string) error {
	var lookupErr error
	for _, argv := range umountCommands(mountpoint) {
		binary, err := exec.LookPath(argv[0])
		if err != nil {
			lookupErr = err
			continue
		}
		output, err := exec.Command(binary, argv[1:]...).CombinedOutput()
		if err != nil {
			message := strings.TrimSpace(string(output))
			if isUmountBusy(message) {
				return fmt.Errorf("%s -u %s: mountpoint is busy; close files under it and make other processes leave the directory, then retry (inspect with: fuser -vm %q): %w: %s", argv[0], mountpoint, mountpoint, err, message)
			}
			return fmt.Errorf("%s -u %s: %w: %s", argv[0], mountpoint, err, message)
		}
		return nil
	}
	return fmt.Errorf("neither fusermount3 nor fusermount is available: %w", lookupErr)
}

func isUmountBusy(message string) bool {
	message = strings.ToLower(message)
	return strings.Contains(message, "device or resource busy") || strings.Contains(message, "target is busy")
}

func runMount(args []string) error {
	config, mountpoint, err := parseMountArgs(args)
	if err != nil {
		return err
	}
	api, err := client.New(config.Config)
	if err != nil {
		return err
	}
	filesystem := memoryfs.NewFileSystem(api, memoryfs.Options{
		ReadOnly:              config.ReadOnly,
		MetadataTTL:           config.MetadataTTL,
		NegativeTTL:           config.NegativeTTL,
		DirectoryTTL:          config.DirectoryTTL,
		MutationTimeout:       config.MutationTimeout,
		MutationErrorReporter: reportMutationError,
	})
	defer filesystem.Close()
	if err := filesystem.Refresh(context.Background()); err != nil {
		return classifyMountError(err)
	}
	server, err := fs.Mount(mountpoint, memoryfs.NewRoot(filesystem), mountOptions(config.MetadataTTL, config.NegativeTTL, config.AllowOther))
	if err != nil {
		return err
	}
	// files=0 且 readOnly=true 多半是存量 file+ots 库未 Reproject——投影键
	// 派生规则变更后旧键查不到 pointer，服务端按空库返回而非报错。
	log.Printf("mounted %s: store=%s files=%d readOnly=%v", mountpoint, config.StoreName, filesystem.Index().Count(), filesystem.ReadOnly())
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		if err := server.Unmount(); err != nil {
			if isUmountBusy(err.Error()) {
				log.Printf("unmount: mountpoint is busy; close files under it and make other processes leave the directory, then run %q umount %q (inspect with: fuser -vm %q); the FUSE process will keep running: %v", executablePath(), mountpoint, mountpoint, err)
				return
			}
			log.Printf("unmount: %v", err)
		}
	}()
	server.Wait()
	return nil
}

func executablePath() string {
	path, err := os.Executable()
	if err == nil {
		return path
	}
	return os.Args[0]
}

func mountOptions(metadataTTL, negativeTTL time.Duration, allowOther bool) *fs.Options {
	return &fs.Options{
		MountOptions: fuse.MountOptions{
			FsName:            "file-memory",
			Name:              "file-memory",
			MaxWrite:          memoryfs.MaxContentBytes,
			ExtraCapabilities: fuse.CAP_ATOMIC_O_TRUNC,
			AllowOther:        allowOther,
			// DirectMount：先自行 mount(2)，失败自动回退 fusermount。容器环境
			// （特权 sidecar 或非特权 userns）镜像里通常没有 fusermount，
			// 直挂让挂载不依赖该外部二进制。
			DirectMount: true,
		},
		EntryTimeout: &metadataTTL, AttrTimeout: &metadataTTL, NegativeTimeout: &negativeTTL,
		UID: uint32(os.Getuid()), GID: uint32(os.Getgid()),
	}
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		var failure *mountFailure
		if errors.As(err, &failure) {
			// Two-line output: human anchor line (timestamped) + pure JSON
			// line starting at '{' for machine collectors. Exit code carries
			// the retryable/auth/config classification for supervisors.
			log.Print(failure.logLines())
			os.Exit(failure.exitCode)
		}
		log.Fatal(err)
	}
}

package client

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"agent-memory-fuse/internal/credential"

	"github.com/aliyun/tablestore-agent-storage-sdk/go/agentstorage"
)

const testRegion = "test-region"

type recordedCall struct {
	action string
	body   map[string]any
}

// newRecordingServer 起 TLS 服务器:SDK 对 apikey 凭证强制 https 端点。
func newRecordingServer(t *testing.T, calls *[]recordedCall, respond func(action string, w http.ResponseWriter)) *httptest.Server {
	t.Helper()
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		*calls = append(*calls, recordedCall{action: r.URL.Path, body: body})
		respond(r.URL.Path, w)
	}))
	t.Cleanup(server.Close)
	return server
}

func newAPIKeyConfig(server *httptest.Server) Config {
	return Config{Endpoint: server.URL, Instance: "instance-a", APIKey: "key", StoreName: "memstore_a", SessionID: "session-a", HTTPClient: server.Client()}
}

func TestClientItemActionsAndBodies(t *testing.T) {
	var calls []recordedCall
	server := newRecordingServer(t, &calls, func(action string, w http.ResponseWriter) {
		scope := map[string]any{"appId": "app-a", "tenantId": "tenant-a", "agentId": "agent-a", "runId": "run-a"}
		item := func(itemID, path, contentSHA string, contentSize, latestSeq int, latestVersionID, createdAt, updatedAt string) map[string]any {
			return map[string]any{
				"type": "memoryfile", "itemId": itemID, "scope": scope, "path": path,
				"contentSha256": contentSHA, "contentSizeBytes": contentSize, "latestSeq": latestSeq, "latestVersionId": latestVersionID,
				"createdAt": createdAt, "updatedAt": updatedAt,
			}
		}
		request := calls[len(calls)-1].body
		var response map[string]any
		switch action {
		case "/ListItems":
			response = map[string]any{
				"type":      "memoryfile",
				"items":     []map[string]any{item("mem_a", "/a.md", "sha", 5, 2, "fmver_2", "2026-07-01T01:02:03Z", "2026-07-01T02:03:04.5Z")},
				"nextToken": "", "readOnly": true,
			}
		case "/GetItem":
			response = item("mem_a", "/a.md", "sha", 5, 2, "fmver_2", "2026-07-01T01:02:03Z", "2026-07-01T02:03:04.5Z")
			if request["view"] == "full" {
				response["content"] = "hello"
			}
		case "/AddItem":
			response = item("mem_created", "/a.md", "sha-hello", 5, 1, "fmver_created", "2026-07-01T01:02:03Z", "2026-07-01T01:02:03Z")
			response["content"] = "hello"
		case "/UpdateItem":
			if request["content"] != nil {
				response = item("mem_a", "/a.md", "sha-changed", 7, 3, "fmver_3", "2026-07-01T01:02:03Z", "2026-07-01T04:05:06.7Z")
				response["content"] = "changed"
			} else {
				response = item("mem_a", "/b.md", "sha-changed", 7, 4, "fmver_4", "2026-07-01T01:02:03Z", "2026-07-01T05:06:07.8Z")
			}
		case "/DeleteItem":
			response = map[string]any{"type": "memoryfile", "itemId": "mem_a", "deleted": true}
		}
		_ = json.NewEncoder(w).Encode(response)
	})

	config := newAPIKeyConfig(server)
	config.Scope = Scope{AppID: "app-a", TenantID: "tenant-a", AgentID: "agent-a", RunID: "run-a"}
	c, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	listResult, err := c.List(ctx, "notes/", "token-1", 100)
	if err != nil {
		t.Fatal(err)
	}
	statMemory, err := c.Stat(ctx, "a.md")
	if err != nil {
		t.Fatal(err)
	}
	readMemory, err := c.Read(ctx, "a.md")
	if err != nil {
		t.Fatal(err)
	}
	createMemory, err := c.Create(ctx, "a.md", []byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	updateMemory, err := c.Update(ctx, "mem_a", []byte("changed"), "fmver_2")
	if err != nil {
		t.Fatal(err)
	}
	renameMemory, err := c.Rename(ctx, "mem_a", "b.md", true, "fmver_3")
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Delete(ctx, "mem_a", "fmver_4"); err != nil {
		t.Fatal(err)
	}

	createdAt := time.Date(2026, 7, 1, 1, 2, 3, 0, time.UTC)
	initialUpdatedAt := time.Date(2026, 7, 1, 2, 3, 4, 500_000_000, time.UTC)
	initialBasic := Memory{
		MemoryID: "mem_a", Path: "/a.md", ContentSHA256: "sha", ContentSizeBytes: 5,
		LatestSeq: 2, LatestVersionID: "fmver_2", CreatedAt: createdAt, UpdatedAt: initialUpdatedAt,
	}
	assertMemory := func(name string, got, want Memory) {
		t.Helper()
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s memory=%+v want %+v", name, got, want)
		}
	}
	if len(listResult.Memories) != 1 || listResult.NextToken != "" || !listResult.ReadOnly {
		t.Fatalf("list result=%+v", listResult)
	}
	assertMemory("list", listResult.Memories[0], initialBasic)
	assertMemory("stat", statMemory, initialBasic)
	readWant := initialBasic
	readWant.Content = "hello"
	assertMemory("read", readMemory, readWant)
	assertMemory("create", createMemory, Memory{
		MemoryID: "mem_created", Path: "/a.md", Content: "hello", ContentSHA256: "sha-hello", ContentSizeBytes: 5,
		LatestSeq: 1, LatestVersionID: "fmver_created", CreatedAt: createdAt, UpdatedAt: createdAt,
	})
	assertMemory("update", updateMemory, Memory{
		MemoryID: "mem_a", Path: "/a.md", Content: "changed", ContentSHA256: "sha-changed", ContentSizeBytes: 7,
		LatestSeq: 3, LatestVersionID: "fmver_3", CreatedAt: createdAt,
		UpdatedAt: time.Date(2026, 7, 1, 4, 5, 6, 700_000_000, time.UTC),
	})
	assertMemory("rename", renameMemory, Memory{
		MemoryID: "mem_a", Path: "/b.md", ContentSHA256: "sha-changed", ContentSizeBytes: 7,
		LatestSeq: 4, LatestVersionID: "fmver_4", CreatedAt: createdAt,
		UpdatedAt: time.Date(2026, 7, 1, 5, 6, 7, 800_000_000, time.UTC),
	})

	// Stat 与 Read 都走 /GetItem(以 basic/full view 区分),Rename 与 Update
	// 都走 /UpdateItem(以目标 path/content 分支区分)。
	wantScope := map[string]any{"appId": "app-a", "tenantId": "tenant-a", "agentId": "agent-a", "runId": "run-a"}
	scopedBody := func(fields map[string]any) map[string]any {
		body := map[string]any{"type": "memoryfile", "memoryStoreName": "memstore_a", "scope": wantScope}
		for key, value := range fields {
			body[key] = value
		}
		return body
	}
	wantCalls := []recordedCall{
		{action: "/ListItems", body: scopedBody(map[string]any{"pathPrefix": "/notes/", "nextToken": "token-1", "limit": float64(100)})},
		{action: "/GetItem", body: scopedBody(map[string]any{"path": "/a.md"})},
		{action: "/GetItem", body: scopedBody(map[string]any{"path": "/a.md", "view": "full"})},
		{action: "/AddItem", body: scopedBody(map[string]any{"path": "/a.md", "content": "hello", "sessionId": "session-a", "view": "full"})},
		{action: "/UpdateItem", body: scopedBody(map[string]any{"itemId": "mem_a", "content": "changed", "precondition": map[string]any{"expectedVersionId": "fmver_2"}, "sessionId": "session-a", "view": "full"})},
		{action: "/UpdateItem", body: scopedBody(map[string]any{"itemId": "mem_a", "path": "/b.md", "precondition": map[string]any{"expectedVersionId": "fmver_3"}, "overwrite": true, "sessionId": "session-a"})},
		{action: "/DeleteItem", body: scopedBody(map[string]any{"itemId": "mem_a", "precondition": map[string]any{"expectedVersionId": "fmver_4"}, "sessionId": "session-a"})},
	}
	if !reflect.DeepEqual(calls, wantCalls) {
		t.Errorf("recorded calls:\n got: %#v\nwant: %#v", calls, wantCalls)
	}
}

func TestClientRejectsUnexpectedRecursiveListEntryType(t *testing.T) {
	var calls []recordedCall
	server := newRecordingServer(t, &calls, func(_ string, w http.ResponseWriter) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"type": "memoryfile",
			"items": []map[string]any{{
				"type":  "memoryfile_prefix",
				"scope": map[string]any{"appId": "app-a", "tenantId": "tenant-a", "agentId": "agent-a", "runId": "run-a"},
				"path":  "/notes/deep/",
			}},
		})
	})

	c, err := New(newAPIKeyConfig(server))
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.List(context.Background(), "notes/", "", 100)
	if err == nil || !strings.Contains(err.Error(), "memoryfile_prefix") || !strings.Contains(err.Error(), "/notes/deep/") {
		t.Fatalf("err=%v, want contract error mentioning unexpected type and path", err)
	}
}

func TestClientSendsAuthHeaders(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("x-ots-apikey") != "key" || r.Header.Get("x-ots-instancename") != "instance-a" {
			t.Errorf("headers=%v", r.Header)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"type": "memoryfile", "itemId": "mem_a", "path": "/a.md"})
	}))
	defer server.Close()
	c, err := New(newAPIKeyConfig(server))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Stat(context.Background(), "/a.md"); err != nil {
		t.Fatal(err)
	}
}

// signedResponse 按请求携带的 V4 date/region 派生响应签名密钥。AK/SK(含
// STS)模式下 SDK 会验证 2xx 响应完整性，测试服务器必须产出合法签名。
func signedResponse(w http.ResponseWriter, r *http.Request, accessKeyID, accessKeySecret string, body []byte) {
	sum := md5.Sum(body)
	headers := map[string]string{
		"x-ots-contentmd5":  base64.StdEncoding.EncodeToString(sum[:]),
		"x-ots-requestid":   "req-sts-1",
		"x-ots-date":        time.Now().UTC().Format("2006-01-02T15:04:05") + ".000Z",
		"x-ots-contenttype": "application/json",
	}
	lines := make([]string, 0, len(headers))
	for key, value := range headers {
		w.Header().Set(key, value)
		lines = append(lines, key+":"+value)
	}
	sort.Strings(lines)
	signDate := r.Header.Get("x-ots-signdate")
	region := r.Header.Get("x-ots-signregion")
	key := hmacSHA256([]byte("aliyun_v4"+accessKeySecret), signDate)
	key = hmacSHA256(key, region)
	key = hmacSHA256(key, "ots")
	key = hmacSHA256(key, "aliyun_v4_request")
	responseKey := []byte(base64.StdEncoding.EncodeToString(key))
	mac := hmac.New(sha1.New, responseKey)
	_, _ = mac.Write([]byte(strings.Join(lines, "\n") + "\n" + r.URL.Path))
	w.Header().Set("Authorization", "OTS "+accessKeyID+":"+base64.StdEncoding.EncodeToString(mac.Sum(nil)))
	_, _ = w.Write(body)
}

func hmacSHA256(key []byte, value string) []byte {
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte(value))
	return mac.Sum(nil)
}

func TestClientSTSTokenSignedRoundTrip(t *testing.T) {
	var gotHeaders http.Header
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHeaders = r.Header.Clone()
		signedResponse(w, r, "sts-ak", "sts-sk", []byte(`{"type":"memoryfile","itemId":"mem_a","path":"/a.md","contentSha256":"sha"}`))
	}))
	defer server.Close()

	c, err := New(Config{Endpoint: server.URL, Instance: "instance-a", Region: testRegion, AccessKeyID: "sts-ak", AccessKeySecret: "sts-sk", STSToken: "sts-token-1", StoreName: "memstore_a", SessionID: "session-a"})
	if err != nil {
		t.Fatal(err)
	}
	memory, err := c.Stat(context.Background(), "/a.md")
	if err != nil {
		t.Fatal(err)
	}
	if memory.MemoryID != "mem_a" {
		t.Fatalf("memory=%+v", memory)
	}
	if got := gotHeaders.Get("x-ots-ststoken"); got != "sts-token-1" {
		t.Fatalf("x-ots-ststoken=%q want sts-token-1", got)
	}
	if gotHeaders.Get("x-ots-accesskeyid") != "sts-ak" || gotHeaders.Get("x-ots-signaturev4") == "" || gotHeaders.Get("x-ots-signregion") != testRegion || gotHeaders.Get("x-ots-signdate") == "" {
		t.Fatalf("signature headers missing: %v", gotHeaders)
	}
	if gotHeaders.Get("x-ots-signature") != "" {
		t.Fatal("v4 mode must not send the v2 signature header")
	}
	if gotHeaders.Get("x-ots-apikey") != "" {
		t.Fatal("sts mode must not send x-ots-apikey")
	}
}

type expectedSTSCredential struct {
	secret string
	token  string
}

func newRotatingSTSServer(t *testing.T, credentials map[string]expectedSTSCredential, seen *[][2]string, mu *sync.Mutex) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		accessKeyID := r.Header.Get("x-ots-accesskeyid")
		current, ok := credentials[accessKeyID]
		if !ok || r.Header.Get("x-ots-ststoken") != current.token {
			w.WriteHeader(http.StatusForbidden)
			_ = json.NewEncoder(w).Encode(map[string]string{"code": "AUTH_FAILED", "message": "credential rejected"})
			return
		}
		if seen != nil {
			mu.Lock()
			*seen = append(*seen, [2]string{accessKeyID, current.token})
			mu.Unlock()
		}
		signedResponse(w, r, accessKeyID, current.secret, []byte(`{"type":"memoryfile","itemId":"mem_a","path":"/a.md","contentSha256":"sha"}`))
	}))
}

func TestClientRotatesCompleteSTSCredentialGeneration(t *testing.T) {
	credentials := map[string]expectedSTSCredential{
		"old-ak": {secret: "old-sk", token: "old-token"},
		"new-ak": {secret: "new-sk", token: "new-token"},
	}
	var mu sync.Mutex
	var seen [][2]string
	server := newRotatingSTSServer(t, credentials, &seen, &mu)
	defer server.Close()

	c, err := New(Config{Endpoint: server.URL, Instance: "instance-a", Region: testRegion, AccessKeyID: "old-ak", AccessKeySecret: "old-sk", STSToken: "old-token", StoreName: "memstore_a", SessionID: "session-a"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Stat(context.Background(), "/a.md"); err != nil {
		t.Fatal(err)
	}
	if err := c.UpdateSTSCredentials(credential.STS{AccessKeyID: "new-ak", AccessKeySecret: "new-sk", Token: "new-token"}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Stat(context.Background(), "/a.md"); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	want := [][2]string{{"old-ak", "old-token"}, {"new-ak", "new-token"}}
	if len(seen) != len(want) || seen[0] != want[0] || seen[1] != want[1] {
		t.Fatalf("seen=%v want=%v", seen, want)
	}
}

func TestClientRejectsSTSRotationForOtherAuthModes(t *testing.T) {
	for name, config := range map[string]Config{
		"api key": {Endpoint: "https://example", Instance: "i", APIKey: "key", StoreName: "s", SessionID: "x"},
		"aksk":    {Endpoint: "https://example", Instance: "i", Region: testRegion, AccessKeyID: "ak", AccessKeySecret: "sk", StoreName: "s", SessionID: "x"},
	} {
		t.Run(name, func(t *testing.T) {
			c, err := New(config)
			if err != nil {
				t.Fatal(err)
			}
			err = c.UpdateSTSCredentials(credential.STS{AccessKeyID: "new-ak", AccessKeySecret: "new-sk", Token: "new-token"})
			if !errors.Is(err, ErrSTSRotationUnsupported) {
				t.Fatalf("err=%v", err)
			}
		})
	}
}

func TestClientRejectedSTSRotationKeepsCurrentGeneration(t *testing.T) {
	credentials := map[string]expectedSTSCredential{
		"old-ak": {secret: "old-sk", token: "old-token"},
	}
	var mu sync.Mutex
	var seen [][2]string
	server := newRotatingSTSServer(t, credentials, &seen, &mu)
	defer server.Close()
	c, err := New(Config{Endpoint: server.URL, Instance: "instance-a", Region: testRegion, AccessKeyID: "old-ak", AccessKeySecret: "old-sk", STSToken: "old-token", StoreName: "memstore_a", SessionID: "session-a"})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.UpdateSTSCredentials(credential.STS{AccessKeyID: "new-ak", AccessKeySecret: "new-sk"}); err == nil {
		t.Fatal("expected incomplete credential to be rejected")
	}
	if _, err := c.Stat(context.Background(), "/a.md"); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 1 || seen[0] != [2]string{"old-ak", "old-token"} {
		t.Fatalf("seen=%v", seen)
	}
}

func TestClientConcurrentSTSRotationUsesCompleteGenerations(t *testing.T) {
	credentials := map[string]expectedSTSCredential{
		"ak-a": {secret: "sk-a", token: "token-a"},
		"ak-b": {secret: "sk-b", token: "token-b"},
	}
	server := newRotatingSTSServer(t, credentials, nil, nil)
	defer server.Close()
	c, err := New(Config{Endpoint: server.URL, Instance: "instance-a", Region: testRegion, AccessKeyID: "ak-a", AccessKeySecret: "sk-a", STSToken: "token-a", StoreName: "memstore_a", SessionID: "session-a"})
	if err != nil {
		t.Fatal(err)
	}

	var failed atomic.Bool
	var wg sync.WaitGroup
	start := make(chan struct{})
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for range 20 {
				if _, err := c.Stat(context.Background(), "/a.md"); err != nil {
					failed.Store(true)
					return
				}
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := range 100 {
			next := credential.STS{AccessKeyID: "ak-a", AccessKeySecret: "sk-a", Token: "token-a"}
			if i%2 == 1 {
				next = credential.STS{AccessKeyID: "ak-b", AccessKeySecret: "sk-b", Token: "token-b"}
			}
			if err := c.UpdateSTSCredentials(next); err != nil {
				failed.Store(true)
				return
			}
		}
	}()
	close(start)
	wg.Wait()
	if failed.Load() {
		t.Fatal("a concurrent request observed an invalid or mixed credential generation")
	}
}

func TestClientAuthLogOncePerCredentialGeneration(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_ = json.NewEncoder(w).Encode(map[string]string{"code": "AUTH_FAILED", "message": "credential rejected"})
	}))
	defer server.Close()
	c, err := New(Config{Endpoint: server.URL, Instance: "instance-a", Region: testRegion, AccessKeyID: "old-ak", AccessKeySecret: "old-sk", STSToken: "old-token", StoreName: "memstore_a", SessionID: "session-a"})
	if err != nil {
		t.Fatal(err)
	}

	var logs bytes.Buffer
	oldOutput := log.Writer()
	log.SetOutput(&logs)
	defer log.SetOutput(oldOutput)
	for range 2 {
		_, _ = c.Stat(context.Background(), "/a.md")
	}
	if err := c.UpdateSTSCredentials(credential.STS{AccessKeyID: "new-ak", AccessKeySecret: "new-sk", Token: "new-token"}); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		_, _ = c.Stat(context.Background(), "/a.md")
	}
	if got := strings.Count(logs.String(), "auth failed"); got != 2 {
		t.Fatalf("auth log count=%d want 2; logs=%q", got, logs.String())
	}
	for _, secret := range []string{"old-ak", "old-sk", "old-token", "new-ak", "new-sk", "new-token"} {
		if strings.Contains(logs.String(), secret) {
			t.Fatalf("log leaked credential %q", secret)
		}
	}
}

func TestClientConfigValidation(t *testing.T) {
	base := Config{Endpoint: "https://example", Instance: "i", StoreName: "s", SessionID: "x"}

	config := base
	if _, err := New(config); err == nil {
		t.Error("missing credentials must fail")
	}
	config = base
	config.STSToken = "token"
	if _, err := New(config); err == nil {
		t.Error("sts token without access keys must fail")
	}
	config = base
	config.APIKey = "k"
	config.AccessKeyID, config.AccessKeySecret = "a", "s"
	config.STSToken = "token"
	if _, err := New(config); err == nil {
		t.Error("sts token combined with api key must fail")
	}
	// SDK 拒绝明文 HTTP 携带 API key。
	config = Config{Endpoint: "http://example", Instance: "i", StoreName: "s", SessionID: "x", APIKey: "k"}
	if _, err := New(config); err == nil || !strings.Contains(err.Error(), "HTTPS") {
		t.Errorf("api key over http err=%v, want HTTPS requirement", err)
	}
	config = base
	config.AccessKeyID, config.AccessKeySecret = "ak", "sk"
	if _, err := New(config); err == nil || !strings.Contains(err.Error(), "region is required") {
		t.Errorf("signed credentials without region err=%v", err)
	}
}

// 校验的值必须等于发给 SDK 的值:此前 Instance/StoreName 按 TrimSpace 后判空却按
// 原值下发,带空白的实例名能通过入口校验、再到服务端变成查不到实例。
func TestClientNormalizesInstanceAndStoreBeforeSending(t *testing.T) {
	c, err := New(Config{Endpoint: "https://example/", Instance: "  inst  ", Region: "  " + testRegion + " ", AccessKeyID: "ak", AccessKeySecret: "sk", StoreName: "\tstore\n", SessionID: "x"})
	if err != nil {
		t.Fatal(err)
	}
	if c.sdkConfig.InstanceName != "inst" || c.sdkConfig.Region != testRegion {
		t.Errorf("instance=%q region=%q", c.sdkConfig.InstanceName, c.sdkConfig.Region)
	}
	if c.config.StoreName != "store" || c.scoped().MemoryStoreName != "store" {
		t.Errorf("store=%q scoped=%q", c.config.StoreName, c.scoped().MemoryStoreName)
	}
	for _, config := range []Config{
		{Endpoint: "https://example", Instance: "   ", Region: testRegion, AccessKeyID: "ak", AccessKeySecret: "sk", StoreName: "s", SessionID: "x"},
		{Endpoint: "https://example", Instance: "i", Region: testRegion, AccessKeyID: "ak", AccessKeySecret: "sk", StoreName: " \t ", SessionID: "x"},
	} {
		if _, err := New(config); err == nil {
			t.Errorf("blank instance/store must fail: %+v", config)
		}
	}
}

// 「固定」而非「默认」:没有任何入口能退回 v2。
func TestClientAlwaysUsesV4Signing(t *testing.T) {
	c, err := New(Config{Endpoint: "https://example", Instance: "i", Region: testRegion, AccessKeyID: "ak", AccessKeySecret: "sk", StoreName: "s", SessionID: "x"})
	if err != nil {
		t.Fatal(err)
	}
	if c.sdkConfig.SignVersion != agentstorage.SignV4 || c.sdkConfig.Region != testRegion {
		t.Fatalf("signVersion=%q region=%q", c.sdkConfig.SignVersion, c.sdkConfig.Region)
	}
}

func TestClientRequiresHTTPSForRemoteEndpoints(t *testing.T) {
	for _, endpoint := range []string{"http://example.com", "http://127.0.0.1.example.com", "ftp://example.com", "example.com"} {
		config := Config{Endpoint: endpoint, Instance: "i", Region: testRegion, AccessKeyID: "ak", AccessKeySecret: "sk", StoreName: "s", SessionID: "x"}
		if _, err := New(config); err == nil {
			t.Errorf("endpoint %q must be rejected", endpoint)
		}
	}
}

func TestClientAllowsHTTPOnlyForLoopbackTesting(t *testing.T) {
	for _, endpoint := range []string{
		"http://localhost:8080",
		"http://LOCALHOST:8080",
		"http://127.0.0.1:8080",
		"http://127.0.0.2:8080",
		"http://[::1]:8080",
		"http://[0:0:0:0:0:0:0:1]:8080",
		"http://[::ffff:127.0.0.1]:8080",
		"http://[::ffff:7f00:1]:8080",
	} {
		config := Config{Endpoint: endpoint, Instance: "i", Region: testRegion, AccessKeyID: "ak", AccessKeySecret: "sk", StoreName: "s", SessionID: "x"}
		if _, err := New(config); err != nil {
			t.Errorf("endpoint %q: %v", endpoint, err)
		}
	}
}

func TestClientRemoteHTTPRequiresExplicitOptIn(t *testing.T) {
	config := Config{Endpoint: "http://192.0.2.1", Instance: "i", Region: testRegion, AccessKeyID: "ak", AccessKeySecret: "sk", StoreName: "s", SessionID: "x"}
	if _, err := New(config); err == nil {
		t.Fatal("remote HTTP must be rejected by default")
	}

	config.AllowInsecureHTTP = true
	if _, err := New(config); err != nil {
		t.Fatalf("remote HTTP with explicit opt-in: %v", err)
	}
}

func TestClientRemoteHTTPAPIKeyRequiresBothOptIns(t *testing.T) {
	config := Config{
		Endpoint: "http://192.0.2.1", Instance: "i", APIKey: "test-key", StoreName: "s", SessionID: "x",
		AllowInsecureHTTP: true,
	}
	if _, err := New(config); err == nil {
		t.Fatal("API key over remote HTTP must still require AllowInsecureAPIKey")
	}

	config.AllowInsecureHTTP = false
	config.AllowInsecureAPIKey = true
	if _, err := New(config); err == nil {
		t.Fatal("AllowInsecureAPIKey must not bypass the remote HTTP endpoint opt-in")
	}

	config.AllowInsecureHTTP = true
	if _, err := New(config); err != nil {
		t.Fatalf("API key over remote HTTP with both explicit opt-ins: %v", err)
	}
}

func TestClientRejectsRedirectBeforeForwardingCredentials(t *testing.T) {
	var targetCalled atomic.Bool
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		targetCalled.Store(true)
	}))
	defer target.Close()

	source := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Location", target.URL)
		w.WriteHeader(http.StatusTemporaryRedirect)
	}))
	defer source.Close()

	config := newAPIKeyConfig(source)
	c, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Stat(context.Background(), "/a.md"); err == nil || !strings.Contains(err.Error(), "redirects are not allowed") {
		t.Fatalf("err=%v", err)
	}
	if targetCalled.Load() {
		t.Fatal("redirect target received credentials")
	}
}

func TestClientDoesNotRetainCredentialsInFacadeConfig(t *testing.T) {
	configs := []Config{
		{Endpoint: "https://example", Instance: "i", APIKey: "api-secret", StoreName: "s", SessionID: "x"},
		{Endpoint: "https://example", Instance: "i", Region: testRegion, AccessKeyID: "sts-ak", AccessKeySecret: "sts-sk", STSToken: "sts-token", StoreName: "s", SessionID: "x"},
	}
	for _, config := range configs {
		c, err := New(config)
		if err != nil {
			t.Fatal(err)
		}
		if c.config.APIKey != "" || c.config.AccessKeyID != "" || c.config.AccessKeySecret != "" || c.config.STSToken != "" {
			t.Fatalf("facade config retained authentication material: %+v", c.config)
		}
	}
}

func TestClientRetriesOnThrottleThenSucceeds(t *testing.T) {
	attempts := 0
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts++
		if attempts <= 2 {
			w.WriteHeader(http.StatusTooManyRequests)
			_ = json.NewEncoder(w).Encode(map[string]string{"code": "THROTTLED", "message": "slow down"})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"type": "memoryfile", "itemId": "mem_a", "path": "/a.md", "contentSha256": "sha"})
	}))
	defer server.Close()

	config := newAPIKeyConfig(server)
	config.RetryBackoff = []time.Duration{time.Millisecond, time.Millisecond}
	c, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	memory, err := c.Stat(context.Background(), "/a.md")
	if err != nil || memory.MemoryID != "mem_a" {
		t.Fatalf("memory=%+v err=%v", memory, err)
	}
	if attempts != 3 {
		t.Fatalf("attempts=%d want 3", attempts)
	}
}

func TestReadAndMutationRetryableClassifyServiceErrors(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
		code       string
		wantRead   bool
		wantWrite  bool
	}{
		{name: "too many requests", statusCode: http.StatusTooManyRequests, code: "THROTTLED", wantRead: true, wantWrite: true},
		{name: "internal server error", statusCode: http.StatusInternalServerError, code: "INTERNAL_ERROR", wantRead: true},
		{name: "service unavailable", statusCode: http.StatusServiceUnavailable, code: "UNAVAILABLE", wantRead: true},
		{name: "gateway timeout", statusCode: http.StatusGatewayTimeout, code: "GATEWAY_TIMEOUT", wantRead: true},
		{name: "lock conflict", statusCode: http.StatusConflict, code: "LOCK_CONFLICT", wantRead: true},
		{name: "bad request", statusCode: http.StatusBadRequest, code: "INVALID_PARAMETER"},
		{name: "unauthorized", statusCode: http.StatusUnauthorized, code: "UNAUTHORIZED"},
		{name: "not found", statusCode: http.StatusNotFound, code: "NOT_FOUND"},
		{name: "path exists", statusCode: http.StatusConflict, code: "PATH_EXISTS"},
		{name: "precondition failed", statusCode: http.StatusConflict, code: "PRECONDITION_FAILED"},
		{name: "quota exceeded", statusCode: http.StatusConflict, code: "QUOTA_EXCEEDED"},
		{name: "other server error", statusCode: http.StatusBadGateway, code: "BAD_GATEWAY"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			apiErr := &APIError{StatusCode: tt.statusCode, Code: tt.code}
			if got := readRetryable(apiErr); got != tt.wantRead {
				t.Fatalf("readRetryable(%d, %q) = %t, want %t", tt.statusCode, tt.code, got, tt.wantRead)
			}
			if got := mutationRetryable(apiErr); got != tt.wantWrite {
				t.Fatalf("mutationRetryable(%d, %q) = %t, want %t", tt.statusCode, tt.code, got, tt.wantWrite)
			}
		})
	}
}

func TestClientRetriesOnInternalServerErrorThenSucceeds(t *testing.T) {
	attempts := 0
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts++
		if attempts == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]string{"code": "INTERNAL_ERROR", "message": "try again"})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"type": "memoryfile", "itemId": "mem_a", "path": "/a.md", "contentSha256": "sha"})
	}))
	defer server.Close()

	config := newAPIKeyConfig(server)
	config.RetryBackoff = []time.Duration{time.Millisecond}
	c, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	memory, err := c.Stat(context.Background(), "/a.md")
	if err != nil || memory.MemoryID != "mem_a" {
		t.Fatalf("memory=%+v err=%v", memory, err)
	}
	if attempts != 2 {
		t.Fatalf("attempts=%d want 2", attempts)
	}
}

func TestClientMutationsDoNotReplayAmbiguousInternalServerError(t *testing.T) {
	tests := []struct {
		name   string
		action string
		call   func(context.Context, *Client) error
	}{
		{
			name:   "create",
			action: "/AddItem",
			call: func(ctx context.Context, c *Client) error {
				_, err := c.Create(ctx, "/a.md", []byte("content"))
				return err
			},
		},
		{
			name:   "update",
			action: "/UpdateItem",
			call: func(ctx context.Context, c *Client) error {
				_, err := c.Update(ctx, "mem_a", []byte("content"), "fmver_1")
				return err
			},
		},
		{
			name:   "rename",
			action: "/UpdateItem",
			call: func(ctx context.Context, c *Client) error {
				_, err := c.Rename(ctx, "mem_a", "/b.md", false, "fmver_1")
				return err
			},
		},
		{
			name:   "delete",
			action: "/DeleteItem",
			call: func(ctx context.Context, c *Client) error {
				return c.Delete(ctx, "mem_a", "fmver_1")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var actions []string
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				actions = append(actions, r.URL.Path)
				switch r.URL.Path {
				case tt.action:
					w.WriteHeader(http.StatusInternalServerError)
					_ = json.NewEncoder(w).Encode(map[string]string{
						"code": "INTERNAL_ERROR", "message": "ambiguous mutation result", "requestId": "req-500",
					})
				case "/GetItem":
					w.WriteHeader(http.StatusServiceUnavailable)
					_ = json.NewEncoder(w).Encode(map[string]string{
						"code": "UNAVAILABLE", "message": "recheck unavailable", "requestId": "req-get",
					})
				default:
					t.Fatalf("unexpected action %q", r.URL.Path)
				}
			}))
			defer server.Close()

			config := newAPIKeyConfig(server)
			config.RetryBackoff = []time.Duration{}
			c, err := New(config)
			if err != nil {
				t.Fatal(err)
			}
			err = tt.call(context.Background(), c)
			var apiErr *APIError
			if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusInternalServerError || apiErr.Code != "INTERNAL_ERROR" || apiErr.RequestID != "req-500" {
				t.Fatalf("error = %v, want structured original 500", err)
			}
			if want := []string{tt.action, "/GetItem"}; !reflect.DeepEqual(actions, want) {
				t.Fatalf("actions = %v, want %v", actions, want)
			}
		})
	}
}

func TestClientSafeReadsRetryInternalServerError(t *testing.T) {
	tests := []struct {
		name   string
		action string
		call   func(context.Context, *Client) error
	}{
		{
			name:   "list",
			action: "/ListItems",
			call: func(ctx context.Context, c *Client) error {
				_, err := c.List(ctx, "", "", 10)
				return err
			},
		},
		{
			name:   "read",
			action: "/GetItem",
			call: func(ctx context.Context, c *Client) error {
				memory, err := c.Read(ctx, "/a.md")
				if err == nil && memory.Content != "hello" {
					return fmt.Errorf("content = %q, want hello", memory.Content)
				}
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			attempts := 0
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				attempts++
				if r.URL.Path != tt.action {
					t.Errorf("action = %q, want %q", r.URL.Path, tt.action)
				}
				if attempts == 1 {
					w.WriteHeader(http.StatusInternalServerError)
					_ = json.NewEncoder(w).Encode(map[string]string{"code": "INTERNAL_ERROR", "message": "try again"})
					return
				}
				if tt.action == "/ListItems" {
					_ = json.NewEncoder(w).Encode(map[string]any{"type": "memoryfile", "items": []any{}, "nextToken": ""})
					return
				}
				_ = json.NewEncoder(w).Encode(map[string]any{
					"type": "memoryfile", "itemId": "mem_a", "path": "/a.md", "content": "hello", "contentSha256": "sha",
				})
			}))
			defer server.Close()

			config := newAPIKeyConfig(server)
			config.RetryBackoff = []time.Duration{time.Nanosecond}
			c, err := New(config)
			if err != nil {
				t.Fatal(err)
			}
			if err := tt.call(context.Background(), c); err != nil {
				t.Fatal(err)
			}
			if attempts != 2 {
				t.Fatalf("attempts = %d, want 2", attempts)
			}
		})
	}
}

func TestClientMutationDoesNotReplayServiceUnavailable(t *testing.T) {
	var actions []string
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		actions = append(actions, r.URL.Path)
		switch r.URL.Path {
		case "/DeleteItem":
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(map[string]string{"code": "UNAVAILABLE", "message": "ambiguous", "requestId": "req-503"})
		case "/GetItem":
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(map[string]string{"code": "NOT_FOUND", "message": "gone"})
		default:
			t.Fatalf("unexpected action %q", r.URL.Path)
		}
	}))
	defer server.Close()

	config := newAPIKeyConfig(server)
	config.RetryBackoff = []time.Duration{time.Nanosecond}
	c, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Delete(context.Background(), "mem_a", "fmver_1"); err != nil {
		t.Fatal(err)
	}
	if want := []string{"/DeleteItem", "/GetItem"}; !reflect.DeepEqual(actions, want) {
		t.Fatalf("actions = %v, want %v", actions, want)
	}
}

func TestClientRetryBudgetExhausted(t *testing.T) {
	attempts := 0
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts++
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]string{"code": "UNAVAILABLE", "message": "down"})
	}))
	defer server.Close()

	config := newAPIKeyConfig(server)
	config.RetryBackoff = []time.Duration{time.Millisecond, time.Millisecond}
	c, _ := New(config)
	_, err := c.Stat(context.Background(), "/a.md")
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("err=%v", err)
	}
	if attempts != 3 {
		t.Fatalf("attempts=%d want 3 (1 initial + 2 retries)", attempts)
	}
}

func TestClientDisambiguatesMutationConflictByAuthoritativeUpdateState(t *testing.T) {
	tests := []struct {
		name           string
		currentVersion string
		currentContent string
		getStatus      int
		getCode        string
		wantCode       string
		wantSuccess    bool
	}{
		{name: "same version and target content no-op", currentVersion: "fmver_old", currentContent: "new", wantSuccess: true},
		{name: "changed version and target content", currentVersion: "fmver_new", currentContent: "new", wantSuccess: true},
		{name: "unchanged content and version", currentVersion: "fmver_old", currentContent: "old", wantCode: "LOCK_CONFLICT"},
		{name: "changed version and other content", currentVersion: "fmver_new", currentContent: "other", wantCode: "PRECONDITION_FAILED"},
		{name: "missing item", getStatus: http.StatusNotFound, getCode: "NOT_FOUND", wantCode: "PRECONDITION_FAILED"},
		{name: "recheck failure", getStatus: http.StatusInternalServerError, wantCode: "LOCK_CONFLICT"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var calls []recordedCall
			server := newRecordingServer(t, &calls, func(action string, w http.ResponseWriter) {
				switch action {
				case "/UpdateItem":
					w.WriteHeader(http.StatusConflict)
					_ = json.NewEncoder(w).Encode(map[string]string{
						"code": "LOCK_CONFLICT", "message": "mutation conflict", "requestId": "mutation-request",
					})
				case "/GetItem":
					if test.getStatus != 0 {
						w.WriteHeader(test.getStatus)
						_ = json.NewEncoder(w).Encode(map[string]string{
							"code": test.getCode, "message": "metadata recheck failed", "requestId": "recheck-request",
						})
						return
					}
					_ = json.NewEncoder(w).Encode(map[string]any{
						"type": "memoryfile", "itemId": "mem_a",
						"scope": map[string]any{
							"appId": "app-a", "tenantId": "tenant-a", "agentId": "agent-a", "runId": "run-a",
						},
						"path": "/a.md", "content": test.currentContent, "contentSha256": "sha", "contentSizeBytes": len([]byte(test.currentContent)),
						"latestSeq": 2, "latestVersionId": test.currentVersion,
						"createdAt": "2026-07-01T01:02:03Z", "updatedAt": "2026-07-01T02:03:04Z",
					})
				default:
					t.Fatalf("unexpected action %q", action)
				}
			})

			config := newAPIKeyConfig(server)
			config.Scope = Scope{AppID: "app-a", TenantID: "tenant-a", AgentID: "agent-a", RunID: "run-a"}
			config.RetryBackoff = []time.Duration{}
			c, err := New(config)
			if err != nil {
				t.Fatal(err)
			}
			memory, err := c.Update(context.Background(), "mem_a", []byte("new"), "fmver_old")
			if test.wantSuccess {
				if err != nil || memory.MemoryID != "mem_a" || memory.Content != "new" || memory.LatestVersionID != test.currentVersion {
					t.Fatalf("memory=%+v err=%v", memory, err)
				}
			} else {
				var apiErr *APIError
				if !errors.As(err, &apiErr) || apiErr.Code != test.wantCode {
					t.Fatalf("err=%v, want API code %s", err, test.wantCode)
				}
				if apiErr.RequestID != "mutation-request" {
					t.Fatalf("requestID=%q want mutation-request", apiErr.RequestID)
				}
				if memory != (Memory{}) {
					t.Fatalf("memory=%+v want zero on failure", memory)
				}
			}

			wantScope := map[string]any{"appId": "app-a", "tenantId": "tenant-a", "agentId": "agent-a", "runId": "run-a"}
			wantCalls := []recordedCall{
				{action: "/UpdateItem", body: map[string]any{
					"type": "memoryfile", "memoryStoreName": "memstore_a", "scope": wantScope,
					"itemId": "mem_a", "content": "new", "precondition": map[string]any{"expectedVersionId": "fmver_old"},
					"sessionId": "session-a", "view": "full",
				}},
				{action: "/GetItem", body: map[string]any{
					"type": "memoryfile", "memoryStoreName": "memstore_a", "scope": wantScope, "itemId": "mem_a", "view": "full",
				}},
			}
			if !reflect.DeepEqual(calls, wantCalls) {
				t.Errorf("recorded calls:\n got: %#v\nwant: %#v", calls, wantCalls)
			}
		})
	}
}

func TestClientDoesNotRetryNonRetryableStatus(t *testing.T) {
	// 安全读在 409 中只重试 LOCK_CONFLICT；其余 409 code（含裸 CONFLICT、
	// CAS 失败、路径占用、配额满）必须一次返回。mutation 的 LOCK_CONFLICT
	// 也不重放 write，而是由 operation-specific authoritative Get 收敛。
	for _, code := range []string{"CONFLICT", "SHA_MISMATCH", "PATH_EXISTS", "QUOTA_EXCEEDED", "READ_ONLY_STORE"} {
		attempts := 0
		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			attempts++
			w.WriteHeader(http.StatusConflict)
			_ = json.NewEncoder(w).Encode(map[string]string{"code": code, "message": "conflict"})
		}))

		config := newAPIKeyConfig(server)
		config.RetryBackoff = []time.Duration{time.Millisecond, time.Millisecond}
		c, _ := New(config)
		_, err := c.Stat(context.Background(), "/a.md")
		var apiErr *APIError
		if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusConflict || apiErr.Code != code {
			t.Fatalf("%s: err=%v", code, err)
		}
		if attempts != 1 {
			t.Fatalf("%s: attempts=%d want 1", code, attempts)
		}
		server.Close()
	}
}

func TestClientReconcilesLockConflictWithoutReplayingMutation(t *testing.T) {
	var actions []string
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		actions = append(actions, r.URL.Path)
		switch r.URL.Path {
		case "/UpdateItem":
			w.WriteHeader(http.StatusConflict)
			_ = json.NewEncoder(w).Encode(map[string]string{
				"code": "LOCK_CONFLICT", "message": "concurrent mutation in flight", "requestId": "req-lock",
			})
		case "/GetItem":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"type": "memoryfile", "itemId": "mem_a", "path": "/a.md", "content": "v2",
				"contentSha256": "sha-v2", "contentSizeBytes": 2, "latestSeq": 2, "latestVersionId": "opaque-after",
				"createdAt": "2026-08-24T00:00:00Z", "updatedAt": "2026-08-24T00:00:01Z",
			})
		default:
			t.Fatalf("unexpected action %q", r.URL.Path)
		}
	}))
	defer server.Close()

	config := newAPIKeyConfig(server)
	config.RetryBackoff = []time.Duration{time.Millisecond, time.Millisecond}
	c, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	memory, err := c.Update(context.Background(), "mem_a", []byte("v2"), "opaque-before")
	if err != nil || memory.MemoryID != "mem_a" {
		t.Fatalf("memory=%+v err=%v", memory, err)
	}
	if want := []string{"/UpdateItem", "/GetItem"}; !reflect.DeepEqual(actions, want) {
		t.Fatalf("actions = %v, want %v", actions, want)
	}
}

func TestClientHTTPTimeoutConfigurable(t *testing.T) {
	c, err := New(Config{Endpoint: "https://example", Instance: "i", APIKey: "k", StoreName: "s", SessionID: "x", HTTPTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if c.config.HTTPClient.Timeout != 5*time.Second {
		t.Fatalf("timeout=%v want 5s", c.config.HTTPClient.Timeout)
	}
	c, err = New(Config{Endpoint: "https://example", Instance: "i", APIKey: "k", StoreName: "s", SessionID: "x"})
	if err != nil {
		t.Fatal(err)
	}
	if c.config.HTTPClient.Timeout != 30*time.Second {
		t.Fatalf("default timeout=%v want 30s", c.config.HTTPClient.Timeout)
	}
}

func TestClientCapsRunawayResponseBody(t *testing.T) {
	// 失控网关返回超大 body 时必须在 maxResponseBytes 截断成解析错误,
	// 不能被 SDK 的无界 io.ReadAll 全量读进内存。
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"itemId":"`))
		junk := strings.Repeat("a", 1<<20)
		for range 6 {
			_, _ = w.Write([]byte(junk))
		}
		_, _ = w.Write([]byte(`"}`))
	}))
	defer server.Close()
	c, err := New(newAPIKeyConfig(server))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Stat(context.Background(), "/a.md"); err == nil {
		t.Fatal("expected truncated-response error for oversized body")
	}
}

func TestClientReturnsStructuredAPIError(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("x-ots-requestid", "req-9")
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(map[string]string{"code": "QUOTA_EXCEEDED", "message": "file quota exceeded"})
	}))
	defer server.Close()
	c, _ := New(newAPIKeyConfig(server))
	_, err := c.Stat(context.Background(), "/a")
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusConflict || apiErr.Code != "QUOTA_EXCEEDED" || apiErr.Message != "file quota exceeded" {
		t.Fatalf("err=%v", err)
	}
	if apiErr.RequestID != "req-9" {
		t.Fatalf("requestID=%q want req-9", apiErr.RequestID)
	}
}

func TestSDKClientErrorKindExtractsWrappedKnownKind(t *testing.T) {
	sdkClient, err := agentstorage.NewClient(agentstorage.Config{
		Endpoint:     "https://[::1",
		InstanceName: "test-instance",
		APIKey:       "test-api-key",
	})
	if err != nil {
		t.Fatal(err)
	}
	err = sdkClient.Do(context.Background(), "TestAction", map[string]any{}, nil)
	if err == nil {
		t.Fatal("SDK request unexpectedly succeeded")
	}

	if got := SDKClientErrorKind(fmt.Errorf("wrapped: %w", err)); got != "request_build" {
		t.Fatalf("SDKClientErrorKind()=%q want request_build", got)
	}
}

func TestSDKClientErrorKindDropsUnknownAndNonClientErrors(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{name: "historical unknown", err: &agentstorage.ClientError{Message: "raw-client-message-sentinel"}},
		{name: "service error", err: &agentstorage.ServiceError{StatusCode: http.StatusInternalServerError, Message: "raw-service-message-sentinel"}},
		{name: "ordinary error", err: errors.New("raw-ordinary-message-sentinel")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := SDKClientErrorKind(test.err); got != "" {
				t.Fatalf("SDKClientErrorKind()=%q want empty", got)
			}
		})
	}
}

func TestServicePathHelpersNormalizeUnicodeToNFC(t *testing.T) {
	const nfd = "notes/cafe\u0301.md"
	const nfc = "notes/caf\u00e9.md"
	if got := serviceItemPath(nfd); got != "/"+nfc {
		t.Fatalf("serviceItemPath(%q)=%q want %q", nfd, got, "/"+nfc)
	}
	if got := servicePathPrefix(nfd); got != "/"+nfc+"/" {
		t.Fatalf("servicePathPrefix(%q)=%q want %q", nfd, got, "/"+nfc+"/")
	}
}

func TestClientValidationFailIsNotLoggedAsAuthFailure(t *testing.T) {
	// 403 OTSValidationFail 是参数/契约校验失败（如 limit 超限、新旧契约
	// 不匹配），不是凭证问题；auth failed 日志会误导排查方向。
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_ = json.NewEncoder(w).Encode(map[string]string{"code": "OTSValidationFail", "message": "limit must be between 1 and 100"})
	}))
	defer server.Close()
	c, err := New(Config{Endpoint: server.URL, Instance: "instance-a", Region: testRegion, AccessKeyID: "ak", AccessKeySecret: "sk", StoreName: "memstore_a", SessionID: "session-a"})
	if err != nil {
		t.Fatal(err)
	}

	var logs bytes.Buffer
	oldOutput := log.Writer()
	log.SetOutput(&logs)
	defer log.SetOutput(oldOutput)
	if _, err := c.Stat(context.Background(), "/a.md"); err == nil {
		t.Fatal("expected validation failure")
	}
	if strings.Contains(logs.String(), "auth failed") {
		t.Fatalf("OTSValidationFail must not log auth failed; logs=%q", logs.String())
	}

	// 真鉴权失败仍要打：同 server 换 code 后再验一次正向路径。
	authServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_ = json.NewEncoder(w).Encode(map[string]string{"code": "OTSAuthFailed", "message": "signature mismatch"})
	}))
	defer authServer.Close()
	c2, err := New(Config{Endpoint: authServer.URL, Instance: "instance-a", Region: testRegion, AccessKeyID: "ak", AccessKeySecret: "sk", StoreName: "memstore_a", SessionID: "session-a"})
	if err != nil {
		t.Fatal(err)
	}
	logs.Reset()
	if _, err := c2.Stat(context.Background(), "/a.md"); err == nil {
		t.Fatal("expected auth failure")
	}
	if !strings.Contains(logs.String(), "auth failed") {
		t.Fatalf("OTSAuthFailed must still log auth failed; logs=%q", logs.String())
	}
}

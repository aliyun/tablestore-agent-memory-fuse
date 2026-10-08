// mockfilemem 是 Agent Memory 服务端 Item API 的有状态内存 mock，供容器内
// localhost 端到端测试 agent-memory-fuse 使用。实现最终 Item 合约的五个 RPC（ListItems/GetItem/
// AddItem/UpdateItem/DeleteItem）+ V4 请求验签与响应签名，外加 __ 前缀的
// GET 控制端点做故障注入与服务端状态直改。
package main

import (
	"bytes"
	"crypto/hmac"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const maxContentBytes = 100 * 1024

const itemType = "memoryfile"

const defaultMemoryStoreName = "memstore_t"

var defaultItemScope = itemScope{AppID: "app", TenantID: "tenant", AgentID: "agent", RunID: "run"}

// item 的 Content 恒为具体值；响应侧由 view 决定是否省略 content 字段，用
// *string 区分 basic view 与 full view 中的空文件。
type item struct {
	ItemID           string
	Path             string
	Content          string
	ContentSHA256    string
	ContentSizeBytes int64
	LatestSeq        int64
	LatestVersionID  string
	CreatedAt        string
	UpdatedAt        string
}

type itemJSON struct {
	Type             string          `json:"type,omitempty"`
	ItemID           string          `json:"itemId"`
	Scope            json.RawMessage `json:"scope"`
	Path             string          `json:"path"`
	Content          *string         `json:"content,omitempty"`
	ContentSHA256    string          `json:"contentSha256"`
	ContentSizeBytes int64           `json:"contentSizeBytes"`
	LatestSeq        int64           `json:"latestSeq"`
	LatestVersionID  string          `json:"latestVersionId"`
	CreatedAt        string          `json:"createdAt"`
	UpdatedAt        string          `json:"updatedAt"`
}

func render(f *item, scope json.RawMessage, includeContent bool) itemJSON {
	j := itemJSON{
		Type: itemType, ItemID: f.ItemID, Scope: append(json.RawMessage(nil), scope...), Path: f.Path,
		ContentSHA256: f.ContentSHA256, ContentSizeBytes: f.ContentSizeBytes,
		LatestSeq: f.LatestSeq, LatestVersionID: f.LatestVersionID,
		CreatedAt: f.CreatedAt, UpdatedAt: f.UpdatedAt,
	}
	if includeContent {
		content := f.Content
		j.Content = &content
	}
	return j
}

type fault struct {
	Action string
	Status int
	Code   string
	Times  int
}

type mockCredential struct {
	secret string
	token  string
}

type mock struct {
	mu            sync.Mutex
	items         map[string]*item
	seq           int64
	idc           int64
	maxFiles      int
	readOnly      bool
	faults        []*fault
	credentials   map[string]mockCredential
	region        string
	lastAuthAK    string
	lastAuthToken string
	expectedStore string
	expectedScope itemScope
	cursors       map[string]listCursor
}

func now() string { return time.Now().UTC().Format(time.RFC3339Nano) }

func shaOf(content string) string {
	sum := sha256.Sum256([]byte(content))
	return hex.EncodeToString(sum[:])
}

// signedResponse 与 agent-memory-fuse client_test.go 的同名 helper 一致：按请求
// 携带的 V4 date/region 派生响应签名密钥。调用前 handleRPC 已校验过 signdate 非空
// 且 signregion 与 mock 自持的 region 相符，所以这里直接取头即可。
func signedResponse(w http.ResponseWriter, r *http.Request, ak, sk string, body []byte) {
	sum := md5.Sum(body)
	headers := map[string]string{
		"x-ots-contentmd5":  base64.StdEncoding.EncodeToString(sum[:]),
		"x-ots-requestid":   fmt.Sprintf("mock-%d", time.Now().UnixNano()),
		"x-ots-date":        time.Now().UTC().Format("2006-01-02T15:04:05") + ".000Z",
		"x-ots-contenttype": "application/json",
	}
	lines := make([]string, 0, len(headers))
	for key, value := range headers {
		w.Header().Set(key, value)
		lines = append(lines, key+":"+value)
	}
	sort.Strings(lines)
	key := v4SigningKey(sk, r.Header.Get("x-ots-signdate"), r.Header.Get("x-ots-signregion"))
	mac := hmac.New(sha1.New, key)
	_, _ = mac.Write([]byte(strings.Join(lines, "\n") + "\n" + r.URL.Path))
	w.Header().Set("Authorization", "OTS "+ak+":"+base64.StdEncoding.EncodeToString(mac.Sum(nil)))
	_, _ = w.Write(body)
}

func hmacSHA256(key []byte, value string) []byte {
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte(value))
	return mac.Sum(nil)
}

func v4SigningKey(secret, signDate, region string) []byte {
	key := hmacSHA256([]byte("aliyun_v4"+secret), signDate)
	key = hmacSHA256(key, region)
	key = hmacSHA256(key, "ots")
	key = hmacSHA256(key, "aliyun_v4_request")
	return []byte(base64.StdEncoding.EncodeToString(key))
}

func validateV4Request(r *http.Request, body []byte, secret, region string) error {
	signDate, signRegion := r.Header.Get("x-ots-signdate"), r.Header.Get("x-ots-signregion")
	if signDate == "" || signRegion != region {
		return fmt.Errorf("v4 signature headers rejected: signDate=%q signRegion=%q serverRegion=%q", signDate, signRegion, region)
	}
	if r.URL.RawQuery != "" {
		return fmt.Errorf("unexpected query string in Item RPC")
	}
	sum := md5.Sum(body)
	if got, want := r.Header.Get("x-ots-contentmd5"), base64.StdEncoding.EncodeToString(sum[:]); got != want {
		return fmt.Errorf("request content MD5 mismatch")
	}

	var headers []string
	for name, values := range r.Header {
		name = strings.ToLower(name)
		if strings.HasPrefix(name, "x-ots-") && name != "x-ots-signaturev4" && len(values) > 0 {
			headers = append(headers, name+":"+strings.TrimSpace(values[0]))
		}
	}
	sort.Strings(headers)
	canonical := r.URL.Path + "\nPOST\n\n" + strings.Join(headers, "\n") + "\nots"
	mac := hmac.New(sha256.New, v4SigningKey(secret, signDate, signRegion))
	_, _ = mac.Write([]byte(canonical))
	want := base64.StdEncoding.EncodeToString(mac.Sum(nil))
	if got := r.Header.Get("x-ots-signaturev4"); !hmac.Equal([]byte(got), []byte(want)) {
		return fmt.Errorf("invalid v4 request signature")
	}
	return nil
}

func (m *mock) ok(w http.ResponseWriter, r *http.Request, payload any) {
	body, err := json.Marshal(payload)
	if err != nil {
		panic(err)
	}
	accessKeyID := r.Header.Get("x-ots-accesskeyid")
	credential := m.credentials[accessKeyID]
	signedResponse(w, r, accessKeyID, credential.secret, body)
}

func (m *mock) fail(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"code": code, "message": message, "requestId": "mock-err"})
}

// takeFault 消耗一条匹配的注入故障；action 为空表示匹配所有 RPC。
func (m *mock) takeFault(action string) *fault {
	for i, f := range m.faults {
		if f.Action == "" || f.Action == action {
			f.Times--
			if f.Times <= 0 {
				m.faults = append(m.faults[:i], m.faults[i+1:]...)
			}
			return f
		}
	}
	return nil
}

func (m *mock) nextID() string {
	m.idc++
	return fmt.Sprintf("item_%06d", m.idc)
}

func canonicalItemPath(path string) string {
	return "/" + strings.TrimLeft(path, "/")
}

func (m *mock) byID(itemID string) *item {
	for _, f := range m.items {
		if f.ItemID == itemID {
			return f
		}
	}
	return nil
}

func (m *mock) advanceVersion(f *item) {
	m.seq++
	f.LatestSeq = m.seq
	f.LatestVersionID = fmt.Sprintf("fmver_%020d_%s", m.seq, f.ItemID)
	f.UpdatedAt = now()
}

func (m *mock) put(path, content string, isNew bool) *item {
	path = canonicalItemPath(path)
	f, exists := m.items[path]
	if !exists || isNew {
		f = &item{ItemID: m.nextID(), Path: path, CreatedAt: now()}
		m.items[path] = f
	}
	f.Content = content
	f.ContentSHA256 = shaOf(content)
	f.ContentSizeBytes = int64(len(content))
	m.advanceVersion(f)
	return f
}

type itemPrecondition struct {
	ExpectedVersionID string `json:"expectedVersionId"`
}

type itemScope struct {
	AppID    string `json:"appId"`
	TenantID string `json:"tenantId"`
	AgentID  string `json:"agentId"`
	RunID    string `json:"runId"`
}

type listCursor struct {
	MemoryStoreName string
	Scope           string
	PathPrefix      string
	View            string
	Depth           int
	LastPath        string
}

type request struct {
	Type            string            `json:"type"`
	MemoryStoreName string            `json:"memoryStoreName"`
	Scope           json.RawMessage   `json:"scope"`
	ItemID          string            `json:"itemId"`
	Path            string            `json:"path"`
	Content         *string           `json:"content"`
	Precondition    *itemPrecondition `json:"precondition"`
	Overwrite       bool              `json:"overwrite"`
	SessionID       string            `json:"sessionId"`
	View            string            `json:"view"`
	PathPrefix      string            `json:"pathPrefix"`
	NextToken       string            `json:"nextToken"`
	Limit           int               `json:"limit"`
	Depth           *int              `json:"depth"`
	presentFields   map[string]struct{}
}

func decodeRequest(body []byte) (request, error) {
	var req request
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return request{}, fmt.Errorf("request body must contain exactly one JSON object")
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		return request{}, err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return request{}, fmt.Errorf("request body must contain exactly one JSON object")
		}
		return request{}, err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(body, &fields); err != nil {
		return request{}, err
	}
	req.presentFields = make(map[string]struct{}, len(fields))
	for field := range fields {
		req.presentFields[field] = struct{}{}
	}
	return req, nil
}

type rpcProblem struct {
	status  int
	code    string
	message string
}

func parseItemScope(raw json.RawMessage) (itemScope, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return itemScope{}, fmt.Errorf("scope must be an object")
	}
	var scope itemScope
	decoder := json.NewDecoder(bytes.NewReader(trimmed))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&scope); err != nil {
		return itemScope{}, err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return itemScope{}, fmt.Errorf("scope must contain exactly one JSON object")
		}
		return itemScope{}, err
	}
	if scope.AppID == "" || scope.TenantID == "" || scope.AgentID == "" || scope.RunID == "" {
		return itemScope{}, fmt.Errorf("scope requires appId, tenantId, agentId and runId")
	}
	return scope, nil
}

func parseConfiguredScope(value string) (itemScope, error) {
	parts := strings.Split(value, "#")
	if len(parts) != 4 {
		return itemScope{}, fmt.Errorf("scope must be app#tenant#agent#run")
	}
	scope := itemScope{AppID: parts[0], TenantID: parts[1], AgentID: parts[2], RunID: parts[3]}
	if scope.AppID == "" || scope.TenantID == "" || scope.AgentID == "" || scope.RunID == "" {
		return itemScope{}, fmt.Errorf("scope segments must be non-empty")
	}
	return scope, nil
}

func (m *mock) configuredStore() string {
	if m.expectedStore != "" {
		return m.expectedStore
	}
	return defaultMemoryStoreName
}

func (m *mock) configuredScope() itemScope {
	if m.expectedScope != (itemScope{}) {
		return m.expectedScope
	}
	return defaultItemScope
}

var allowedRequestFields = map[string]map[string]struct{}{
	"ListItems":  fields("type", "memoryStoreName", "scope", "pathPrefix", "depth", "view", "limit", "nextToken"),
	"GetItem":    fields("type", "memoryStoreName", "scope", "itemId", "path", "view"),
	"AddItem":    fields("type", "memoryStoreName", "scope", "path", "content", "sessionId", "view"),
	"UpdateItem": fields("type", "memoryStoreName", "scope", "itemId", "path", "content", "precondition", "overwrite", "sessionId", "view"),
	"DeleteItem": fields("type", "memoryStoreName", "scope", "itemId", "path", "pathPrefix", "precondition", "sessionId"),
}

func fields(names ...string) map[string]struct{} {
	result := make(map[string]struct{}, len(names))
	for _, name := range names {
		result[name] = struct{}{}
	}
	return result
}

func (m *mock) validateRequest(action string, req *request) *rpcProblem {
	if req.Type != itemType {
		return &rpcProblem{http.StatusBadRequest, "VALIDATION_ERROR", "type must be memoryfile"}
	}
	if req.MemoryStoreName != m.configuredStore() {
		return &rpcProblem{http.StatusBadRequest, "VALIDATION_ERROR", "memoryStoreName does not match mock configuration"}
	}
	scope, err := parseItemScope(req.Scope)
	if err != nil {
		return &rpcProblem{http.StatusBadRequest, "VALIDATION_ERROR", err.Error()}
	}
	if scope != m.configuredScope() {
		return &rpcProblem{http.StatusBadRequest, "VALIDATION_ERROR", "scope does not match mock configuration"}
	}
	canonicalScope, err := json.Marshal(scope)
	if err != nil {
		panic(err)
	}
	req.Scope = canonicalScope

	allowed, knownAction := allowedRequestFields[action]
	if !knownAction {
		return nil
	}
	for field := range req.presentFields {
		if _, ok := allowed[field]; !ok {
			return &rpcProblem{http.StatusBadRequest, "VALIDATION_ERROR", fmt.Sprintf("field %q is not valid for %s", field, action)}
		}
	}
	return nil
}

func normalizedView(view string) string {
	if view == "" {
		return "basic"
	}
	return view
}

func requestDepth(req request) int {
	if req.Depth == nil {
		return 0
	}
	return *req.Depth
}

func cursorForRequest(req request, lastPath string) listCursor {
	return listCursor{
		MemoryStoreName: req.MemoryStoreName,
		Scope:           string(req.Scope),
		PathPrefix:      req.PathPrefix,
		View:            normalizedView(req.View),
		Depth:           requestDepth(req),
		LastPath:        lastPath,
	}
}

func (m *mock) storeCursor(req request, lastPath string) string {
	if m.cursors == nil {
		m.cursors = map[string]listCursor{}
	}
	for {
		var random [18]byte
		if _, err := rand.Read(random[:]); err != nil {
			panic(err)
		}
		token := "cursor_" + base64.RawURLEncoding.EncodeToString(random[:])
		if _, exists := m.cursors[token]; !exists {
			m.cursors[token] = cursorForRequest(req, lastPath)
			return token
		}
	}
}

func (m *mock) resolveCursor(req request) (listCursor, *rpcProblem) {
	cursor, ok := m.cursors[req.NextToken]
	if !ok {
		return listCursor{}, &rpcProblem{http.StatusBadRequest, "VALIDATION_ERROR", "nextToken is unknown or expired"}
	}
	want := cursorForRequest(req, cursor.LastPath)
	if cursor != want {
		return listCursor{}, &rpcProblem{http.StatusBadRequest, "VALIDATION_ERROR", "nextToken does not match list parameters"}
	}
	return cursor, nil
}

func validItemPath(path string) bool {
	return strings.HasPrefix(path, "/") && path != "/" && !strings.HasSuffix(path, "/") && !strings.Contains(path, "//")
}

func validPathPrefix(prefix string) bool {
	if prefix == "" || prefix == "/" {
		return true
	}
	return strings.HasPrefix(prefix, "/") && strings.HasSuffix(prefix, "/") && !strings.Contains(strings.TrimPrefix(prefix, "/"), "//")
}

func validView(view string) bool {
	return view == "" || view == "basic" || view == "full"
}

// update applies one final-contract UpdateItem mutation. All validation is
// completed before the map changes so a combined content+rename stays atomic.
func (m *mock) update(req request) (*item, *rpcProblem) {
	if req.ItemID == "" {
		return nil, &rpcProblem{http.StatusBadRequest, "VALIDATION_ERROR", "itemId is required"}
	}
	if req.Content == nil && req.Path == "" {
		return nil, &rpcProblem{http.StatusBadRequest, "VALIDATION_ERROR", "content or path is required"}
	}
	if req.Overwrite && req.Path == "" {
		return nil, &rpcProblem{http.StatusBadRequest, "VALIDATION_ERROR", "overwrite requires path"}
	}
	if req.Path != "" && !validItemPath(req.Path) {
		return nil, &rpcProblem{http.StatusBadRequest, "VALIDATION_ERROR", "path must be a canonical absolute item path"}
	}
	if req.Content != nil && len(*req.Content) > maxContentBytes {
		return nil, &rpcProblem{http.StatusRequestEntityTooLarge, "PAYLOAD_TOO_LARGE", "content exceeds 100KiB"}
	}

	f := m.byID(req.ItemID)
	if f == nil {
		return nil, &rpcProblem{http.StatusNotFound, "NOT_FOUND", "no such item"}
	}
	if req.Precondition != nil && req.Precondition.ExpectedVersionID != f.LatestVersionID {
		return nil, &rpcProblem{http.StatusConflict, "LOCK_CONFLICT", "expectedVersionId does not match"}
	}

	var occupied *item
	if req.Path != "" {
		occupied = m.items[req.Path]
		if occupied != nil && occupied != f && !req.Overwrite {
			return nil, &rpcProblem{http.StatusConflict, "PATH_EXISTS", "target path already exists"}
		}
	}

	oldPath := f.Path
	if req.Path != "" {
		if occupied != nil && occupied != f {
			delete(m.items, occupied.Path)
		}
		delete(m.items, oldPath)
		f.Path = req.Path
		m.items[f.Path] = f
	}
	if req.Content != nil {
		f.Content = *req.Content
		f.ContentSHA256 = shaOf(*req.Content)
		f.ContentSizeBytes = int64(len(*req.Content))
	}
	m.advanceVersion(f)
	return f, nil
}

func (m *mock) handleRPC(w http.ResponseWriter, r *http.Request) {
	action := strings.TrimPrefix(r.URL.Path, "/")
	body, err := io.ReadAll(r.Body)
	if err != nil {
		m.fail(w, http.StatusBadRequest, "INVALID_REQUEST", "failed to read request body")
		return
	}
	req, err := decodeRequest(body)
	if err != nil {
		m.fail(w, http.StatusBadRequest, "INVALID_REQUEST", err.Error())
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	accessKeyID := r.Header.Get("x-ots-accesskeyid")
	requestCredential, ok := m.credentials[accessKeyID]
	if !ok || r.Header.Get("x-ots-ststoken") != requestCredential.token {
		m.fail(w, http.StatusForbidden, "AUTH_FAILED", "credential tuple rejected")
		return
	}
	// Region、正文摘要和请求签名均由 mock 独立验证，避免客户端与 mock 使用同一
	// 错误输入仍能自洽通过。真实服务端对 Region 错配返回 OTSAuthFailed。
	if err := validateV4Request(r, body, requestCredential.secret, m.region); err != nil {
		m.fail(w, http.StatusForbidden, "AUTH_FAILED",
			err.Error())
		return
	}
	m.lastAuthAK = accessKeyID
	m.lastAuthToken = requestCredential.token
	if problem := m.validateRequest(action, &req); problem != nil {
		m.fail(w, problem.status, problem.code, problem.message)
		return
	}

	if f := m.takeFault(action); f != nil {
		log.Printf("%s: injected fault %d %s", action, f.Status, f.Code)
		m.fail(w, f.Status, f.Code, "injected fault")
		return
	}
	writeAction := action == "AddItem" || action == "UpdateItem" || action == "DeleteItem"
	if m.readOnly && writeAction {
		m.fail(w, http.StatusConflict, "READ_ONLY_STORE", "store is a read-only projection")
		return
	}

	switch action {
	case "ListItems":
		if !validView(req.View) {
			m.fail(w, http.StatusBadRequest, "VALIDATION_ERROR", "view must be basic or full")
			return
		}
		if !validPathPrefix(req.PathPrefix) {
			m.fail(w, http.StatusBadRequest, "VALIDATION_ERROR", "pathPrefix must be segment aligned")
			return
		}
		if req.Limit < 0 || req.Limit > 100 {
			m.fail(w, http.StatusBadRequest, "VALIDATION_ERROR", "limit must be between 0 and 100")
			return
		}
		if req.Depth != nil && *req.Depth != 0 {
			m.fail(w, http.StatusBadRequest, "VALIDATION_ERROR", "mock supports recursive list depth only")
			return
		}
		limit := req.Limit
		if limit == 0 {
			limit = 20
		}
		paths := make([]string, 0, len(m.items))
		for p := range m.items {
			if req.PathPrefix == "/" || strings.HasPrefix(p, req.PathPrefix) {
				paths = append(paths, p)
			}
		}
		sort.Strings(paths)
		start := 0
		if req.NextToken != "" {
			cursor, problem := m.resolveCursor(req)
			if problem != nil {
				m.fail(w, problem.status, problem.code, problem.message)
				return
			}
			start = sort.SearchStrings(paths, cursor.LastPath)
			if start < len(paths) && paths[start] == cursor.LastPath {
				start++
			}
		}
		end := len(paths)
		if start+limit < end {
			end = start + limit
		}
		items := make([]itemJSON, 0, end-start)
		for _, p := range paths[start:end] {
			items = append(items, render(m.items[p], req.Scope, req.View == "full"))
		}
		next := ""
		if end < len(paths) {
			next = m.storeCursor(req, paths[end-1])
		}
		m.ok(w, r, map[string]any{"type": itemType, "items": items, "nextToken": next, "readOnly": m.readOnly})

	case "GetItem":
		if !validView(req.View) {
			m.fail(w, http.StatusBadRequest, "VALIDATION_ERROR", "view must be basic or full")
			return
		}
		if (req.ItemID == "") == (req.Path == "") {
			m.fail(w, http.StatusBadRequest, "VALIDATION_ERROR", "exactly one of itemId and path is required")
			return
		}
		if req.Path != "" && !validItemPath(req.Path) {
			m.fail(w, http.StatusBadRequest, "VALIDATION_ERROR", "path must be a canonical absolute item path")
			return
		}
		var f *item
		if req.ItemID != "" {
			f = m.byID(req.ItemID)
		} else {
			f = m.items[req.Path]
		}
		if f == nil {
			m.fail(w, http.StatusNotFound, "NOT_FOUND", "no such item")
			return
		}
		m.ok(w, r, render(f, req.Scope, req.View == "full"))

	case "AddItem":
		if !validView(req.View) {
			m.fail(w, http.StatusBadRequest, "VALIDATION_ERROR", "view must be basic or full")
			return
		}
		if !validItemPath(req.Path) {
			m.fail(w, http.StatusBadRequest, "VALIDATION_ERROR", "path must be a canonical absolute item path")
			return
		}
		if req.Content == nil {
			m.fail(w, http.StatusBadRequest, "VALIDATION_ERROR", "content is required")
			return
		}
		if _, exists := m.items[req.Path]; exists {
			m.fail(w, http.StatusConflict, "PATH_EXISTS", "path already exists")
			return
		}
		if len(m.items) >= m.maxFiles {
			m.fail(w, http.StatusConflict, "QUOTA_EXCEEDED", "item quota exceeded")
			return
		}
		if len(*req.Content) > maxContentBytes {
			m.fail(w, http.StatusRequestEntityTooLarge, "PAYLOAD_TOO_LARGE", "content exceeds 100KiB")
			return
		}
		m.ok(w, r, render(m.put(req.Path, *req.Content, true), req.Scope, req.View == "full"))

	case "UpdateItem":
		if !validView(req.View) {
			m.fail(w, http.StatusBadRequest, "VALIDATION_ERROR", "view must be basic or full")
			return
		}
		f, problem := m.update(req)
		if problem != nil {
			m.fail(w, problem.status, problem.code, problem.message)
			return
		}
		m.ok(w, r, render(f, req.Scope, req.View == "full"))

	case "DeleteItem":
		if (req.ItemID == "") == (req.Path == "") {
			m.fail(w, http.StatusBadRequest, "VALIDATION_ERROR", "exactly one of itemId and path is required")
			return
		}
		if req.Path != "" && !validItemPath(req.Path) {
			m.fail(w, http.StatusBadRequest, "VALIDATION_ERROR", "path must be a canonical absolute item path")
			return
		}
		var f *item
		if req.ItemID != "" {
			f = m.byID(req.ItemID)
		} else {
			f = m.items[req.Path]
		}
		if f == nil {
			m.fail(w, http.StatusNotFound, "NOT_FOUND", "no such item")
			return
		}
		if req.Precondition != nil && req.Precondition.ExpectedVersionID != f.LatestVersionID {
			m.fail(w, http.StatusConflict, "LOCK_CONFLICT", "expectedVersionId does not match")
			return
		}
		delete(m.items, f.Path)
		m.ok(w, r, map[string]any{"type": itemType, "itemId": f.ItemID, "deleted": true})

	default:
		m.fail(w, http.StatusNotFound, "UNKNOWN_ACTION", "unknown action "+action)
	}
}

// 控制端点全部用 GET+query，便于 busybox wget 调用。
func (m *mock) handleControl(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	m.mu.Lock()
	defer m.mu.Unlock()
	switch r.URL.Path {
	case "/__put": // 服务端直改内容（模拟并发写方，制造资源版本冲突）
		f := m.put(canonicalItemPath(q.Get("path")), q.Get("content"), false)
		_ = json.NewEncoder(w).Encode(render(f, nil, true))
	case "/__del":
		delete(m.items, canonicalItemPath(q.Get("path")))
		fmt.Fprintln(w, "ok")
	case "/__fault": // ?action=UpdateItem&status=429&code=THROTTLED&times=2
		status, _ := strconv.Atoi(q.Get("status"))
		times, _ := strconv.Atoi(q.Get("times"))
		if times <= 0 {
			times = 1
		}
		m.faults = append(m.faults, &fault{Action: q.Get("action"), Status: status, Code: q.Get("code"), Times: times})
		fmt.Fprintln(w, "ok")
	case "/__readonly":
		m.readOnly = q.Get("v") == "1"
		fmt.Fprintln(w, "ok")
	case "/__maxfiles":
		if n, err := strconv.Atoi(q.Get("v")); err == nil && n > 0 {
			m.maxFiles = n
		}
		fmt.Fprintln(w, "ok")
	case "/__seed": // ?n=1200&prefix=bulk/ 批量造文件，测 Refresh 分页
		n, _ := strconv.Atoi(q.Get("n"))
		prefix := canonicalItemPath(q.Get("prefix"))
		for i := 0; i < n; i++ {
			m.put(fmt.Sprintf("%sf%04d.md", prefix, i), fmt.Sprintf("seed content %d", i), false)
		}
		fmt.Fprintln(w, "ok")
	case "/__count":
		fmt.Fprintln(w, len(m.items))
	case "/__lastauth":
		fmt.Fprintf(w, "%s %s\n", m.lastAuthAK, m.lastAuthToken)
	case "/__dump":
		_ = json.NewEncoder(w).Encode(m.items)
	case "/__reset":
		m.items = map[string]*item{}
		m.faults = nil
		m.readOnly = false
		m.maxFiles = 2000
		m.lastAuthAK = ""
		m.lastAuthToken = ""
		m.seq = 0
		m.idc = 0
		m.cursors = nil
		fmt.Fprintln(w, "ok")
	default:
		http.NotFound(w, r)
	}
}

func main() {
	addr := os.Getenv("MOCK_ADDR")
	if addr == "" {
		addr = "127.0.0.1:8770"
	}
	// 期望的 v4 signregion：客户端 --region 必须与之相符，否则按真实服务端的行为拒绝。
	region := os.Getenv("MOCK_REGION")
	if region == "" {
		region = "test-region"
	}
	expectedStore := os.Getenv("MOCK_STORE")
	if expectedStore == "" {
		expectedStore = defaultMemoryStoreName
	}
	expectedScopeValue := os.Getenv("MOCK_SCOPE")
	if expectedScopeValue == "" {
		expectedScopeValue = "app#tenant#agent#run"
	}
	expectedScope, err := parseConfiguredScope(expectedScopeValue)
	if err != nil {
		log.Fatalf("invalid MOCK_SCOPE: %v", err)
	}
	m := &mock{
		items: map[string]*item{}, maxFiles: 2000, region: region,
		expectedStore: expectedStore, expectedScope: expectedScope,
		credentials: map[string]mockCredential{
			"test-ak":     {secret: "test-sk"},
			"test-old-ak": {secret: "test-old-sk", token: "test-old-token"},
			"test-new-ak": {secret: "test-new-sk", token: "test-new-token"},
		},
	}
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/__") {
			m.handleControl(w, r)
			return
		}
		m.handleRPC(w, r)
	})
	log.Printf("mockfilemem listening on %s (item API, store=%s scope=%s, v4 signregion=%s)", addr, expectedStore, expectedScopeValue, region)
	log.Fatal(http.ListenAndServe(addr, nil))
}

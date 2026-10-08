// Package client 是 file memory 服务的访问层:协议细节(签名、STS、错误体解析)
// 全部委托给官方 tablestore-agent-storage-sdk,本包只保留 FUSE 侧需要的
// 门面——store/scope 注入、瞬时错误重试、SDK 类型到本地类型的转换。
package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"agent-memory-fuse/internal/credential"

	"github.com/aliyun/tablestore-agent-storage-sdk/go/agentstorage"
	"golang.org/x/text/unicode/norm"
)

// Scope 复用 SDK 的四段精确 scope(JSON 形状一致,直接别名避免来回转换)。
type Scope = agentstorage.Scope

// Memory 是本地门面结构,由 SDK Item 转换而来,fs 层按字段访问,
// 不做 JSON 序列化。
type Memory struct {
	MemoryID         string
	Path             string
	Content          string
	ContentSHA256    string
	ContentSizeBytes int64
	LatestSeq        int64
	LatestVersionID  string
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

type Config struct {
	Endpoint        string
	Instance        string
	Region          string
	APIKey          string
	AccessKeyID     string
	AccessKeySecret string
	// STSToken 是 STS 临时凭证的 security token,与临时 AccessKeyID/Secret
	// 配套使用,由 SDK 经 x-ots-ststoken 头随请求签名发送。
	STSToken string
	// AllowInsecureAPIKey 允许 APIKey 走非 HTTPS 端点。仅限本地测试
	// (key 明文传输),生产环境禁用。mount 命令没有开启它的入口,只有 Go 调用方
	// 能设置——所以 CLI 上的 APIKey 挂载即使打到回环地址也必须用 https://。
	AllowInsecureAPIKey bool
	// AllowInsecureHTTP 允许非回环地址使用明文 HTTP。仅供明确受控的测试
	// 环境使用；它不放宽 API key 自身的 AllowInsecureAPIKey 保护。
	AllowInsecureHTTP bool
	StoreName         string
	Scope             Scope
	SessionID         string
	HTTPClient        *http.Client
	HTTPTimeout       time.Duration // 每次 HTTP 请求超时,缺省 30s(HTTPClient 非空时以其为准)
	// RetryBackoff 是安全读的 429/500/503/504/LOCK_CONFLICT，以及
	// mutation 的 429 重试退避序列；缺省 100ms/500ms。mutation 收到
	// LOCK_CONFLICT 后不重放 write，直接进入 authoritative Get 收敛。
	// mutation 的模糊 5xx/transport/response 结果不重放，而用同一预算做
	// authoritative Get 收敛。
	RetryBackoff []time.Duration
}

var defaultRetryBackoff = []time.Duration{100 * time.Millisecond, 500 * time.Millisecond}

var ErrSTSRotationUnsupported = errors.New("STS credential rotation is only supported for STS mounts")

// maxResponseBytes 是单响应体上限(沿袭旧手写 client 的 io.LimitReader 兜底):
// 正常响应远小于此(文件内容 ≤100KiB),仅防失控网关/中间层返回超大 body 时,
// 被 go-fuse 每内核请求一 goroutine 的并发放大成无界内存。SDK 内部是无界
// io.ReadAll,所以在传给它的 HTTP 客户端传输层截断。
const maxResponseBytes = 4 << 20

type limitedBody struct {
	io.Reader
	io.Closer
}

type limitedTransport struct{ base http.RoundTripper }

func (t limitedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := t.base.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	resp.Body = limitedBody{Reader: io.LimitReader(resp.Body, maxResponseBytes), Closer: resp.Body}
	return resp, nil
}

type credentialGeneration struct {
	api *agentstorage.Client
	// 每代凭证的鉴权失败只打一条日志，轮转后允许新一代重新报告。
	authLogOnce sync.Once
}

type Client struct {
	config    Config
	sdkConfig agentstorage.Config
	stsMode   bool
	rotateMu  sync.Mutex
	active    atomic.Pointer[credentialGeneration]
}

type APIError struct {
	StatusCode int
	Code       string
	Message    string
	RequestID  string
}

// responseSemanticError is a private facade marker for a syntactically valid
// 2xx response that does not prove the requested mutation completed. Its
// fixed text intentionally carries no item content, path, or credentials.
type responseSemanticError struct{}

func (*responseSemanticError) Error() string {
	return "file memory mutation response did not provide valid evidence"
}

func (e *APIError) Error() string {
	if e.RequestID != "" {
		return fmt.Sprintf("file memory API %s (%d): %s [requestId=%s]", e.Code, e.StatusCode, e.Message, e.RequestID)
	}
	return fmt.Sprintf("file memory API %s (%d): %s", e.Code, e.StatusCode, e.Message)
}

func New(config Config) (*Client, error) {
	// 归一化必须先于校验,且校验的值必须就是发出去的值:此前 Instance/StoreName
	// 按 TrimSpace 后判空却按原值发送,`--instance " inst"` 能过校验再在服务端
	// 变成查不到实例的 403/404。
	config.Endpoint = strings.TrimRight(strings.TrimSpace(config.Endpoint), "/")
	config.Region = strings.TrimSpace(config.Region)
	config.Instance = strings.TrimSpace(config.Instance)
	config.StoreName = strings.TrimSpace(config.StoreName)
	if config.Endpoint == "" || config.Instance == "" || config.StoreName == "" {
		return nil, errors.New("endpoint, instance and store are required")
	}
	if err := validateEndpoint(config.Endpoint, config.AllowInsecureHTTP); err != nil {
		return nil, err
	}
	if config.APIKey == "" && (config.AccessKeyID == "" || config.AccessKeySecret == "") {
		return nil, errors.New("api key or access key credentials are required")
	}
	// SDK 在 APIKey 与 AK/SK 并存时静默优先 APIKey,STS token 会被忽略;
	// 与其静默丢鉴权材料,不如在入口拒绝。
	if config.STSToken != "" && config.APIKey != "" {
		return nil, errors.New("sts token and api key are mutually exclusive")
	}
	if config.STSToken != "" && (config.AccessKeyID == "" || config.AccessKeySecret == "") {
		return nil, errors.New("sts token requires access key credentials")
	}
	if config.APIKey == "" && config.Region == "" {
		return nil, errors.New("region is required for v4 signing")
	}
	if config.HTTPTimeout <= 0 {
		config.HTTPTimeout = 30 * time.Second
	}
	if config.HTTPClient == nil {
		config.HTTPClient = &http.Client{Timeout: config.HTTPTimeout}
	}
	if config.RetryBackoff == nil {
		config.RetryBackoff = defaultRetryBackoff
	}
	// 拷贝(而非改写调用方的)HTTP 客户端后做两件事:禁止重定向、包响应体上限
	// 传输层。禁重定向是唯一拦住凭证跨主机外泄的防线——net/http 的跨域敏感头
	// 剥离白名单只含 Authorization/Cookie/Proxy-*,x-ots-apikey 与
	// x-ots-signaturev4 会被原样拷到下一跳,所以放宽这里之前必须先在传输层
	// 钉住 endpoint 的 scheme+host(单靠 validateEndpoint 不够,它不比对 host)。
	httpClient := *config.HTTPClient
	httpClient.CheckRedirect = func(*http.Request, []*http.Request) error {
		return errors.New("HTTP redirects are not allowed")
	}
	transport := httpClient.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}
	httpClient.Transport = limitedTransport{base: transport}
	sdkConfig := agentstorage.Config{
		Endpoint:            config.Endpoint,
		InstanceName:        config.Instance,
		Region:              config.Region,
		SignVersion:         agentstorage.SignV4,
		AccessKeyID:         config.AccessKeyID,
		AccessKeySecret:     config.AccessKeySecret,
		STSToken:            config.STSToken,
		APIKey:              config.APIKey,
		AllowInsecureAPIKey: config.AllowInsecureAPIKey,
		HTTPClient:          &httpClient,
		UserAgentSuffix:     "agent-memory-fuse",
	}
	api, err := agentstorage.NewClient(sdkConfig)
	if err != nil {
		return nil, err
	}
	stsMode := config.STSToken != ""
	// 轮转工厂只保留连接配置，活动凭证仅存在于对应 SDK generation 中。
	sdkConfig.AccessKeyID = ""
	sdkConfig.AccessKeySecret = ""
	sdkConfig.STSToken = ""
	sdkConfig.APIKey = ""
	config.AccessKeyID = ""
	config.AccessKeySecret = ""
	config.STSToken = ""
	config.APIKey = ""
	client := &Client{config: config, sdkConfig: sdkConfig, stsMode: stsMode}
	client.active.Store(&credentialGeneration{api: api})
	return client, nil
}

func validateEndpoint(endpoint string, allowInsecureHTTP bool) error {
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Host == "" {
		return errors.New("endpoint must be a valid URL")
	}
	if strings.EqualFold(parsed.Scheme, "https") {
		return nil
	}
	if strings.EqualFold(parsed.Scheme, "http") && isLoopbackHost(parsed.Hostname()) {
		return nil
	}
	if strings.EqualFold(parsed.Scheme, "http") && allowInsecureHTTP {
		return nil
	}
	return errors.New("endpoint must use HTTPS; plain HTTP is allowed only for loopback testing or with explicit test opt-in")
}

func isLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// UpdateSTSCredentials 构造完整的新 SDK client 后一次发布，绝不逐字段改写
// 正在使用的 AK/SK/token。本方法只做本地校验，不向服务端探测凭证有效性。
func (c *Client) UpdateSTSCredentials(credentials credential.STS) error {
	if !c.stsMode {
		return ErrSTSRotationUnsupported
	}
	if err := credentials.Validate(); err != nil {
		return err
	}
	c.rotateMu.Lock()
	defer c.rotateMu.Unlock()
	config := c.sdkConfig
	config.AccessKeyID = credentials.AccessKeyID
	config.AccessKeySecret = credentials.AccessKeySecret
	config.STSToken = credentials.Token
	api, err := agentstorage.NewClient(config)
	if err != nil {
		return err
	}
	c.active.Store(&credentialGeneration{api: api})
	return nil
}

// ListResult 是 List() 的本地结果(由 /ListItems 响应转换而来)。
// ReadOnly 是服务端只读握手标志:file+ots 投影库返回 true(设计 §5),
// filemem 省略。
type ListResult struct {
	Memories  []Memory
	NextToken string
	ReadOnly  bool
}

// scoped 留空 Type,由 SDK 自动填充 "memoryfile"(Item API 鉴别字段)。
func (c *Client) scoped() agentstorage.ItemScopedRequest {
	return agentstorage.ItemScopedRequest{MemoryStoreName: c.config.StoreName, Scope: c.config.Scope}
}

func serviceItemPath(value string) string {
	value = norm.NFC.String(value)
	return "/" + strings.TrimLeft(value, "/")
}

func servicePathPrefix(value string) string {
	value = norm.NFC.String(value)
	value = strings.Trim(value, "/")
	if value == "" {
		return ""
	}
	return "/" + value + "/"
}

func (c *Client) List(ctx context.Context, pathPrefix, nextToken string, limit int) (ListResult, error) {
	generation := c.active.Load()
	var result ListResult
	err := c.doRead(ctx, generation, func(ctx context.Context, api *agentstorage.Client) error {
		response, err := api.ListItems(ctx, &agentstorage.ListItemsRequest{
			ItemScopedRequest: c.scoped(), PathPrefix: servicePathPrefix(pathPrefix), NextToken: nextToken, Limit: limit,
		})
		if err != nil {
			return err
		}
		memories := make([]Memory, 0, len(response.Items))
		for _, item := range response.Items {
			if item.Type != agentstorage.ItemTypeMemoryFile {
				return fmt.Errorf("unexpected recursive list entry type %q for path %q", item.Type, item.Path)
			}
			memories = append(memories, toMemoryEntry(item))
		}
		result = ListResult{Memories: memories, NextToken: response.NextToken, ReadOnly: response.ReadOnly}
		return nil
	})
	return result, err
}

// Stat 使用 GetItem 的 basic view,只取元数据并省略 content。
func (c *Client) Stat(ctx context.Context, path string) (Memory, error) {
	return c.doMemory(ctx, func(ctx context.Context, api *agentstorage.Client) (*agentstorage.Item, error) {
		return api.GetItem(ctx, &agentstorage.GetItemRequest{
			ItemScopedRequest: c.scoped(), Path: serviceItemPath(path),
		})
	})
}

// Read 使用 GetItem 的 full view 返回 content。
func (c *Client) Read(ctx context.Context, path string) (Memory, error) {
	return c.doMemory(ctx, func(ctx context.Context, api *agentstorage.Client) (*agentstorage.Item, error) {
		return api.GetItem(ctx, &agentstorage.GetItemRequest{
			ItemScopedRequest: c.scoped(), Path: serviceItemPath(path), View: agentstorage.ItemViewFull,
		})
	})
}

// Create 新建文件并以 full view 返回完整内容。
func (c *Client) Create(ctx context.Context, path string, content []byte) (Memory, error) {
	generation := c.active.Load()
	admitted := ctx.Err() == nil
	canonicalPath := serviceItemPath(path)
	var response *agentstorage.Item
	err := c.doMutation(ctx, generation, func(ctx context.Context, api *agentstorage.Client) error {
		var err error
		response, err = api.AddItem(ctx, &agentstorage.AddItemRequest{
			ItemScopedRequest: c.scoped(), Path: serviceItemPath(path), Content: string(content), SessionID: c.config.SessionID, View: agentstorage.ItemViewFull,
		})
		return err
	})
	ownerCandidateID := createOwnerCandidateID(response)
	if err == nil && !validCreateResponse(response, canonicalPath, string(content)) {
		err = &responseSemanticError{}
	}
	if err == nil {
		return toMemory(*response), nil
	}
	if !mutationNeedsReconciliation(admitted, ctx.Err(), err) {
		return Memory{}, err
	}
	reconcileCtx, cancel := c.reconciliationContext(ctx)
	defer cancel()
	return c.reconcileCreate(reconcileCtx, generation, canonicalPath, string(content), ownerCandidateID, err)
}

func itemPrecondition(versionID string) *agentstorage.ItemPrecondition {
	return &agentstorage.ItemPrecondition{ExpectedVersionID: versionID}
}

// Update 以稳定 item ID 覆写内容,并用当前资源版本 ID 做并发前置条件。
func (c *Client) Update(ctx context.Context, itemID string, content []byte, expectedVersionID string) (Memory, error) {
	generation := c.active.Load()
	admitted := ctx.Err() == nil
	var response *agentstorage.Item
	err := c.doMutation(ctx, generation, func(ctx context.Context, api *agentstorage.Client) error {
		var err error
		response, err = api.UpdateItem(ctx, &agentstorage.UpdateItemRequest{
			ItemScopedRequest: c.scoped(), ItemID: itemID, Content: agentstorage.String(string(content)), Precondition: itemPrecondition(expectedVersionID), SessionID: c.config.SessionID, View: agentstorage.ItemViewFull,
		})
		return err
	})
	if err == nil && !validUpdateResponse(response, itemID, string(content)) {
		err = &responseSemanticError{}
	}
	if err == nil {
		return toMemory(*response), nil
	}
	if !mutationNeedsReconciliation(admitted, ctx.Err(), err) {
		return Memory{}, err
	}
	reconcileCtx, cancel := c.reconciliationContext(ctx)
	defer cancel()
	return c.reconcileUpdate(reconcileCtx, generation, itemID, string(content), expectedVersionID, err)
}

// Rename 以稳定 item ID 寻址,只更新目标 path,并使用 basic response。
func (c *Client) Rename(ctx context.Context, itemID, toPath string, overwrite bool, expectedVersionID string) (Memory, error) {
	generation := c.active.Load()
	admitted := ctx.Err() == nil
	canonicalPath := serviceItemPath(toPath)
	var response *agentstorage.Item
	err := c.doMutation(ctx, generation, func(ctx context.Context, api *agentstorage.Client) error {
		var err error
		response, err = api.UpdateItem(ctx, &agentstorage.UpdateItemRequest{
			ItemScopedRequest: c.scoped(), ItemID: itemID, Path: agentstorage.String(serviceItemPath(toPath)), Precondition: itemPrecondition(expectedVersionID), Overwrite: overwrite, SessionID: c.config.SessionID,
		})
		return err
	})
	if err == nil && !validRenameResponse(response, itemID, canonicalPath) {
		err = &responseSemanticError{}
	}
	if err == nil {
		return toMemory(*response), nil
	}
	if !mutationNeedsReconciliation(admitted, ctx.Err(), err) {
		return Memory{}, err
	}
	reconcileCtx, cancel := c.reconciliationContext(ctx)
	defer cancel()
	return c.reconcileRename(reconcileCtx, generation, itemID, canonicalPath, expectedVersionID, err)
}

// Delete 以稳定 item ID 寻址,并用当前资源版本 ID 做并发前置条件。
func (c *Client) Delete(ctx context.Context, itemID, expectedVersionID string) error {
	generation := c.active.Load()
	admitted := ctx.Err() == nil
	var response *agentstorage.DeleteItemResponse
	err := c.doMutation(ctx, generation, func(ctx context.Context, api *agentstorage.Client) error {
		var err error
		response, err = api.DeleteItem(ctx, &agentstorage.DeleteItemRequest{
			ItemScopedRequest: c.scoped(), ItemID: itemID, Precondition: itemPrecondition(expectedVersionID), SessionID: c.config.SessionID,
		})
		return err
	})
	if err == nil && !validDeleteResponse(response, itemID) {
		err = &responseSemanticError{}
	}
	if err == nil || !mutationNeedsReconciliation(admitted, ctx.Err(), err) {
		return err
	}
	reconcileCtx, cancel := c.reconciliationContext(ctx)
	defer cancel()
	return c.reconcileDelete(reconcileCtx, generation, itemID, expectedVersionID, err)
}

func (c *Client) reconciliationContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx.Err() == nil {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(context.WithoutCancel(ctx), c.config.HTTPTimeout)
}

func (c *Client) reconcileCreate(ctx context.Context, generation *credentialGeneration, canonicalPath, content, ownerCandidateID string, mutationErr error) (Memory, error) {
	current, recheckErr := c.getItem(ctx, generation, &agentstorage.GetItemRequest{
		ItemScopedRequest: c.scoped(), Path: canonicalPath, View: agentstorage.ItemViewFull,
	})
	if recheckErr != nil {
		return Memory{}, mutationErr
	}
	if !validAuthoritativeItem(current, true) || current.Path != canonicalPath {
		return Memory{}, mutationErr
	}
	contentMatches := current.Content != nil && *current.Content == content
	if ownerCandidateID == "" {
		if !contentMatches {
			return Memory{}, pathExists(mutationErr)
		}
		return Memory{}, mutationErr
	}
	if current.ItemID != ownerCandidateID || !contentMatches {
		return Memory{}, pathExists(mutationErr)
	}
	return toMemory(*current), nil
}

func (c *Client) reconcileRename(ctx context.Context, generation *credentialGeneration, itemID, canonicalPath, expectedVersionID string, mutationErr error) (Memory, error) {
	current, recheckErr := c.getItem(ctx, generation, &agentstorage.GetItemRequest{
		ItemScopedRequest: c.scoped(), ItemID: itemID,
	})
	if recheckErr != nil {
		if exactNotFound(recheckErr) {
			return Memory{}, preconditionFailed(mutationErr)
		}
		return Memory{}, mutationErr
	}
	if !validAuthoritativeItem(current, false) || current.ItemID != itemID {
		return Memory{}, mutationErr
	}
	if current.Path != canonicalPath {
		if current.LatestVersionID == expectedVersionID && exactLockConflict(mutationErr) {
			return Memory{}, mutationErr
		}
		return Memory{}, preconditionFailed(mutationErr)
	}
	return toMemory(*current), nil
}

func (c *Client) reconcileUpdate(ctx context.Context, generation *credentialGeneration, itemID, content, expectedVersionID string, mutationErr error) (Memory, error) {
	current, recheckErr := c.getItem(ctx, generation, &agentstorage.GetItemRequest{
		ItemScopedRequest: c.scoped(), ItemID: itemID, View: agentstorage.ItemViewFull,
	})
	if recheckErr != nil {
		if exactNotFound(recheckErr) {
			return Memory{}, preconditionFailed(mutationErr)
		}
		return Memory{}, mutationErr
	}
	if !validAuthoritativeItem(current, true) || current.ItemID != itemID {
		return Memory{}, mutationErr
	}
	if current.Content == nil || *current.Content != content {
		if current.LatestVersionID == expectedVersionID && exactLockConflict(mutationErr) {
			return Memory{}, mutationErr
		}
		return Memory{}, preconditionFailed(mutationErr)
	}
	return toMemory(*current), nil
}

func (c *Client) reconcileDelete(ctx context.Context, generation *credentialGeneration, itemID, expectedVersionID string, mutationErr error) error {
	current, recheckErr := c.getItem(ctx, generation, &agentstorage.GetItemRequest{
		ItemScopedRequest: c.scoped(), ItemID: itemID,
	})
	if recheckErr != nil {
		if exactNotFound(recheckErr) {
			return nil
		}
		return mutationErr
	}
	if !validAuthoritativeItem(current, false) || current.ItemID != itemID {
		return mutationErr
	}
	if current.LatestVersionID == expectedVersionID {
		return mutationErr
	}
	return preconditionFailed(mutationErr)
}

func (c *Client) getItem(ctx context.Context, generation *credentialGeneration, request *agentstorage.GetItemRequest) (*agentstorage.Item, error) {
	var current *agentstorage.Item
	err := c.doRead(ctx, generation, func(ctx context.Context, api *agentstorage.Client) error {
		var err error
		current, err = api.GetItem(ctx, request)
		return err
	})
	return current, err
}

func validAuthoritativeItem(item *agentstorage.Item, full bool) bool {
	if item == nil || item.Type != agentstorage.ItemTypeMemoryFile || item.ItemID == "" || item.Path == "" || item.LatestVersionID == "" || item.LatestSeq <= 0 || item.ContentSHA256 == "" {
		return false
	}
	return !full || item.Content != nil && item.ContentSizeBytes == int64(len([]byte(*item.Content)))
}

func validCreateResponse(item *agentstorage.Item, canonicalPath, content string) bool {
	return validAuthoritativeItem(item, true) && item.Path == canonicalPath && item.Content != nil && *item.Content == content
}

func createOwnerCandidateID(item *agentstorage.Item) string {
	if item == nil || item.Type != agentstorage.ItemTypeMemoryFile {
		return ""
	}
	return item.ItemID
}

func validUpdateResponse(item *agentstorage.Item, itemID, content string) bool {
	return validAuthoritativeItem(item, true) && item.ItemID == itemID && item.Content != nil && *item.Content == content
}

func validRenameResponse(item *agentstorage.Item, itemID, canonicalPath string) bool {
	return validAuthoritativeItem(item, false) && item.ItemID == itemID && item.Path == canonicalPath
}

func validDeleteResponse(response *agentstorage.DeleteItemResponse, itemID string) bool {
	return response != nil && response.Type == agentstorage.ItemTypeMemoryFile && response.ItemID == itemID && response.Deleted && !response.IsRanged()
}

func exactNotFound(err error) bool {
	var apiErr *APIError
	return errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusNotFound && apiErr.Code == "NOT_FOUND"
}

func exactLockConflict(err error) bool {
	var apiErr *APIError
	return errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusConflict && apiErr.Code == "LOCK_CONFLICT"
}

func preconditionFailed(original error) error {
	requestID := ""
	var apiErr *APIError
	if errors.As(original, &apiErr) {
		requestID = apiErr.RequestID
	}
	return &APIError{
		StatusCode: http.StatusConflict,
		Code:       "PRECONDITION_FAILED",
		Message:    "resource state differs from the requested mutation",
		RequestID:  requestID,
	}
}

func pathExists(original error) error {
	requestID := ""
	var apiErr *APIError
	if errors.As(original, &apiErr) {
		requestID = apiErr.RequestID
	}
	return &APIError{
		StatusCode: http.StatusConflict,
		Code:       "PATH_EXISTS",
		Message:    "the requested path is occupied by different content",
		RequestID:  requestID,
	}
}

func (c *Client) doMemory(ctx context.Context, call func(ctx context.Context, api *agentstorage.Client) (*agentstorage.Item, error)) (Memory, error) {
	generation := c.active.Load()
	var memory Memory
	err := c.doRead(ctx, generation, func(ctx context.Context, api *agentstorage.Client) error {
		response, err := call(ctx, api)
		if err != nil {
			return err
		}
		memory = toMemory(*response)
		return nil
	})
	return memory, err
}

// doRead 在 SDK 的安全读调用外保留有界退避重试。mutation reconciliation
// 也复用它，因此一次逻辑 authoritative Get 可以在相同 caller context 和
// credential generation 内按现有读预算重试。
func (c *Client) doRead(ctx context.Context, generation *credentialGeneration, call func(ctx context.Context, api *agentstorage.Client) error) error {
	for attempt := 0; ; attempt++ {
		err := convertError(call(ctx, generation.api))
		if err == nil {
			return nil
		}
		var apiErr *APIError
		if errors.As(err, &apiErr) {
			if readRetryable(apiErr) && attempt < len(c.config.RetryBackoff) {
				select {
				case <-time.After(c.config.RetryBackoff[attempt]):
					continue
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			logAuthFailure(generation, apiErr)
		}
		return err
	}
}

// doMutation only replays failures with an explicit not-applied retry
// contract. Ambiguous service and transport outcomes are left to the
// operation-specific authoritative reconciliation path.
func (c *Client) doMutation(ctx context.Context, generation *credentialGeneration, call func(context.Context, *agentstorage.Client) error) error {
	for attempt := 0; ; attempt++ {
		err := convertError(call(ctx, generation.api))
		if err == nil {
			return nil
		}
		var apiErr *APIError
		if errors.As(err, &apiErr) {
			if mutationRetryable(apiErr) && attempt < len(c.config.RetryBackoff) {
				select {
				case <-time.After(c.config.RetryBackoff[attempt]):
					continue
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			logAuthFailure(generation, apiErr)
		}
		return err
	}
}

func mutationRetryable(apiErr *APIError) bool {
	return apiErr.StatusCode == http.StatusTooManyRequests
}

func mutationNeedsReconciliation(admitted bool, contextErr, err error) bool {
	if !admitted || err == nil {
		return false
	}
	if contextErr != nil && !errors.Is(contextErr, context.DeadlineExceeded) {
		return false
	}
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		return apiErr.StatusCode == http.StatusConflict && apiErr.Code == "LOCK_CONFLICT" || apiErr.StatusCode >= 500 && apiErr.StatusCode <= 599
	}
	switch SDKClientErrorKind(err) {
	case "transport_context", "transport_timeout", "transport_network", "transport_other",
		"response_read", "response_integrity", "response_auth", "response_decode", "response_semantic":
		return true
	default:
		return false
	}
}

// readRetryable 只用于可安全重放的读：429/500/503/504 与精确
// 409 LOCK_CONFLICT。mutation 使用更窄的 mutationRetryable 白名单。
func readRetryable(apiErr *APIError) bool {
	if apiErr.StatusCode == http.StatusConflict {
		return apiErr.Code == "LOCK_CONFLICT"
	}
	switch apiErr.StatusCode {
	case http.StatusTooManyRequests, http.StatusInternalServerError, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	}
	return false
}

// convertError 把 SDK 的 *ServiceError 映射为 *APIError,保持 fs 层 errno
// 映射与调用方 errors.As(*APIError) 的既有依赖不变;客户端侧错误
// (*ClientError:配置、网络、响应校验)原样透传,由 fs 层归入 EIO。
func convertError(err error) error {
	var serviceErr *agentstorage.ServiceError
	if errors.As(err, &serviceErr) {
		code := serviceErr.Code
		if code == "" {
			code = http.StatusText(serviceErr.StatusCode)
		}
		return &APIError{StatusCode: serviceErr.StatusCode, Code: code, Message: serviceErr.Message, RequestID: serviceErr.RequestID}
	}
	return err
}

// toMemory 把 SDK Item(线上字段 itemId)转成本地 Memory(内部标识符保留
// MemoryID 命名,fs 层零改动)。SDK Item.Content 为 *string:nil 表示响应
// 采用 basic projection 并省略了 content(例如 Stat/List/Rename),
// 本地门面统一折叠为 ""。
func toMemory(item agentstorage.Item) Memory {
	content := ""
	if item.Content != nil {
		content = *item.Content
	}
	return Memory{
		MemoryID:         item.ItemID,
		Path:             item.Path,
		Content:          content,
		ContentSHA256:    item.ContentSHA256,
		ContentSizeBytes: item.ContentSizeBytes,
		LatestSeq:        item.LatestSeq,
		LatestVersionID:  item.LatestVersionID,
		CreatedAt:        parseTime(item.CreatedAt),
		UpdatedAt:        parseTime(item.UpdatedAt),
	}
}

// toMemoryEntry converts a recursive ListItems file entry while preserving
// its stable item ID and head-version metadata.
func toMemoryEntry(item agentstorage.ItemListEntry) Memory {
	content := ""
	if item.Content != nil {
		content = *item.Content
	}
	return Memory{
		MemoryID:         item.ItemID,
		Path:             item.Path,
		Content:          content,
		ContentSHA256:    item.ContentSHA256,
		ContentSizeBytes: item.ContentSizeBytes,
		LatestSeq:        item.LatestSeq,
		LatestVersionID:  item.LatestVersionID,
		CreatedAt:        parseTime(item.CreatedAt),
		UpdatedAt:        parseTime(item.UpdatedAt),
	}
}

// parseTime 容忍解析失败返回零值:时间戳只用于 FUSE mtime 展示,坏时间戳
// 不应让文件本身不可见(fs 层对零值按"无 mtime"处理)。
func parseTime(value string) time.Time {
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}
	}
	return parsed
} // logAuthFailure 对真鉴权失败按凭证代打一条日志。403 OTSValidationFail 是
// 参数/契约校验失败而非凭证问题——误报 auth failed 会把排查引向凭证
// (mount 错误分类将其归为 config/validation_failed)。
func logAuthFailure(generation *credentialGeneration, apiErr *APIError) {
	if apiErr.StatusCode == http.StatusUnauthorized ||
		(apiErr.StatusCode == http.StatusForbidden && apiErr.Code != "OTSValidationFail") {
		generation.authLogOnce.Do(func() {
			log.Printf("file memory auth failed (%d %s, requestId=%s): credential rejected or expired; rotate STS credentials or remount", apiErr.StatusCode, apiErr.Code, apiErr.RequestID)
		})
	}
}

package main

import (
	"bytes"
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
)

func testMock() *mock {
	return &mock{items: map[string]*item{}, maxFiles: 2000}
}

func rpcTestMock() *mock {
	m := testMock()
	m.region = "test-region"
	m.credentials = map[string]mockCredential{"test-ak": {secret: "test-sk"}}
	return m
}

func validRPCPayload() map[string]any {
	return map[string]any{
		"type":            itemType,
		"memoryStoreName": "memstore_t",
		"scope": map[string]any{
			"appId": "app", "tenantId": "tenant", "agentId": "agent", "runId": "run",
		},
	}
}

func clonePayload(t *testing.T, payload map[string]any) map[string]any {
	t.Helper()
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	var cloned map[string]any
	if err := json.Unmarshal(body, &cloned); err != nil {
		t.Fatal(err)
	}
	return cloned
}

func callRPC(t *testing.T, m *mock, action string, payload map[string]any) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/"+action, bytes.NewReader(body))
	req.Header.Set("x-ots-accesskeyid", "test-ak")
	sum := md5.Sum(body)
	req.Header.Set("x-ots-contentmd5", base64.StdEncoding.EncodeToString(sum[:]))
	req.Header.Set("x-ots-date", "2026-08-21T00:00:00.000Z")
	req.Header.Set("x-ots-signdate", "20260821")
	req.Header.Set("x-ots-signregion", "test-region")

	var headers []string
	for name, values := range req.Header {
		name = strings.ToLower(name)
		if strings.HasPrefix(name, "x-ots-") && name != "x-ots-signaturev4" && len(values) > 0 {
			headers = append(headers, name+":"+strings.TrimSpace(values[0]))
		}
	}
	sort.Strings(headers)
	canonical := req.URL.Path + "\nPOST\n\n" + strings.Join(headers, "\n") + "\nots"
	mac := hmac.New(sha256.New, v4SigningKey("test-sk", "20260821", "test-region"))
	_, _ = mac.Write([]byte(canonical))
	req.Header.Set("x-ots-signaturev4", base64.StdEncoding.EncodeToString(mac.Sum(nil)))

	recorder := httptest.NewRecorder()
	m.handleRPC(recorder, req)
	return recorder
}

func requireValidationError(t *testing.T, recorder *httptest.ResponseRecorder) {
	t.Helper()
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body=%s", recorder.Code, recorder.Body.String())
	}
	var response map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if response["code"] != "VALIDATION_ERROR" {
		t.Fatalf("code = %v, want VALIDATION_ERROR; body=%s", response["code"], recorder.Body.String())
	}
}

func TestPutAdvancesLatestVersionID(t *testing.T) {
	m := testMock()

	first := m.put("/a.md", "one", true)
	firstID := first.ItemID
	firstVersionID := first.LatestVersionID
	if firstVersionID == "" {
		t.Fatal("first version ID is empty")
	}

	second := m.put("/a.md", "two", false)
	if second.ItemID != firstID {
		t.Fatalf("item ID changed from %q to %q", firstID, second.ItemID)
	}
	if second.LatestVersionID == "" || second.LatestVersionID == firstVersionID {
		t.Fatalf("second version ID = %q, want a new non-empty ID after %q", second.LatestVersionID, firstVersionID)
	}
}

func TestFinalMutationRequestShape(t *testing.T) {
	body := []byte(`{
		"type":"memoryfile",
		"memoryStoreName":"store",
		"scope":{"appId":"app","tenantId":"tenant","agentId":"agent","runId":"run"},
		"itemId":"item_1",
		"content":"",
		"path":"/renamed.md",
		"precondition":{"expectedVersionId":"fmver_1"},
		"overwrite":true,
		"sessionId":"session",
		"view":"full"
	}`)

	req, err := decodeRequest(body)
	if err != nil {
		t.Fatalf("decodeRequest: %v", err)
	}
	if req.ItemID != "item_1" || req.Path != "/renamed.md" {
		t.Fatalf("identity/path = (%q, %q)", req.ItemID, req.Path)
	}
	if req.Content == nil || *req.Content != "" {
		t.Fatalf("content = %#v, want pointer to empty string", req.Content)
	}
	if req.Precondition == nil || req.Precondition.ExpectedVersionID != "fmver_1" {
		t.Fatalf("precondition = %#v", req.Precondition)
	}
	if req.View != "full" || !req.Overwrite || req.SessionID != "session" {
		t.Fatalf("view/overwrite/session = (%q, %t, %q)", req.View, req.Overwrite, req.SessionID)
	}
	if !json.Valid(req.Scope) {
		t.Fatalf("scope is not valid JSON: %q", req.Scope)
	}
}

func TestDecodeRequestRejectsObsoleteFields(t *testing.T) {
	for _, field := range []string{"newPath", "expectedSha256", "includeContent"} {
		t.Run(field, func(t *testing.T) {
			_, err := decodeRequest([]byte(`{"type":"memoryfile","` + field + `":"obsolete"}`))
			if err == nil {
				t.Fatalf("decodeRequest accepted obsolete field %q", field)
			}
			if !strings.Contains(err.Error(), field) {
				t.Fatalf("error %q does not identify field %q", err, field)
			}
		})
	}
}

func TestDecodeRequestRejectsTrailingJSONObject(t *testing.T) {
	_, err := decodeRequest([]byte(`{"type":"memoryfile"}{"type":"memoryfile"}`))
	if err == nil {
		t.Fatal("decodeRequest accepted two JSON objects")
	}
}

func TestDecodeRequestRejectsNonObject(t *testing.T) {
	_, err := decodeRequest([]byte(`null`))
	if err == nil {
		t.Fatal("decodeRequest accepted a non-object JSON value")
	}
}

func TestRenderFullDistinguishesEmptyContent(t *testing.T) {
	scope := json.RawMessage(`{"appId":"app","tenantId":"tenant","agentId":"agent","runId":"run"}`)
	f := &item{
		ItemID: "item_1", Path: "/empty.md", Content: "", LatestVersionID: "fmver_1",
		ContentSHA256: shaOf(""), CreatedAt: "created", UpdatedAt: "updated",
	}

	basic := render(f, scope, false)
	if basic.Content != nil {
		t.Fatalf("basic content = %#v, want nil", basic.Content)
	}
	full := render(f, scope, true)
	if full.Content == nil || *full.Content != "" {
		t.Fatalf("full content = %#v, want pointer to empty string", full.Content)
	}
	if basic.LatestVersionID != f.LatestVersionID || full.LatestVersionID != f.LatestVersionID {
		t.Fatalf("version IDs basic/full = (%q, %q), want %q", basic.LatestVersionID, full.LatestVersionID, f.LatestVersionID)
	}
	if basic.Path != f.Path || full.Path != f.Path {
		t.Fatalf("paths basic/full = (%q, %q), want %q", basic.Path, full.Path, f.Path)
	}
	if !bytes.Equal(basic.Scope, scope) || !bytes.Equal(full.Scope, scope) {
		t.Fatalf("scopes basic/full = (%s, %s), want %s", basic.Scope, full.Scope, scope)
	}
}

func TestCanonicalItemPath(t *testing.T) {
	for input, want := range map[string]string{
		"a.md":       "/a.md",
		"/a.md":      "/a.md",
		"//dir/a.md": "/dir/a.md",
		"bulk/":      "/bulk/",
	} {
		if got := canonicalItemPath(input); got != want {
			t.Errorf("canonicalItemPath(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestByIDFindsItemAtItsCurrentPath(t *testing.T) {
	m := testMock()
	f := m.put("/before.md", "content", true)
	delete(m.items, f.Path)
	f.Path = "/after.md"
	m.items[f.Path] = f

	got := m.byID(f.ItemID)
	if got != f || got.Path != "/after.md" {
		t.Fatalf("byID(%q) = %#v, want current item %#v", f.ItemID, got, f)
	}
}

func TestUpdateOverwritePreservesSourceIdentityAndAdvancesOneVersion(t *testing.T) {
	m := testMock()
	source := m.put("/source.md", "source", true)
	sourceID, sourceVersionID := source.ItemID, source.LatestVersionID
	target := m.put("/target.md", "target", true)
	targetID := target.ItemID
	seqBefore := m.seq
	targetPath, content := "/target.md", "replacement"

	got, problem := m.update(request{
		ItemID: sourceID, Path: targetPath, Content: &content, Overwrite: true,
		Precondition: &itemPrecondition{ExpectedVersionID: sourceVersionID},
	})
	if problem != nil {
		t.Fatalf("update: %#v", problem)
	}
	if m.seq != seqBefore+1 {
		t.Fatalf("sequence advanced from %d to %d, want exactly once", seqBefore, m.seq)
	}
	if got.ItemID != sourceID || got.Path != targetPath || got.Content != content {
		t.Fatalf("updated item = %#v", got)
	}
	if got.LatestVersionID == "" || got.LatestVersionID == sourceVersionID {
		t.Fatalf("updated version ID = %q, want new version after %q", got.LatestVersionID, sourceVersionID)
	}
	if _, exists := m.items["/source.md"]; exists {
		t.Fatal("source path still exists after rename")
	}
	if m.items[targetPath] != got {
		t.Fatalf("target path points to %#v, want %#v", m.items[targetPath], got)
	}
	if m.byID(targetID) != nil {
		t.Fatalf("overwritten target identity %q still exists", targetID)
	}
}

func TestRPCRejectsInvalidCommonItemIdentity(t *testing.T) {
	tests := map[string]func(map[string]any){
		"missing type":  func(p map[string]any) { delete(p, "type") },
		"wrong type":    func(p map[string]any) { p["type"] = "other" },
		"missing store": func(p map[string]any) { delete(p, "memoryStoreName") },
		"wrong store":   func(p map[string]any) { p["memoryStoreName"] = "other" },
		"missing scope": func(p map[string]any) { delete(p, "scope") },
		"malformed scope": func(p map[string]any) {
			p["scope"] = "not-an-object"
		},
		"incomplete scope": func(p map[string]any) {
			p["scope"] = map[string]any{"appId": "app", "tenantId": "tenant", "agentId": "agent"}
		},
		"mismatched scope": func(p map[string]any) {
			p["scope"] = map[string]any{"appId": "other", "tenantId": "tenant", "agentId": "agent", "runId": "run"}
		},
	}

	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			m := rpcTestMock()
			m.put("/a.md", "content", true)
			payload := validRPCPayload()
			payload["path"] = "/a.md"
			mutate(payload)
			requireValidationError(t, callRPC(t, m, "GetItem", payload))
		})
	}
}

func TestRPCRejectsFieldsFromOtherActions(t *testing.T) {
	tests := []struct {
		name    string
		action  string
		payload map[string]any
	}{
		{
			name: "get sessionId", action: "GetItem",
			payload: map[string]any{"path": "/a.md", "sessionId": "cross-action"},
		},
		{
			name: "get precondition", action: "GetItem",
			payload: map[string]any{"path": "/a.md", "precondition": map[string]any{"expectedVersionId": "v1"}},
		},
		{
			name: "list itemId", action: "ListItems",
			payload: map[string]any{"itemId": "item_1"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			m := rpcTestMock()
			m.put("/a.md", "content", true)
			payload := validRPCPayload()
			for key, value := range test.payload {
				payload[key] = value
			}
			requireValidationError(t, callRPC(t, m, test.action, payload))
		})
	}
}

func TestListPaginationUsesOpaqueBoundTokens(t *testing.T) {
	m := rpcTestMock()
	m.put("/bulk/a.md", "a", true)
	m.put("/bulk/b.md", "b", true)
	m.put("/bulk/c.md", "c", true)
	payload := validRPCPayload()
	payload["pathPrefix"] = "/bulk/"
	payload["limit"] = 2
	payload["view"] = "basic"

	first := callRPC(t, m, "ListItems", payload)
	if first.Code != http.StatusOK {
		t.Fatalf("first page status = %d; body=%s", first.Code, first.Body.String())
	}
	var firstPage struct {
		Items     []itemJSON `json:"items"`
		NextToken string     `json:"nextToken"`
	}
	if err := json.Unmarshal(first.Body.Bytes(), &firstPage); err != nil {
		t.Fatal(err)
	}
	if len(firstPage.Items) != 2 || firstPage.NextToken == "" {
		t.Fatalf("first page = %#v", firstPage)
	}
	if strings.Contains(firstPage.NextToken, "/bulk/") {
		t.Fatalf("next token exposes last path: %q", firstPage.NextToken)
	}

	continuation := clonePayload(t, payload)
	continuation["nextToken"] = firstPage.NextToken
	second := callRPC(t, m, "ListItems", continuation)
	if second.Code != http.StatusOK {
		t.Fatalf("second page status = %d; body=%s", second.Code, second.Body.String())
	}
	var secondPage struct {
		Items     []itemJSON `json:"items"`
		NextToken string     `json:"nextToken"`
	}
	if err := json.Unmarshal(second.Body.Bytes(), &secondPage); err != nil {
		t.Fatal(err)
	}
	if len(secondPage.Items) != 1 || secondPage.Items[0].Path != "/bulk/c.md" || secondPage.NextToken != "" {
		t.Fatalf("second page = %#v", secondPage)
	}

	mismatches := map[string]func(map[string]any){
		"unknown token": func(p map[string]any) { p["nextToken"] = "never-issued" },
		"different store": func(p map[string]any) {
			p["memoryStoreName"] = "other"
		},
		"different scope": func(p map[string]any) {
			p["scope"] = map[string]any{"appId": "app", "tenantId": "tenant", "agentId": "agent", "runId": "other"}
		},
		"different prefix": func(p map[string]any) { p["pathPrefix"] = "/other/" },
		"different view":   func(p map[string]any) { p["view"] = "full" },
	}
	for name, mutate := range mismatches {
		t.Run(name, func(t *testing.T) {
			replay := clonePayload(t, continuation)
			mutate(replay)
			requireValidationError(t, callRPC(t, m, "ListItems", replay))
		})
	}
}

package client

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"agent-memory-fuse/internal/credential"

	"github.com/aliyun/tablestore-agent-storage-sdk/go/agentstorage"
)

func receiveMutationWithin[T any](t *testing.T, ch <-chan T, description string) T {
	t.Helper()
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	select {
	case value := <-ch:
		return value
	case <-timer.C:
		t.Fatalf("timed out waiting for %s", description)
		var zero T
		return zero
	}
}

type mutationRoundTripFunc func(*http.Request) (*http.Response, error)

func (f mutationRoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

type mutationReadErrorBody struct{ err error }

func (b mutationReadErrorBody) Read([]byte) (int, error) { return 0, b.err }
func (mutationReadErrorBody) Close() error               { return nil }

type mutationNetworkError struct{}

func (mutationNetworkError) Error() string   { return "controlled network failure" }
func (mutationNetworkError) Timeout() bool   { return false }
func (mutationNetworkError) Temporary() bool { return false }

func mutationTestItem(itemID, itemPath, content, version string, seq int64) map[string]any {
	return map[string]any{
		"type":             "memoryfile",
		"itemId":           itemID,
		"path":             itemPath,
		"content":          content,
		"contentSha256":    "sha-present",
		"contentSizeBytes": len([]byte(content)),
		"latestSeq":        seq,
		"latestVersionId":  version,
		"createdAt":        "2026-08-24T00:00:00Z",
		"updatedAt":        "2026-08-24T00:00:01Z",
	}
}

func mutationTestBasicItem(itemID, itemPath, version string, seq int64) map[string]any {
	item := mutationTestItem(itemID, itemPath, "content", version, seq)
	delete(item, "content")
	return item
}

func writeMutationTestError(w http.ResponseWriter, status int, code, requestID string) {
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"code": code, "message": "safe test service error", "requestId": requestID,
	})
}

func TestClientMutationRetryGate(t *testing.T) {
	t.Run("429 then success", func(t *testing.T) {
		var actions []string
		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			actions = append(actions, r.URL.Path)
			if len(actions) == 1 {
				writeMutationTestError(w, http.StatusTooManyRequests, "THROTTLED", "req-throttle")
				return
			}
			_ = json.NewEncoder(w).Encode(mutationTestItem("mem_a", "/a.md", "new", "version-new", 2))
		}))
		defer server.Close()

		config := newAPIKeyConfig(server)
		config.RetryBackoff = []time.Duration{time.Nanosecond}
		c, err := New(config)
		if err != nil {
			t.Fatal(err)
		}
		memory, err := c.Update(context.Background(), "mem_a", []byte("new"), "version-old")
		if err != nil || memory.MemoryID != "mem_a" || memory.LatestVersionID != "version-new" {
			t.Fatalf("memory=%+v err=%v", memory, err)
		}
		if want := []string{"/UpdateItem", "/UpdateItem"}; !reflect.DeepEqual(actions, want) {
			t.Fatalf("actions=%v want %v", actions, want)
		}
	})

	t.Run("lock reconciles without replay", func(t *testing.T) {
		var actions []string
		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			actions = append(actions, r.URL.Path)
			switch r.URL.Path {
			case "/UpdateItem":
				writeMutationTestError(w, http.StatusConflict, "LOCK_CONFLICT", "req-lock")
			case "/GetItem":
				_ = json.NewEncoder(w).Encode(mutationTestItem("mem_a", "/a.md", "new", "opaque-next", 2))
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
		memory, err := c.Update(context.Background(), "mem_a", []byte("new"), "opaque-before")
		if err != nil || memory.Content != "new" || memory.LatestVersionID != "opaque-next" {
			t.Fatalf("memory=%+v err=%v", memory, err)
		}
		if want := []string{"/UpdateItem", "/GetItem"}; !reflect.DeepEqual(actions, want) {
			t.Fatalf("actions=%v want %v", actions, want)
		}
	})

	t.Run("exhausted 429 does not get", func(t *testing.T) {
		var actions []string
		requestID := 0
		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			actions = append(actions, r.URL.Path)
			requestID++
			writeMutationTestError(w, http.StatusTooManyRequests, "THROTTLED", "req-throttle-"+string(rune('0'+requestID)))
		}))
		defer server.Close()

		config := newAPIKeyConfig(server)
		config.RetryBackoff = []time.Duration{time.Nanosecond}
		c, err := New(config)
		if err != nil {
			t.Fatal(err)
		}
		memory, err := c.Update(context.Background(), "mem_a", []byte("new"), "version-old")
		var apiErr *APIError
		if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusTooManyRequests || apiErr.RequestID != "req-throttle-2" {
			t.Fatalf("memory=%+v err=%v", memory, err)
		}
		if memory != (Memory{}) {
			t.Fatalf("failed mutation memory=%+v want zero", memory)
		}
		if want := []string{"/UpdateItem", "/UpdateItem"}; !reflect.DeepEqual(actions, want) {
			t.Fatalf("actions=%v want %v", actions, want)
		}
	})

	t.Run("lock does one logical get without replay", func(t *testing.T) {
		var actions []string
		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			actions = append(actions, r.URL.Path)
			switch r.URL.Path {
			case "/UpdateItem":
				writeMutationTestError(w, http.StatusConflict, "LOCK_CONFLICT", "req-lock-final")
			case "/GetItem":
				_ = json.NewEncoder(w).Encode(mutationTestItem("mem_a", "/renamed.md", "new", "opaque-advanced", 2))
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
		memory, err := c.Update(context.Background(), "mem_a", []byte("new"), "opaque-before")
		if err != nil || memory.Path != "/renamed.md" || memory.Content != "new" || memory.LatestVersionID != "opaque-advanced" {
			t.Fatalf("memory=%+v err=%v", memory, err)
		}
		if want := []string{"/UpdateItem", "/GetItem"}; !reflect.DeepEqual(actions, want) {
			t.Fatalf("actions=%v want %v", actions, want)
		}
	})
}

func TestClientDeleteReconcilesLockConflictMissingWithoutReplay(t *testing.T) {
	var actions []string
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		actions = append(actions, r.URL.Path)
		switch r.URL.Path {
		case "/DeleteItem":
			writeMutationTestError(w, http.StatusConflict, "LOCK_CONFLICT", "req-lock-delete")
		case "/GetItem":
			writeMutationTestError(w, http.StatusNotFound, "NOT_FOUND", "req-get")
		default:
			t.Fatalf("unexpected action %q", r.URL.Path)
		}
	}))
	defer server.Close()

	config := newAPIKeyConfig(server)
	config.RetryBackoff = []time.Duration{time.Nanosecond, time.Nanosecond}
	c, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Delete(context.Background(), "mem_a", "opaque-before"); err != nil {
		t.Fatal(err)
	}
	if want := []string{"/DeleteItem", "/GetItem"}; !reflect.DeepEqual(actions, want) {
		t.Fatalf("actions=%v want %v", actions, want)
	}
}

func TestClientRenameReconcilesLockConflictWithoutReplay(t *testing.T) {
	for _, test := range []struct {
		name        string
		currentPath string
		wantSuccess bool
	}{
		{name: "unchanged source preserves lock conflict", currentPath: "/a.md"},
		{name: "target no-op succeeds", currentPath: "/b.md", wantSuccess: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			var actions []string
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				actions = append(actions, r.URL.Path)
				switch r.URL.Path {
				case "/UpdateItem":
					writeMutationTestError(w, http.StatusConflict, "LOCK_CONFLICT", "req-lock-rename")
				case "/GetItem":
					_ = json.NewEncoder(w).Encode(mutationTestBasicItem("mem_a", test.currentPath, "opaque-before", 7))
				default:
					t.Fatalf("unexpected action %q", r.URL.Path)
				}
			}))
			defer server.Close()

			config := newAPIKeyConfig(server)
			config.RetryBackoff = []time.Duration{time.Nanosecond, time.Nanosecond}
			c, err := New(config)
			if err != nil {
				t.Fatal(err)
			}
			memory, err := c.Rename(context.Background(), "mem_a", "b.md", false, "opaque-before")
			if test.wantSuccess {
				if err != nil || memory.MemoryID != "mem_a" || memory.Path != "/b.md" {
					t.Fatalf("memory=%+v err=%v", memory, err)
				}
			} else {
				var apiErr *APIError
				if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusConflict || apiErr.Code != "LOCK_CONFLICT" || apiErr.RequestID != "req-lock-rename" {
					t.Fatalf("memory=%+v err=%v want original LOCK_CONFLICT requestId", memory, err)
				}
				if memory != (Memory{}) {
					t.Fatalf("memory=%+v want zero", memory)
				}
			}
			if want := []string{"/UpdateItem", "/GetItem"}; !reflect.DeepEqual(actions, want) {
				t.Fatalf("actions=%v want %v", actions, want)
			}
		})
	}
}

func TestClientMutationDoesNotReplayAmbiguousServiceErrors(t *testing.T) {
	for _, status := range []int{http.StatusInternalServerError, http.StatusServiceUnavailable, http.StatusGatewayTimeout} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			var actions []string
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				actions = append(actions, r.URL.Path)
				switch r.URL.Path {
				case "/UpdateItem":
					writeMutationTestError(w, status, "AMBIGUOUS", "req-ambiguous")
				case "/GetItem":
					_ = json.NewEncoder(w).Encode(mutationTestItem("mem_a", "/externally-renamed.md", "new", "opaque-next", 2))
				default:
					t.Fatalf("unexpected action %q", r.URL.Path)
				}
			}))
			defer server.Close()

			config := newAPIKeyConfig(server)
			config.RetryBackoff = []time.Duration{time.Nanosecond, time.Nanosecond}
			c, err := New(config)
			if err != nil {
				t.Fatal(err)
			}
			memory, err := c.Update(context.Background(), "mem_a", []byte("new"), "opaque-before")
			if err != nil || memory.Path != "/externally-renamed.md" || memory.LatestVersionID != "opaque-next" {
				t.Fatalf("memory=%+v err=%v", memory, err)
			}
			if want := []string{"/UpdateItem", "/GetItem"}; !reflect.DeepEqual(actions, want) {
				t.Fatalf("actions=%v want %v", actions, want)
			}
		})
	}
}

func TestClientMutationReconcileFinalStates(t *testing.T) {
	type testCase struct {
		name        string
		operation   string
		getStatus   int
		getCode     string
		getItem     map[string]any
		wantCode    string
		wantSuccess bool
	}
	tests := []testCase{
		{name: "create not found preserves original", operation: "create", getStatus: http.StatusNotFound, getCode: "NOT_FOUND", wantCode: "AMBIGUOUS"},
		{name: "create same content without owner preserves original", operation: "create", getItem: mutationTestItem("mem_created", "/a.md", "new", "opaque-created", 1), wantCode: "AMBIGUOUS"},
		{name: "create different content is path exists", operation: "create", getItem: mutationTestItem("mem_other", "/a.md", "different", "opaque-other", 9), wantCode: "PATH_EXISTS"},
		{name: "create inexact not found preserves original", operation: "create", getStatus: http.StatusNotFound, getCode: "GONE", wantCode: "AMBIGUOUS"},
		{name: "update no-op succeeds across concurrent rename", operation: "update", getItem: mutationTestItem("mem_a", "/external.md", "new", "opaque-before", 7), wantSuccess: true},
		{name: "update advanced same content succeeds across rename", operation: "update", getItem: mutationTestItem("mem_a", "/external.md", "new", "opaque-after", 8), wantSuccess: true},
		{name: "update unchanged different content is precondition", operation: "update", getItem: mutationTestItem("mem_a", "/a.md", "different", "opaque-before", 7), wantCode: "PRECONDITION_FAILED"},
		{name: "update advanced different content is precondition", operation: "update", getItem: mutationTestItem("mem_a", "/a.md", "different", "opaque-after", 8), wantCode: "PRECONDITION_FAILED"},
		{name: "update not found is precondition", operation: "update", getStatus: http.StatusNotFound, getCode: "NOT_FOUND", wantCode: "PRECONDITION_FAILED"},
		{name: "update inexact not found preserves original", operation: "update", getStatus: http.StatusNotFound, getCode: "GONE", wantCode: "AMBIGUOUS"},
		{name: "rename no-op succeeds", operation: "rename", getItem: mutationTestBasicItem("mem_a", "/b.md", "opaque-before", 7), wantSuccess: true},
		{name: "rename target advanced succeeds", operation: "rename", getItem: mutationTestBasicItem("mem_a", "/b.md", "opaque-after", 8), wantSuccess: true},
		{name: "rename unchanged source is precondition", operation: "rename", getItem: mutationTestBasicItem("mem_a", "/a.md", "opaque-before", 7), wantCode: "PRECONDITION_FAILED"},
		{name: "rename other target advanced is precondition", operation: "rename", getItem: mutationTestBasicItem("mem_a", "/other.md", "opaque-after", 8), wantCode: "PRECONDITION_FAILED"},
		{name: "rename not found is precondition", operation: "rename", getStatus: http.StatusNotFound, getCode: "NOT_FOUND", wantCode: "PRECONDITION_FAILED"},
		{name: "delete same version preserves original", operation: "delete", getItem: mutationTestBasicItem("mem_a", "/a.md", "opaque-before", 7), wantCode: "AMBIGUOUS"},
		{name: "delete not found succeeds", operation: "delete", getStatus: http.StatusNotFound, getCode: "NOT_FOUND", wantSuccess: true},
		{name: "delete inexact not found preserves original", operation: "delete", getStatus: http.StatusNotFound, getCode: "GONE", wantCode: "AMBIGUOUS"},
		{name: "delete advanced is precondition", operation: "delete", getItem: mutationTestBasicItem("mem_a", "/a.md", "opaque-after", 8), wantCode: "PRECONDITION_FAILED"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var calls []recordedCall
			server := newRecordingServer(t, &calls, func(action string, w http.ResponseWriter) {
				if action != "/GetItem" {
					writeMutationTestError(w, http.StatusServiceUnavailable, "AMBIGUOUS", "mutation-request")
					return
				}
				if test.getStatus != 0 {
					writeMutationTestError(w, test.getStatus, test.getCode, "recheck-request")
					return
				}
				_ = json.NewEncoder(w).Encode(test.getItem)
			})

			config := newAPIKeyConfig(server)
			config.RetryBackoff = []time.Duration{}
			c, err := New(config)
			if err != nil {
				t.Fatal(err)
			}

			var memory Memory
			switch test.operation {
			case "create":
				memory, err = c.Create(context.Background(), "a.md", []byte("new"))
			case "update":
				memory, err = c.Update(context.Background(), "mem_a", []byte("new"), "opaque-before")
			case "rename":
				memory, err = c.Rename(context.Background(), "mem_a", "b.md", false, "opaque-before")
			case "delete":
				err = c.Delete(context.Background(), "mem_a", "opaque-before")
			default:
				t.Fatalf("unknown operation %q", test.operation)
			}

			if test.wantSuccess {
				if err != nil {
					t.Fatalf("err=%v want success", err)
				}
				if test.operation != "delete" {
					if memory.MemoryID != test.getItem["itemId"] || memory.Path != test.getItem["path"] || memory.LatestVersionID != test.getItem["latestVersionId"] || memory.ContentSHA256 == "" || memory.LatestSeq <= 0 {
						t.Fatalf("memory=%+v does not preserve authoritative item", memory)
					}
				}
			} else {
				var apiErr *APIError
				if !errors.As(err, &apiErr) || apiErr.Code != test.wantCode || apiErr.RequestID != "mutation-request" {
					t.Fatalf("memory=%+v err=%v want code=%s requestId=mutation-request", memory, err, test.wantCode)
				}
				if memory != (Memory{}) {
					t.Fatalf("failed mutation memory=%+v want zero", memory)
				}
			}

			if len(calls) != 2 || calls[1].action != "/GetItem" {
				t.Fatalf("calls=%+v want one write and one logical Get", calls)
			}
			getBody := calls[1].body
			switch test.operation {
			case "create":
				if getBody["path"] != "/a.md" || getBody["view"] != "full" || getBody["itemId"] != nil {
					t.Fatalf("create Get body=%v", getBody)
				}
			case "update":
				if getBody["itemId"] != "mem_a" || getBody["view"] != "full" || getBody["path"] != nil {
					t.Fatalf("update Get body=%v", getBody)
				}
			case "rename", "delete":
				if getBody["itemId"] != "mem_a" || getBody["view"] != nil || getBody["path"] != nil {
					t.Fatalf("%s Get body=%v", test.operation, getBody)
				}
			}
		})
	}
}

func TestClientMutationReconcileReadUsesSafeRetryBudget(t *testing.T) {
	t.Run("retry then success", func(t *testing.T) {
		var actions []string
		getAttempts := 0
		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			actions = append(actions, r.URL.Path)
			if r.URL.Path == "/UpdateItem" {
				writeMutationTestError(w, http.StatusInternalServerError, "AMBIGUOUS", "mutation-request")
				return
			}
			getAttempts++
			if getAttempts == 1 {
				writeMutationTestError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "get-request-1")
				return
			}
			_ = json.NewEncoder(w).Encode(mutationTestItem("mem_a", "/a.md", "new", "opaque-after", 2))
		}))
		defer server.Close()

		config := newAPIKeyConfig(server)
		config.RetryBackoff = []time.Duration{time.Nanosecond}
		c, err := New(config)
		if err != nil {
			t.Fatal(err)
		}
		memory, err := c.Update(context.Background(), "mem_a", []byte("new"), "opaque-before")
		if err != nil || memory.LatestVersionID != "opaque-after" {
			t.Fatalf("memory=%+v err=%v", memory, err)
		}
		if want := []string{"/UpdateItem", "/GetItem", "/GetItem"}; !reflect.DeepEqual(actions, want) {
			t.Fatalf("actions=%v want %v", actions, want)
		}
	})

	t.Run("exhausted preserves original", func(t *testing.T) {
		var actions []string
		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			actions = append(actions, r.URL.Path)
			if r.URL.Path == "/UpdateItem" {
				writeMutationTestError(w, http.StatusInternalServerError, "AMBIGUOUS", "mutation-request")
				return
			}
			writeMutationTestError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "recheck-request")
		}))
		defer server.Close()

		config := newAPIKeyConfig(server)
		config.RetryBackoff = []time.Duration{time.Nanosecond}
		c, err := New(config)
		if err != nil {
			t.Fatal(err)
		}
		memory, err := c.Update(context.Background(), "mem_a", []byte("new"), "opaque-before")
		var apiErr *APIError
		if !errors.As(err, &apiErr) || apiErr.Code != "AMBIGUOUS" || apiErr.RequestID != "mutation-request" || memory != (Memory{}) {
			t.Fatalf("memory=%+v err=%v", memory, err)
		}
		if want := []string{"/UpdateItem", "/GetItem", "/GetItem"}; !reflect.DeepEqual(actions, want) {
			t.Fatalf("actions=%v want %v", actions, want)
		}
	})
}

func TestClientMutationDirectNoOpResponsesSucceed(t *testing.T) {
	tests := []struct {
		name      string
		operation string
		response  map[string]any
	}{
		{name: "update", operation: "update", response: mutationTestItem("mem_a", "/concurrently-renamed.md", "new", "opaque-before", 7)},
		{name: "rename", operation: "rename", response: mutationTestBasicItem("mem_a", "/b.md", "opaque-before", 7)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var calls []recordedCall
			server := newRecordingServer(t, &calls, func(_ string, w http.ResponseWriter) {
				_ = json.NewEncoder(w).Encode(test.response)
			})

			config := newAPIKeyConfig(server)
			config.RetryBackoff = []time.Duration{}
			c, err := New(config)
			if err != nil {
				t.Fatal(err)
			}

			var memory Memory
			switch test.operation {
			case "update":
				memory, err = c.Update(context.Background(), "mem_a", []byte("new"), "opaque-before")
			case "rename":
				memory, err = c.Rename(context.Background(), "mem_a", "b.md", false, "opaque-before")
			}
			if err != nil || memory.MemoryID != "mem_a" || memory.LatestVersionID != "opaque-before" {
				t.Fatalf("memory=%+v err=%v want direct no-op success", memory, err)
			}
			if len(calls) != 1 || calls[0].action != "/UpdateItem" {
				t.Fatalf("calls=%+v want one UpdateItem", calls)
			}
		})
	}
}

func TestClientMutationSemanticResponsesReconcile(t *testing.T) {
	type testCase struct {
		name          string
		operation     string
		direct        map[string]any
		authoritative map[string]any
		getNotFound   bool
	}
	validCreate := mutationTestItem("mem_created", "/a.md", "new", "created-version", 1)
	validUpdate := mutationTestItem("mem_a", "/external.md", "new", "opaque-after", 2)
	validRename := mutationTestBasicItem("mem_a", "/b.md", "opaque-after", 2)
	updateNoOp := mutationTestItem("mem_a", "/concurrently-renamed.md", "new", "opaque-before", 7)
	renameNoOp := mutationTestBasicItem("mem_a", "/b.md", "opaque-before", 7)
	tests := []testCase{
		{name: "create wrong path", operation: "create", direct: mutationTestItem("mem_created", "/wrong.md", "new", "created-version", 1), authoritative: validCreate},
		{name: "update missing path", operation: "update", direct: cloneMutationItemWithout(validUpdate, "path"), authoritative: validUpdate},
		{name: "update full content omitted", operation: "update", direct: cloneMutationItemWithout(validUpdate, "content"), authoritative: validUpdate},
		{name: "update wrong item", operation: "update", direct: mutationTestItem("mem_other", "/external.md", "new", "opaque-after", 2), authoritative: validUpdate},
		{name: "update malformed response reconciles no-op", operation: "update", direct: cloneMutationItemWithout(updateNoOp, "path"), authoritative: updateNoOp},
		{name: "rename missing version", operation: "rename", direct: cloneMutationItemWithout(validRename, "latestVersionId"), authoritative: validRename},
		{name: "rename malformed response reconciles no-op", operation: "rename", direct: cloneMutationItemWithout(renameNoOp, "latestVersionId"), authoritative: renameNoOp},
		{name: "rename wrong target", operation: "rename", direct: mutationTestBasicItem("mem_a", "/wrong.md", "opaque-after", 2), authoritative: validRename},
		{name: "delete false", operation: "delete", direct: map[string]any{"type": "memoryfile", "itemId": "mem_a", "deleted": false}, getNotFound: true},
		{name: "delete wrong item", operation: "delete", direct: map[string]any{"type": "memoryfile", "itemId": "mem_other", "deleted": true}, getNotFound: true},
		{name: "delete ranged", operation: "delete", direct: map[string]any{"type": "memoryfile", "taskId": "task-a", "status": "pending"}, getNotFound: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var actions []string
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				actions = append(actions, r.URL.Path)
				if r.URL.Path != "/GetItem" {
					_ = json.NewEncoder(w).Encode(test.direct)
					return
				}
				if test.getNotFound {
					writeMutationTestError(w, http.StatusNotFound, "NOT_FOUND", "recheck-request")
					return
				}
				_ = json.NewEncoder(w).Encode(test.authoritative)
			}))
			defer server.Close()

			config := newAPIKeyConfig(server)
			config.RetryBackoff = []time.Duration{}
			c, err := New(config)
			if err != nil {
				t.Fatal(err)
			}
			var memory Memory
			switch test.operation {
			case "create":
				memory, err = c.Create(context.Background(), "a.md", []byte("new"))
			case "update":
				memory, err = c.Update(context.Background(), "mem_a", []byte("new"), "opaque-before")
			case "rename":
				memory, err = c.Rename(context.Background(), "mem_a", "b.md", false, "opaque-before")
			case "delete":
				err = c.Delete(context.Background(), "mem_a", "opaque-before")
			}
			if err != nil {
				t.Fatalf("memory=%+v err=%v want reconciled success", memory, err)
			}
			if test.operation != "delete" && (memory.MemoryID == "" || memory.Path == "" || memory.LatestVersionID == "" || memory.ContentSHA256 == "") {
				t.Fatalf("memory=%+v want authoritative complete memory", memory)
			}
			if len(actions) != 2 || actions[1] != "/GetItem" {
				t.Fatalf("actions=%v want malformed write followed by Get", actions)
			}
		})
	}
}

func TestClientMutationReconcileCreateRequiresOwnerCandidate(t *testing.T) {
	type testCase struct {
		name              string
		directStatus      int
		direct            map[string]any
		authoritative     map[string]any
		wantCode          string
		wantKind          string
		wantSuccess       bool
		wantMutationReqID bool
	}
	candidateA := cloneMutationItemWithout(mutationTestItem("mem_a", "/a.md", "new", "opaque-candidate", 1), "latestVersionId")
	tests := []testCase{
		{
			name: "ambiguous write has no owner even when content matches", directStatus: http.StatusInternalServerError,
			authoritative: mutationTestItem("mem_a", "/a.md", "new", "opaque-current", 1),
			wantCode:      "AMBIGUOUS", wantMutationReqID: true,
		},
		{
			name: "semantic response missing item id has no owner", direct: cloneMutationItemWithout(mutationTestItem("mem_a", "/a.md", "new", "opaque-candidate", 1), "itemId"),
			authoritative: mutationTestItem("mem_a", "/a.md", "new", "opaque-current", 1),
			wantKind:      "response_semantic",
		},
		{
			name: "candidate and authoritative owners differ", direct: candidateA,
			authoritative: mutationTestItem("mem_b", "/a.md", "new", "opaque-current", 1),
			wantCode:      "PATH_EXISTS",
		},
		{
			name: "candidate and authoritative owner match", direct: candidateA,
			authoritative: mutationTestItem("mem_a", "/a.md", "new", "opaque-current", 1),
			wantSuccess:   true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var actions []string
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				actions = append(actions, r.URL.Path)
				if r.URL.Path == "/AddItem" {
					if test.directStatus != 0 {
						writeMutationTestError(w, test.directStatus, "AMBIGUOUS", "mutation-request")
						return
					}
					_ = json.NewEncoder(w).Encode(test.direct)
					return
				}
				_ = json.NewEncoder(w).Encode(test.authoritative)
			}))
			defer server.Close()

			config := newAPIKeyConfig(server)
			config.RetryBackoff = []time.Duration{}
			c, err := New(config)
			if err != nil {
				t.Fatal(err)
			}
			memory, err := c.Create(context.Background(), "a.md", []byte("new"))
			if test.wantSuccess {
				if err != nil || memory.MemoryID != "mem_a" || memory.Content != "new" {
					t.Fatalf("memory=%+v err=%v want candidate-owned success", memory, err)
				}
			} else if test.wantKind != "" {
				if SDKClientErrorKind(err) != test.wantKind || memory != (Memory{}) {
					t.Fatalf("memory=%+v err=%v kind=%q want %q", memory, err, SDKClientErrorKind(err), test.wantKind)
				}
			} else {
				var apiErr *APIError
				if !errors.As(err, &apiErr) || apiErr.Code != test.wantCode || memory != (Memory{}) {
					t.Fatalf("memory=%+v err=%v want code=%s", memory, err, test.wantCode)
				}
				if test.wantMutationReqID && apiErr.RequestID != "mutation-request" {
					t.Fatalf("requestId=%q want mutation-request", apiErr.RequestID)
				}
			}
			if want := []string{"/AddItem", "/GetItem"}; !reflect.DeepEqual(actions, want) {
				t.Fatalf("actions=%v want %v", actions, want)
			}
		})
	}
}

func cloneMutationItemWithout(item map[string]any, field string) map[string]any {
	clone := make(map[string]any, len(item))
	for key, value := range item {
		if key != field {
			clone[key] = value
		}
	}
	return clone
}

func TestClientMutationSemanticFailurePreservesSafeOriginal(t *testing.T) {
	var actions []string
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		actions = append(actions, r.URL.Path)
		if r.URL.Path == "/AddItem" {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"type": "memoryfile", "path": "/wrong.md", "content": "secret-content-must-not-leak",
			})
			return
		}
		writeMutationTestError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "recheck-request")
	}))
	defer server.Close()

	config := newAPIKeyConfig(server)
	config.RetryBackoff = []time.Duration{}
	c, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	memory, err := c.Create(context.Background(), "sensitive-path.md", []byte("secret-content-must-not-leak"))
	if err == nil || SDKClientErrorKind(err) != "response_semantic" {
		t.Fatalf("memory=%+v err=%v kind=%q", memory, err, SDKClientErrorKind(err))
	}
	if memory != (Memory{}) {
		t.Fatalf("memory=%+v want zero", memory)
	}
	if rendered := err.Error(); rendered == "" || containsAny(rendered, "sensitive-path", "secret-content", "wrong.md") {
		t.Fatalf("semantic error leaked target data: %q", rendered)
	}
	if want := []string{"/AddItem", "/GetItem"}; !reflect.DeepEqual(actions, want) {
		t.Fatalf("actions=%v want %v", actions, want)
	}
}

func containsAny(value string, needles ...string) bool {
	for _, needle := range needles {
		if len(needle) > 0 && len(value) >= len(needle) {
			for i := 0; i+len(needle) <= len(value); i++ {
				if value[i:i+len(needle)] == needle {
					return true
				}
			}
		}
	}
	return false
}

func TestClientMutationSemanticAllowsEmptyFullContent(t *testing.T) {
	var actions []string
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		actions = append(actions, r.URL.Path)
		_ = json.NewEncoder(w).Encode(mutationTestItem("mem_empty", "/empty.md", "", "opaque-empty", 1))
	}))
	defer server.Close()

	c, err := New(newAPIKeyConfig(server))
	if err != nil {
		t.Fatal(err)
	}
	memory, err := c.Create(context.Background(), "empty.md", []byte{})
	if err != nil || memory.MemoryID != "mem_empty" || memory.Content != "" || memory.ContentSizeBytes != 0 {
		t.Fatalf("memory=%+v err=%v", memory, err)
	}
	if want := []string{"/AddItem"}; !reflect.DeepEqual(actions, want) {
		t.Fatalf("actions=%v want %v", actions, want)
	}
}

func TestClientMutationReconcileKnownClientErrors(t *testing.T) {
	t.Run("response decode", func(t *testing.T) {
		var actions []string
		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			actions = append(actions, r.URL.Path)
			if r.URL.Path == "/UpdateItem" {
				_, _ = w.Write([]byte(`{"unterminated"`))
				return
			}
			_ = json.NewEncoder(w).Encode(mutationTestItem("mem_a", "/a.md", "new", "opaque-after", 2))
		}))
		defer server.Close()

		config := newAPIKeyConfig(server)
		config.RetryBackoff = []time.Duration{}
		c, err := New(config)
		if err != nil {
			t.Fatal(err)
		}
		memory, err := c.Update(context.Background(), "mem_a", []byte("new"), "opaque-before")
		if err != nil || memory.LatestVersionID != "opaque-after" {
			t.Fatalf("memory=%+v err=%v", memory, err)
		}
		if want := []string{"/UpdateItem", "/GetItem"}; !reflect.DeepEqual(actions, want) {
			t.Fatalf("actions=%v want %v", actions, want)
		}
	})

	t.Run("http client timeout with live caller", func(t *testing.T) {
		var mu sync.Mutex
		var actions []string
		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			actions = append(actions, r.URL.Path)
			mu.Unlock()
			if r.URL.Path == "/UpdateItem" {
				time.Sleep(300 * time.Millisecond)
				_ = json.NewEncoder(w).Encode(mutationTestItem("mem_a", "/a.md", "new", "opaque-after", 2))
				return
			}
			_ = json.NewEncoder(w).Encode(mutationTestItem("mem_a", "/a.md", "new", "opaque-after", 2))
		}))
		defer server.Close()

		config := newAPIKeyConfig(server)
		config.HTTPClient = &http.Client{Transport: server.Client().Transport, Timeout: 100 * time.Millisecond}
		config.RetryBackoff = []time.Duration{}
		c, err := New(config)
		if err != nil {
			t.Fatal(err)
		}
		memory, err := c.Update(context.Background(), "mem_a", []byte("new"), "opaque-before")
		if err != nil || memory.LatestVersionID != "opaque-after" {
			t.Fatalf("memory=%+v err=%v", memory, err)
		}
		mu.Lock()
		got := append([]string(nil), actions...)
		mu.Unlock()
		if want := []string{"/UpdateItem", "/GetItem"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("actions=%v want %v", got, want)
		}
	})
}

func TestClientMutationReconcileAdditionalSDKClientErrors(t *testing.T) {
	tests := []struct {
		name        string
		firstResult func(*http.Request) (*http.Response, error)
	}{
		{
			name: "response read",
			firstResult: func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     make(http.Header),
					Body:       mutationReadErrorBody{err: errors.New("controlled response read failure")},
					Request:    req,
				}, nil
			},
		},
		{
			name: "transport network",
			firstResult: func(*http.Request) (*http.Response, error) {
				return nil, mutationNetworkError{}
			},
		},
		{
			name: "transport other",
			firstResult: func(*http.Request) (*http.Response, error) {
				return nil, errors.New("controlled non-network transport failure")
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var mu sync.Mutex
			var actions []string
			attempt := 0
			transport := mutationRoundTripFunc(func(req *http.Request) (*http.Response, error) {
				mu.Lock()
				actions = append(actions, req.URL.Path)
				attempt++
				currentAttempt := attempt
				mu.Unlock()
				if currentAttempt == 1 {
					return test.firstResult(req)
				}
				body, err := json.Marshal(mutationTestItem("mem_a", "/a.md", "new", "opaque-after", 2))
				if err != nil {
					return nil, err
				}
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     make(http.Header),
					Body:       io.NopCloser(bytes.NewReader(body)),
					Request:    req,
				}, nil
			})
			c, err := New(Config{
				Endpoint: "https://example.invalid", Instance: "instance-a", APIKey: "key", StoreName: "memstore-a",
				HTTPClient: &http.Client{Transport: transport}, RetryBackoff: []time.Duration{},
			})
			if err != nil {
				t.Fatal(err)
			}
			memory, err := c.Update(context.Background(), "mem_a", []byte("new"), "opaque-before")
			if err != nil || memory.MemoryID != "mem_a" || memory.LatestVersionID != "opaque-after" {
				t.Fatalf("memory=%+v err=%v", memory, err)
			}
			mu.Lock()
			gotActions := append([]string(nil), actions...)
			mu.Unlock()
			if want := []string{"/UpdateItem", "/GetItem"}; !reflect.DeepEqual(gotActions, want) {
				t.Fatalf("actions=%v want %v", gotActions, want)
			}
		})
	}
}

func TestClientMutationTransportUnchangedStateIsPrecondition(t *testing.T) {
	for _, test := range []struct {
		name          string
		operation     string
		authoritative map[string]any
	}{
		{name: "update", operation: "update", authoritative: mutationTestItem("mem_a", "/a.md", "old", "opaque-before", 7)},
		{name: "rename", operation: "rename", authoritative: mutationTestBasicItem("mem_a", "/a.md", "opaque-before", 7)},
	} {
		t.Run(test.name, func(t *testing.T) {
			var actions []string
			transport := mutationRoundTripFunc(func(req *http.Request) (*http.Response, error) {
				actions = append(actions, req.URL.Path)
				if len(actions) == 1 {
					return nil, mutationNetworkError{}
				}
				body, err := json.Marshal(test.authoritative)
				if err != nil {
					return nil, err
				}
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     make(http.Header),
					Body:       io.NopCloser(bytes.NewReader(body)),
					Request:    req,
				}, nil
			})
			c, err := New(Config{
				Endpoint: "https://example.invalid", Instance: "instance-a", APIKey: "key", StoreName: "memstore-a",
				HTTPClient: &http.Client{Transport: transport}, RetryBackoff: []time.Duration{},
			})
			if err != nil {
				t.Fatal(err)
			}
			var memory Memory
			switch test.operation {
			case "update":
				memory, err = c.Update(context.Background(), "mem_a", []byte("new"), "opaque-before")
			case "rename":
				memory, err = c.Rename(context.Background(), "mem_a", "b.md", false, "opaque-before")
			default:
				t.Fatalf("unknown operation %q", test.operation)
			}
			var apiErr *APIError
			if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusConflict || apiErr.Code != "PRECONDITION_FAILED" || memory != (Memory{}) {
				t.Fatalf("memory=%+v err=%v want PRECONDITION_FAILED", memory, err)
			}
			if want := []string{"/UpdateItem", "/GetItem"}; !reflect.DeepEqual(actions, want) {
				t.Fatalf("actions=%v want %v", actions, want)
			}
		})
	}
}

func TestMutationReconciliationWhitelistIncludesSignedResponseValidationErrors(t *testing.T) {
	body := []byte(`{}`)
	md5Sum := md5.Sum(body)
	for _, test := range []struct {
		name    string
		kind    string
		headers http.Header
	}{
		{name: "response integrity", kind: "response_integrity", headers: make(http.Header)},
		{name: "response auth", kind: "response_auth", headers: http.Header{
			"x-ots-contentmd5":  []string{base64.StdEncoding.EncodeToString(md5Sum[:])},
			"x-ots-requestid":   []string{"request-id"},
			"x-ots-date":        []string{"2026-08-24T00:00:00Z"},
			"x-ots-contenttype": []string{"application/json"},
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			sdk, err := agentstorage.NewClient(agentstorage.Config{
				Endpoint: "https://example.invalid", InstanceName: "instance-a", AccessKeyID: "ak", AccessKeySecret: "sk",
				HTTPClient: &http.Client{
					Transport: mutationRoundTripFunc(func(req *http.Request) (*http.Response, error) {
						return &http.Response{
							StatusCode: http.StatusOK,
							Header:     test.headers.Clone(),
							Body:       io.NopCloser(bytes.NewReader(body)),
							Request:    req,
						}, nil
					}),
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			_, err = sdk.UpdateItem(context.Background(), &agentstorage.UpdateItemRequest{
				ItemScopedRequest: agentstorage.ItemScopedRequest{MemoryStoreName: "memstore-a"},
				ItemID:            "mem_a",
				Content:           agentstorage.String("new"),
				Precondition:      &agentstorage.ItemPrecondition{ExpectedVersionID: "opaque-before"},
				View:              agentstorage.ItemViewFull,
			})
			if got := SDKClientErrorKind(err); got != test.kind {
				t.Fatalf("kind=%q want %q err=%v", got, test.kind, err)
			}
			if !mutationNeedsReconciliation(true, nil, err) {
				t.Fatalf("kind %q was not admitted to mutation reconciliation", test.kind)
			}
		})
	}
}

func TestClientMutationReconcileCallerContextStopsGet(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		calls.Add(1)
	}))
	defer server.Close()

	config := newAPIKeyConfig(server)
	config.RetryBackoff = []time.Duration{}
	c, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name     string
		context  func() (context.Context, context.CancelFunc)
		wantKind string
	}{
		{name: "canceled", context: func() (context.Context, context.CancelFunc) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			return ctx, func() {}
		}, wantKind: "transport_context"},
		{name: "deadline", context: func() (context.Context, context.CancelFunc) {
			return context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
		}, wantKind: "transport_timeout"},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := test.context()
			defer cancel()
			memory, err := c.Update(ctx, "mem_a", []byte("new"), "opaque-before")
			if err == nil || memory != (Memory{}) || SDKClientErrorKind(err) != test.wantKind {
				t.Fatalf("memory=%+v err=%v kind=%q want kind=%q", memory, err, SDKClientErrorKind(err), test.wantKind)
			}
			if got := calls.Load(); got != 0 {
				t.Fatalf("HTTP calls=%d want 0 for inactive caller", got)
			}
		})
	}
}

func TestClientMutationReconcileDeadlineAfterDispatch(t *testing.T) {
	var mu sync.Mutex
	var actions []string
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		actions = append(actions, r.URL.Path)
		mu.Unlock()
		if r.URL.Path == "/UpdateItem" {
			// Model a server that commits but produces its response after the
			// caller's mutation deadline. A fixed wait also lets the handler
			// converge on SDK versions whose request context is not propagated
			// to the test server after the client returns.
			time.Sleep(100 * time.Millisecond)
			_ = json.NewEncoder(w).Encode(mutationTestItem("mem_a", "/a.md", "new", "fmver_after", 2))
			return
		}
		_ = json.NewEncoder(w).Encode(mutationTestItem("mem_a", "/a.md", "new", "fmver_after", 2))
	}))
	defer server.Close()

	config := newAPIKeyConfig(server)
	config.RetryBackoff = []time.Duration{}
	c, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	memory, err := c.Update(ctx, "mem_a", []byte("new"), "fmver_before")
	if err != nil || memory.MemoryID != "mem_a" || memory.LatestVersionID != "fmver_after" {
		t.Fatalf("memory=%+v err=%v", memory, err)
	}
	mu.Lock()
	gotActions := append([]string(nil), actions...)
	mu.Unlock()
	if want := []string{"/UpdateItem", "/GetItem"}; !reflect.DeepEqual(gotActions, want) {
		t.Fatalf("actions=%v want=%v", gotActions, want)
	}
}

func TestClientMutationReconcileContextLossPreservesMutationError(t *testing.T) {
	getStarted := make(chan struct{})
	releaseGet := make(chan struct{})
	var getOnce sync.Once
	var releaseOnce sync.Once
	releaseBlockedGet := func() { releaseOnce.Do(func() { close(releaseGet) }) }
	var actionsMu sync.Mutex
	var actions []string
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		actionsMu.Lock()
		actions = append(actions, r.URL.Path)
		actionsMu.Unlock()
		if r.URL.Path == "/UpdateItem" {
			writeMutationTestError(w, http.StatusInternalServerError, "AMBIGUOUS", "mutation-request")
			return
		}
		getOnce.Do(func() { close(getStarted) })
		select {
		case <-r.Context().Done():
		case <-releaseGet:
		}
	}))
	defer server.Close()
	defer releaseBlockedGet()

	config := newAPIKeyConfig(server)
	config.RetryBackoff = []time.Duration{}
	c, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan struct {
		memory Memory
		err    error
	}, 1)
	go func() {
		memory, err := c.Update(ctx, "mem_a", []byte("new"), "opaque-before")
		result <- struct {
			memory Memory
			err    error
		}{memory: memory, err: err}
	}()
	receiveMutationWithin(t, getStarted, "authoritative GetItem request")
	cancel()
	got := receiveMutationWithin(t, result, "context-canceled reconciliation result")
	releaseBlockedGet()
	var apiErr *APIError
	if !errors.As(got.err, &apiErr) || apiErr.Code != "AMBIGUOUS" || apiErr.RequestID != "mutation-request" || got.memory != (Memory{}) {
		t.Fatalf("memory=%+v err=%v want original mutation error", got.memory, got.err)
	}
	actionsMu.Lock()
	gotActions := append([]string(nil), actions...)
	actionsMu.Unlock()
	if want := []string{"/UpdateItem", "/GetItem"}; !reflect.DeepEqual(gotActions, want) {
		t.Fatalf("actions=%v want %v", gotActions, want)
	}
}

func TestClientMutationReconcileKeepsCredentialGeneration(t *testing.T) {
	var c *Client
	var mu sync.Mutex
	var seen [][2]string
	getCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		accessKeyID := r.Header.Get("x-ots-accesskeyid")
		token := r.Header.Get("x-ots-ststoken")
		mu.Lock()
		seen = append(seen, [2]string{accessKeyID, token})
		mu.Unlock()
		switch r.URL.Path {
		case "/UpdateItem":
			if err := c.UpdateSTSCredentials(credential.STS{AccessKeyID: "new-ak", AccessKeySecret: "new-sk", Token: "new-token"}); err != nil {
				t.Errorf("rotate credentials: %v", err)
			}
			writeMutationTestError(w, http.StatusInternalServerError, "AMBIGUOUS", "mutation-request")
		case "/GetItem":
			getCount++
			secret := "old-sk"
			if getCount == 1 && (accessKeyID != "old-ak" || token != "old-token") {
				t.Errorf("recheck credential=(%q,%q) want original generation", accessKeyID, token)
			}
			if getCount > 1 {
				secret = "new-sk"
				if accessKeyID != "new-ak" || token != "new-token" {
					t.Errorf("subsequent read credential=(%q,%q) want rotated generation", accessKeyID, token)
				}
			}
			body, _ := json.Marshal(mutationTestItem("mem_a", "/a.md", "new", "opaque-after", 2))
			signedResponse(w, r, accessKeyID, secret, body)
		default:
			t.Errorf("unexpected action %q", r.URL.Path)
		}
	}))
	defer server.Close()

	var err error
	c, err = New(Config{
		Endpoint: server.URL, Instance: "instance-a", Region: testRegion,
		AccessKeyID: "old-ak", AccessKeySecret: "old-sk", STSToken: "old-token",
		StoreName: "memstore_a", SessionID: "session-a", RetryBackoff: []time.Duration{},
	})
	if err != nil {
		t.Fatal(err)
	}
	memory, err := c.Update(context.Background(), "mem_a", []byte("new"), "opaque-before")
	if err != nil || memory.LatestVersionID != "opaque-after" {
		t.Fatalf("memory=%+v err=%v", memory, err)
	}
	if _, err := c.Stat(context.Background(), "/a.md"); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	got := append([][2]string(nil), seen...)
	mu.Unlock()
	want := [][2]string{{"old-ak", "old-token"}, {"old-ak", "old-token"}, {"new-ak", "new-token"}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("credential generations=%v want %v", got, want)
	}
}

func TestClientSafeReadRetryMatrix(t *testing.T) {
	tests := []struct {
		name   string
		status int
		code   string
	}{
		{name: "429", status: http.StatusTooManyRequests, code: "THROTTLED"},
		{name: "500", status: http.StatusInternalServerError, code: "INTERNAL_ERROR"},
		{name: "503", status: http.StatusServiceUnavailable, code: "UNAVAILABLE"},
		{name: "504", status: http.StatusGatewayTimeout, code: "GATEWAY_TIMEOUT"},
		{name: "lock", status: http.StatusConflict, code: "LOCK_CONFLICT"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			attempts := 0
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				attempts++
				if attempts == 1 {
					writeMutationTestError(w, test.status, test.code, "read-request")
					return
				}
				_ = json.NewEncoder(w).Encode(mutationTestBasicItem("mem_a", "/a.md", "opaque-current", 1))
			}))
			defer server.Close()

			config := newAPIKeyConfig(server)
			config.RetryBackoff = []time.Duration{time.Nanosecond}
			c, err := New(config)
			if err != nil {
				t.Fatal(err)
			}
			memory, err := c.Stat(context.Background(), "a.md")
			if err != nil || memory.MemoryID != "mem_a" || attempts != 2 {
				t.Fatalf("memory=%+v err=%v attempts=%d", memory, err, attempts)
			}
		})
	}
}

func TestClientMutationDoesNotReconcileExplicitFailures(t *testing.T) {
	for _, test := range []struct {
		name   string
		status int
		code   string
	}{
		{name: "bad request", status: http.StatusBadRequest, code: "INVALID_PARAMETER"},
		{name: "other conflict", status: http.StatusConflict, code: "PATH_EXISTS"},
		{name: "unauthorized", status: http.StatusUnauthorized, code: "UNAUTHORIZED"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var actions []string
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				actions = append(actions, r.URL.Path)
				writeMutationTestError(w, test.status, test.code, "mutation-request")
			}))
			defer server.Close()

			config := newAPIKeyConfig(server)
			config.RetryBackoff = []time.Duration{time.Nanosecond}
			c, err := New(config)
			if err != nil {
				t.Fatal(err)
			}
			memory, err := c.Update(context.Background(), "mem_a", []byte("new"), "opaque-before")
			var apiErr *APIError
			if !errors.As(err, &apiErr) || apiErr.Code != test.code || apiErr.RequestID != "mutation-request" || memory != (Memory{}) {
				t.Fatalf("memory=%+v err=%v", memory, err)
			}
			if want := []string{"/UpdateItem"}; !reflect.DeepEqual(actions, want) {
				t.Fatalf("actions=%v want %v", actions, want)
			}
		})
	}
}

func TestClientMutationRetryBackoffUsesCallerContext(t *testing.T) {
	called := make(chan struct{})
	var calls atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) == 1 {
			close(called)
		}
		writeMutationTestError(w, http.StatusTooManyRequests, "THROTTLED", "mutation-request")
	}))
	defer server.Close()

	config := newAPIKeyConfig(server)
	config.RetryBackoff = []time.Duration{time.Second}
	c, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := c.Update(ctx, "mem_a", []byte("new"), "opaque-before")
		result <- err
	}()
	receiveMutationWithin(t, called, "first mutation attempt")
	cancel()
	if err := receiveMutationWithin(t, result, "retry-backoff cancellation result"); !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v want context.Canceled", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("calls=%d want 1", got)
	}
}

func TestClientMutationDoesNotReconcileRequestOrUnknownClientErrors(t *testing.T) {
	serializeClient, err := agentstorage.NewClient(agentstorage.Config{
		Endpoint: "https://example", InstanceName: "instance", APIKey: "key",
	})
	if err != nil {
		t.Fatal(err)
	}
	serializeErr := serializeClient.Do(context.Background(), "Action", func() {}, nil)
	if SDKClientErrorKind(serializeErr) != "request_serialize" || mutationNeedsReconciliation(true, nil, serializeErr) {
		t.Fatalf("serialize err=%v kind=%q unexpectedly reconciled", serializeErr, SDKClientErrorKind(serializeErr))
	}

	buildClient, err := agentstorage.NewClient(agentstorage.Config{
		Endpoint: "https://[::1", InstanceName: "instance", APIKey: "key",
	})
	if err != nil {
		t.Fatal(err)
	}
	buildErr := buildClient.Do(context.Background(), "Action", map[string]any{}, nil)
	if SDKClientErrorKind(buildErr) != "request_build" || mutationNeedsReconciliation(true, nil, buildErr) {
		t.Fatalf("build err=%v kind=%q unexpectedly reconciled", buildErr, SDKClientErrorKind(buildErr))
	}

	unknownErr := &agentstorage.ClientError{Message: "safe unknown client error"}
	if SDKClientErrorKind(unknownErr) != "" || mutationNeedsReconciliation(true, nil, unknownErr) {
		t.Fatalf("unknown err kind=%q unexpectedly reconciled", SDKClientErrorKind(unknownErr))
	}
}

func TestClientMutationReconcileRejectsInvalidAuthoritativeEvidence(t *testing.T) {
	valid := mutationTestItem("mem_a", "/a.md", "new", "opaque-after", 2)
	tests := []struct {
		name   string
		mutate func(map[string]any)
	}{
		{name: "wrong type", mutate: func(item map[string]any) { item["type"] = "other" }},
		{name: "wrong item id", mutate: func(item map[string]any) { item["itemId"] = "mem_other" }},
		{name: "empty path", mutate: func(item map[string]any) { item["path"] = "" }},
		{name: "empty version", mutate: func(item map[string]any) { item["latestVersionId"] = "" }},
		{name: "nonpositive seq", mutate: func(item map[string]any) { item["latestSeq"] = 0 }},
		{name: "empty sha", mutate: func(item map[string]any) { item["contentSha256"] = "" }},
		{name: "omitted full content", mutate: func(item map[string]any) { delete(item, "content") }},
		{name: "wrong content size", mutate: func(item map[string]any) { item["contentSizeBytes"] = 99 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			item := cloneMutationItemWithout(valid, "unused")
			test.mutate(item)
			var actions []string
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				actions = append(actions, r.URL.Path)
				if r.URL.Path == "/UpdateItem" {
					writeMutationTestError(w, http.StatusInternalServerError, "AMBIGUOUS", "mutation-request")
					return
				}
				_ = json.NewEncoder(w).Encode(item)
			}))
			defer server.Close()

			config := newAPIKeyConfig(server)
			config.RetryBackoff = []time.Duration{}
			c, err := New(config)
			if err != nil {
				t.Fatal(err)
			}
			memory, err := c.Update(context.Background(), "mem_a", []byte("new"), "opaque-before")
			var apiErr *APIError
			if !errors.As(err, &apiErr) || apiErr.Code != "AMBIGUOUS" || apiErr.RequestID != "mutation-request" || memory != (Memory{}) {
				t.Fatalf("memory=%+v err=%v", memory, err)
			}
			if want := []string{"/UpdateItem", "/GetItem"}; !reflect.DeepEqual(actions, want) {
				t.Fatalf("actions=%v want %v", actions, want)
			}
		})
	}
}

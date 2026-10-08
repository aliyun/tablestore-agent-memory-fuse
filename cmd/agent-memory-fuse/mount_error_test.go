package main

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"agent-memory-fuse/internal/client"
)

func TestClassifyMountError(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		wantClass  string
		wantReason string
		wantExit   int
	}{
		{"429 throttled", &client.APIError{StatusCode: 429, Code: "THROTTLED"}, "retryable", "throttled", 2},
		{"503 unavailable", &client.APIError{StatusCode: 503, Code: "UNAVAILABLE"}, "retryable", "throttled", 2},
		{"500 server error", &client.APIError{StatusCode: 500, Code: "INTERNAL"}, "retryable", "server_error", 2},
		{"401 unauthorized", &client.APIError{StatusCode: 401, Code: "Unauthorized"}, "auth", "auth_rejected", 3},
		{"403 auth failed", &client.APIError{StatusCode: 403, Code: "OTSAuthFailed"}, "auth", "auth_rejected", 3},
		{"403 no permission", &client.APIError{StatusCode: 403, Code: "OTSNoPermissionAccess"}, "auth", "auth_rejected", 3},
		{"403 validation is config not auth", &client.APIError{StatusCode: 403, Code: "OTSValidationFail"}, "config", "validation_failed", 4},
		{"404 store missing", &client.APIError{StatusCode: 404, Code: "OTSObjectNotExist"}, "config", "store_not_found", 4},
		{"400 bad request", &client.APIError{StatusCode: 400, Code: "VALIDATION_ERROR"}, "config", "validation_failed", 4},
		{"unclassified status", &client.APIError{StatusCode: 405, Code: "WEIRD"}, "unknown", "unknown", 1},
		{"tls hostname mismatch", errors.New(`OTSClientError: failed to send request: Post "https://ots-cn-beijing.aliyuncs.com/ListItems": tls: failed to verify certificate: x509: certificate is valid for other names`), "config", "tls_mismatch", 4},
		{"generic error", errors.New("something exploded"), "unknown", "unknown", 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			failure := classifyMountError(test.err)
			if failure.class != test.wantClass || failure.reason != test.wantReason || failure.exitCode != test.wantExit {
				t.Fatalf("got (%s,%s,%d) want (%s,%s,%d)", failure.class, failure.reason, failure.exitCode, test.wantClass, test.wantReason, test.wantExit)
			}
			if failure.advice == "" {
				t.Fatal("advice must never be empty")
			}
			if !errors.Is(failure, test.err) {
				t.Fatal("mountFailure must unwrap to the original error")
			}
		})
	}
}

func TestMountFailureLogLinesFormat(t *testing.T) {
	failure := classifyMountError(&client.APIError{StatusCode: 404, Code: "OTSObjectNotExist", Message: "resource not found", RequestID: "req-1"})
	lines := strings.SplitN(failure.logLines(), "\n", 2)
	if len(lines) != 2 || lines[0] != mountErrorLogPrefix {
		t.Fatalf("first line must be the fixed prefix, got %q", lines[0])
	}
	if !strings.HasPrefix(lines[1], "mount_error json: {") {
		t.Fatalf("second line must be tagged JSON 'mount_error json: {...}', got %q", lines[1])
	}
	var record map[string]any
	if err := json.Unmarshal([]byte(strings.TrimPrefix(lines[1], "mount_error json: ")), &record); err != nil {
		t.Fatalf("second line is not valid JSON: %v", err)
	}
	if record["operation"] != "mount" || record["class"] != "config" || record["reason"] != "store_not_found" {
		t.Fatalf("record=%v", record)
	}
	if int(record["exit_code"].(float64)) != failure.exitCode {
		t.Fatalf("exit_code=%v must equal process exit %d", record["exit_code"], failure.exitCode)
	}
	if record["http_status"].(float64) != 404 || record["service_code"] != "OTSObjectNotExist" || record["request_id"] != "req-1" {
		t.Fatalf("service fields=%v", record)
	}
	if _, has := record["message"]; has {
		t.Fatal("classified failures must not carry a free-text message")
	}
}

func TestMountFailureUnknownKeepsRawMessage(t *testing.T) {
	failure := classifyMountError(errors.New("dial tcp: weird failure"))
	lines := strings.SplitN(failure.logLines(), "\n", 2)
	var record map[string]any
	if err := json.Unmarshal([]byte(strings.TrimPrefix(lines[1], "mount_error json: ")), &record); err != nil {
		t.Fatal(err)
	}
	if record["message"] != "dial tcp: weird failure" || record["exit_code"].(float64) != 1 {
		t.Fatalf("record=%v", record)
	}
}

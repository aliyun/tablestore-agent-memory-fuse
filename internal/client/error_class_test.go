package client

import (
	"context"
	"errors"
	"testing"
)

func TestErrorClass(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		wantClass  string
		wantReason string
	}{
		{"nil", nil, ClassUnknown, "unknown"},
		{"429", &APIError{StatusCode: 429, Code: "THROTTLED"}, ClassRetryable, "throttled"},
		{"503", &APIError{StatusCode: 503}, ClassRetryable, "throttled"},
		{"500", &APIError{StatusCode: 500}, ClassRetryable, "server_error"},
		{"401", &APIError{StatusCode: 401}, ClassAuth, "auth_rejected"},
		{"403 auth", &APIError{StatusCode: 403, Code: "OTSAuthFailed"}, ClassAuth, "auth_rejected"},
		{"403 validation is config", &APIError{StatusCode: 403, Code: "OTSValidationFail"}, ClassConfig, "validation_failed"},
		{"404", &APIError{StatusCode: 404, Code: "OTSObjectNotExist"}, ClassConfig, "store_not_found"},
		{"400", &APIError{StatusCode: 400}, ClassConfig, "validation_failed"},
		{"413", &APIError{StatusCode: 413}, ClassRejected, "content_too_large"},
		{"409 lock", &APIError{StatusCode: 409, Code: "LOCK_CONFLICT"}, ClassRetryable, "lock_conflict"},
		{"409 stale", &APIError{StatusCode: 409, Code: "PRECONDITION_FAILED"}, ClassRejected, "stale_version"},
		{"409 exists", &APIError{StatusCode: 409, Code: "PATH_EXISTS"}, ClassRejected, "already_exists"},
		{"409 quota", &APIError{StatusCode: 409, Code: "QUOTA_EXCEEDED"}, ClassRejected, "quota_exceeded"},
		{"409 readonly", &APIError{StatusCode: 409, Code: "READ_ONLY_STORE"}, ClassConfig, "read_only_store"},
		{"409 bare", &APIError{StatusCode: 409}, ClassRejected, "stale_version"},
		{"405 unclassified", &APIError{StatusCode: 405}, ClassUnknown, "unknown"},
		{"deadline", context.DeadlineExceeded, ClassRetryable, "timeout"},
		{"canceled", context.Canceled, ClassUnknown, "interrupted"},
		{"x509", errors.New(`tls: failed to verify certificate: x509: certificate is valid for other names`), ClassConfig, "tls_mismatch"},
		{"generic", errors.New("boom"), ClassUnknown, "unknown"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			class, reason := ErrorClass(test.err)
			if class != test.wantClass || reason != test.wantReason {
				t.Fatalf("got (%s,%s) want (%s,%s)", class, reason, test.wantClass, test.wantReason)
			}
			if !ValidErrorClass(class) || !ValidErrorReason(reason) {
				t.Fatalf("(%s,%s) must be in the safe enums", class, reason)
			}
		})
	}
}

package client

import (
	"context"
	"errors"
	"net/http"
	"strings"
)

// Error classes shared by the mount-failure exit path and the runtime
// mutation error log. One source of truth so both events can never disagree
// about how an error should be handled.
const (
	ClassRetryable = "retryable" // transient; the same operation may succeed later
	ClassAuth      = "auth"      // credential rejected/expired or permission denied
	ClassConfig    = "config"    // parameters, endpoint, contract or store setup
	ClassRejected  = "rejected"  // deterministic business rejection; blind retry is pointless
	ClassUnknown   = "unknown"
)

// ErrorClass maps an error returned by this client to (class, reason).
// reason values are a fixed enumeration safe for structured logging.
func ErrorClass(err error) (class, reason string) {
	if err == nil {
		return ClassUnknown, "unknown"
	}
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		return classifyAPIError(apiErr)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return ClassRetryable, "timeout"
	}
	if errors.Is(err, context.Canceled) {
		return ClassUnknown, "interrupted"
	}
	// TLS hostname/certificate mismatches surface as transport errors with
	// x509 detail; they are endpoint configuration problems, not transient.
	if strings.Contains(err.Error(), "x509") {
		return ClassConfig, "tls_mismatch"
	}
	switch SDKClientErrorKind(err) {
	case "transport_timeout":
		return ClassRetryable, "timeout"
	case "transport_context":
		return ClassRetryable, "timeout"
	case "transport_network", "transport_other":
		return ClassRetryable, "network"
	}
	return ClassUnknown, "unknown"
}

// ValidErrorClass reports whether class is one of the fixed enumeration
// values safe to place in structured logs.
func ValidErrorClass(class string) bool {
	switch class {
	case ClassRetryable, ClassAuth, ClassConfig, ClassRejected, ClassUnknown:
		return true
	}
	return false
}

var validErrorReasons = map[string]bool{
	"throttled": true, "server_error": true, "timeout": true, "network": true,
	"lock_conflict": true, "auth_rejected": true, "store_not_found": true,
	"validation_failed": true, "tls_mismatch": true, "read_only_store": true,
	"content_too_large": true, "invalid_encoding": true, "quota_exceeded": true,
	"already_exists": true, "stale_version": true, "not_supported": true,
	"interrupted": true, "unknown": true,
}

// ValidErrorReason reports whether reason belongs to the fixed enumeration.
func ValidErrorReason(reason string) bool { return validErrorReasons[reason] }

func classifyAPIError(apiErr *APIError) (class, reason string) {
	switch {
	case apiErr.StatusCode == http.StatusTooManyRequests,
		apiErr.StatusCode == http.StatusServiceUnavailable,
		apiErr.StatusCode == http.StatusGatewayTimeout:
		return ClassRetryable, "throttled"
	case apiErr.StatusCode >= 500:
		return ClassRetryable, "server_error"
	case apiErr.StatusCode == http.StatusUnauthorized:
		return ClassAuth, "auth_rejected"
	case apiErr.StatusCode == http.StatusForbidden && apiErr.Code == "OTSValidationFail":
		// 403 but NOT auth: server-side parameter/contract validation.
		return ClassConfig, "validation_failed"
	case apiErr.StatusCode == http.StatusForbidden:
		return ClassAuth, "auth_rejected"
	case apiErr.StatusCode == http.StatusNotFound:
		return ClassConfig, "store_not_found"
	case apiErr.StatusCode == http.StatusBadRequest:
		return ClassConfig, "validation_failed"
	case apiErr.StatusCode == http.StatusRequestEntityTooLarge:
		return ClassRejected, "content_too_large"
	case apiErr.StatusCode == http.StatusConflict:
		return classifyConflict(apiErr.Code)
	}
	return ClassUnknown, "unknown"
}

func classifyConflict(code string) (class, reason string) {
	switch code {
	case "LOCK_CONFLICT":
		return ClassRetryable, "lock_conflict"
	case "SHA_MISMATCH", "PRECONDITION_FAILED":
		return ClassRejected, "stale_version"
	case "PATH_EXISTS":
		return ClassRejected, "already_exists"
	case "QUOTA_EXCEEDED":
		return ClassRejected, "quota_exceeded"
	case "READ_ONLY_STORE", "FILE_MEMORY_ARCHIVED":
		return ClassConfig, "read_only_store"
	}
	// Bare 409 maps to ESTALE in errnoFor; keep the same disposition.
	return ClassRejected, "stale_version"
}

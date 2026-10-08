package fs

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"path"
	"syscall"

	"agent-memory-fuse/internal/client"
)

const (
	mutationPathHashDomain      = "agent-memory-fuse/mutation-path/v1\x00"
	maxMutationServiceCodeBytes = 64
	maxMutationRequestIDBytes   = 128
)

// MutationErrorEvent is the safe, bounded diagnostic emitted after a remote
// operation fails. It intentionally contains no raw path or error text.
type MutationErrorEvent struct {
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

// MutationErrorEventName is the event discriminator carried by every runtime
// remote-failure record; mount failures use their own event name so one
// parser can dispatch on the field instead of the human prefix line.
const MutationErrorEventName = "mutation_error"

func newMutationErrorEvent(operation, value string, err error, errno syscall.Errno) MutationErrorEvent {
	digest := sha256.Sum256([]byte(mutationPathHashDomain + normalizeMutationPath(value)))
	class, reason := client.ErrorClass(err)
	if class == client.ClassUnknown && reason == "unknown" {
		// Local filesystem rejections (size/encoding/quota/exclusive-create)
		// never reach the client layer; classify them from the errno. A
		// specific reason from ErrorClass (e.g. interrupted) is kept.
		class, reason = classForLocalErrno(errno)
	}
	event := MutationErrorEvent{
		Event:      MutationErrorEventName,
		Operation:  operation,
		Class:      class,
		Reason:     reason,
		PathSHA256: hex.EncodeToString(digest[:]),
		Errno:      int(errno),
		SDKKind:    client.SDKClientErrorKind(err),
	}

	var apiErr *client.APIError
	switch {
	case errors.As(err, &apiErr):
		event.Category = "service"
		event.HTTPStatus = apiErr.StatusCode
		event.ServiceCode = apiErr.Code
		event.RequestID = apiErr.RequestID
		event.Message = "service request failed"
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		event.Category = "context"
		event.Message = "operation canceled"
	default:
		event.Category = "client"
		event.Message = "client-side error"
	}
	return SanitizeMutationErrorIdentifiers(event)
}

func normalizeMutationPath(value string) string {
	return canonicalPathKey(path.Clean("/" + canonicalPathKey(value)))
}

// SanitizeMutationErrorIdentifiers bounds the two service-provided fields
// carried by a mutation event. The CLI applies it again before formatting so a
// directly constructed event cannot bypass the constructor boundary.
func SanitizeMutationErrorIdentifiers(event MutationErrorEvent) MutationErrorEvent {
	event.ServiceCode = safeMutationIdentifier(event.ServiceCode, maxMutationServiceCodeBytes)
	event.RequestID = safeMutationIdentifier(event.RequestID, maxMutationRequestIDBytes)
	event.SDKKind = client.SanitizeSDKClientErrorKind(event.SDKKind)
	// Directly constructed events cannot bypass the classification enums.
	if !client.ValidErrorClass(event.Class) || !client.ValidErrorReason(event.Reason) {
		event.Class, event.Reason = client.ClassUnknown, "unknown"
	}
	return event
}

func safeMutationIdentifier(value string, maxBytes int) string {
	if len(value) > maxBytes {
		return ""
	}
	for index := 0; index < len(value); index++ {
		character := value[index]
		if character >= 'a' && character <= 'z' ||
			character >= 'A' && character <= 'Z' ||
			character >= '0' && character <= '9' ||
			character == '.' || character == '_' || character == ':' || character == '-' {
			continue
		}
		return ""
	}
	return value
}

func safelyReportMutationError(reporter func(MutationErrorEvent), event MutationErrorEvent) {
	defer func() { _ = recover() }()
	reporter(event)
}

// classForLocalErrno classifies rejections produced inside the filesystem
// layer (content limits, encoding, quota, local CAS) that never surface as
// client APIError values.
func classForLocalErrno(errno syscall.Errno) (class, reason string) {
	switch errno {
	case syscall.EFBIG:
		return client.ClassRejected, "content_too_large"
	case syscall.EILSEQ:
		return client.ClassRejected, "invalid_encoding"
	case syscall.ENOSPC:
		return client.ClassRejected, "quota_exceeded"
	case syscall.EEXIST:
		return client.ClassRejected, "already_exists"
	case syscall.ESTALE:
		return client.ClassRejected, "stale_version"
	case syscall.EROFS:
		return client.ClassConfig, "read_only_store"
	case syscall.EINVAL:
		return client.ClassConfig, "validation_failed"
	case syscall.EACCES:
		return client.ClassAuth, "auth_rejected"
	case syscall.EAGAIN:
		return client.ClassRetryable, "throttled"
	case syscall.ENOTSUP, syscall.EISDIR, syscall.ENOTEMPTY:
		return client.ClassRejected, "not_supported"
	}
	return client.ClassUnknown, "unknown"
}

func safelyReportRemoteError(reporter func(MutationErrorEvent), operation, value string, err error, errno syscall.Errno) {
	if reporter == nil || errno == syscall.ENOENT || errno == syscall.EINTR &&
		(operation == "list" || operation == "stat" || operation == "read") {
		return
	}
	safelyReportMutationError(reporter, newMutationErrorEvent(operation, value, err, errno))
}

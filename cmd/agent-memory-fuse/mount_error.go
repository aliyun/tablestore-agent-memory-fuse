package main

import (
	"encoding/json"
	"errors"

	"agent-memory-fuse/internal/client"
)

// Mount failure classification. The FUSE runtime path reports errors through
// POSIX errno plus the mutation error JSON; this scheme covers the
// mount-startup failure exit, where the process is about to terminate and can
// additionally hand machine consumers (FC sidecar, systemd) an exit code.
//
// Exit codes:
//
//	1  unknown / unclassified (same as before this scheme)
//	2  retryable: transient network or throttling, retry later
//	3  auth: credential rejected, expired, or RAM policy denied
//	4  config: parameters, endpoint TLS, client/server contract, or missing store
const (
	exitUnknown   = 1
	exitRetryable = 2
	exitAuth      = 3
	exitConfig    = 4
)

// mountFailure carries the classification of a failed initial directory load.
// It implements error so runMount can return it through the normal path.
type mountFailure struct {
	class    string
	reason   string
	advice   string
	exitCode int
	raw      error
	apiErr   *client.APIError
}

func (f *mountFailure) Error() string { return f.raw.Error() }
func (f *mountFailure) Unwrap() error { return f.raw }

// mountErrorLogRecord is the structured second line printed on mount failure.
// Field names and the event discriminator follow the runtime mutation error
// record so one parser handles both events.
type mountErrorLogRecord struct {
	Event       string `json:"event"`
	Operation   string `json:"operation"`
	Class       string `json:"class"`
	Reason      string `json:"reason"`
	ExitCode    int    `json:"exit_code"`
	Advice      string `json:"advice"`
	HTTPStatus  int    `json:"http_status,omitempty"`
	ServiceCode string `json:"service_code,omitempty"`
	RequestID   string `json:"request_id,omitempty"`
	Message     string `json:"message,omitempty"`
}

// MountErrorEventName is the event discriminator for mount-startup failures;
// runtime remote failures use memoryfs.MutationErrorEventName.
const MountErrorEventName = "mount_error"

const mountErrorLogPrefix = "initial directory load failed"

// logLines renders the two-line failure output: a timestamped human anchor
// line, then a tagged JSON line "<event> json: {...}". The tag lets collectors
// grep and dispatch by event type without parsing; the payload after the tag
// is pure JSON.
func (f *mountFailure) logLines() string {
	record := mountErrorLogRecord{
		Event:     MountErrorEventName,
		Operation: "mount",
		Class:     f.class,
		Reason:    f.reason,
		ExitCode:  f.exitCode,
		Advice:    f.advice,
	}
	if f.apiErr != nil {
		record.HTTPStatus = f.apiErr.StatusCode
		record.ServiceCode = f.apiErr.Code
		record.RequestID = f.apiErr.RequestID
	}
	if f.class == client.ClassUnknown {
		// Unknown failures keep the raw error text (identical to the previous
		// log.Fatal behaviour) so no diagnostic information is lost. Mount
		// errors never embed credentials; endpoints are non-secret argv data.
		record.Message = f.raw.Error()
	}
	payload, err := json.Marshal(record)
	if err != nil {
		// Defensive: the record only contains strings and ints.
		return mountErrorLogPrefix + "\n" + MountErrorEventName + ` json: {"event":"mount_error","class":"unknown","reason":"unknown","exit_code":1}`
	}
	return mountErrorLogPrefix + "\n" + MountErrorEventName + " json: " + string(payload)
}

// classifyMountError shares the single error-class source of truth with the
// runtime mutation log (client.ErrorClass), then maps class/reason onto the
// mount-only exit code and operator advice.
func classifyMountError(err error) *mountFailure {
	failure := &mountFailure{raw: err}
	var apiErr *client.APIError
	if errors.As(err, &apiErr) {
		failure.apiErr = apiErr
	}
	failure.class, failure.reason = client.ErrorClass(err)
	failure.exitCode, failure.advice = mountExitFor(failure.class, failure.reason)
	return failure
}

func mountExitFor(class, reason string) (int, string) {
	switch class {
	case client.ClassRetryable:
		switch reason {
		case "throttled", "lock_conflict":
			return exitRetryable, "server throttled or busy, retry later"
		case "timeout":
			return exitRetryable, "request timed out, retry later"
		default:
			return exitRetryable, "transient failure, retry later"
		}
	case client.ClassAuth:
		return exitAuth, "credential rejected or denied — check validity, STS expiry, and RAM policy for this store/scope"
	case client.ClassConfig:
		switch reason {
		case "store_not_found":
			return exitConfig, "store or instance not found — check --store/--instance/--endpoint"
		case "tls_mismatch":
			return exitConfig, "endpoint TLS certificate mismatch — check --endpoint hostname"
		case "read_only_store":
			return exitConfig, "store is read-only — check --store or mount with --read-only"
		default:
			return exitConfig, "server rejected request parameters — check --scope/--store values and client/server version compatibility"
		}
	case client.ClassRejected:
		// Deterministic business rejections do not occur during the initial
		// list, but map them to config so a supervisor never hot-loops.
		return exitConfig, "request rejected by server policy, retrying will not help"
	default:
		return exitUnknown, "unclassified failure, see message"
	}
}

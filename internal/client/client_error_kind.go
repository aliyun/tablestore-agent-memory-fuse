package client

import (
	"errors"

	"github.com/aliyun/tablestore-agent-storage-sdk/go/agentstorage"
)

// SDKClientErrorKind returns the fixed, safe kind of an SDK client-side
// failure. Service errors, historical unclassified client errors, and unknown
// future kinds intentionally produce no diagnostic value.
func SDKClientErrorKind(err error) string {
	var semanticErr *responseSemanticError
	if errors.As(err, &semanticErr) {
		return string(agentstorage.ClientErrorKindResponseSemantic)
	}
	var clientErr *agentstorage.ClientError
	if !errors.As(err, &clientErr) {
		return ""
	}
	return SanitizeSDKClientErrorKind(string(clientErr.Kind()))
}

// SanitizeSDKClientErrorKind accepts only the fixed SDK kinds that are safe to
// place in diagnostics, including events constructed outside the client layer.
func SanitizeSDKClientErrorKind(kind string) string {
	switch agentstorage.ClientErrorKind(kind) {
	case agentstorage.ClientErrorKindRequestSerialize,
		agentstorage.ClientErrorKindRequestBuild,
		agentstorage.ClientErrorKindTransportContext,
		agentstorage.ClientErrorKindTransportTimeout,
		agentstorage.ClientErrorKindTransportNetwork,
		agentstorage.ClientErrorKindTransportOther,
		agentstorage.ClientErrorKindResponseRead,
		agentstorage.ClientErrorKindResponseIntegrity,
		agentstorage.ClientErrorKindResponseAuth,
		agentstorage.ClientErrorKindResponseDecode,
		agentstorage.ClientErrorKindResponseSemantic:
		return kind
	default:
		return ""
	}
}

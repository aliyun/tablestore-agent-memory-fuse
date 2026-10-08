package fs

import (
	"strings"

	"golang.org/x/text/unicode/norm"
)

// normalizePathNFC normalizes the Unicode representation without changing
// whether the path is absolute. Service responses are stored on handles in
// this form so callers keep the authoritative path shape returned remotely.
func normalizePathNFC(value string) string {
	return norm.NFC.String(value)
}

// canonicalPathKey returns the relative NFC form used by every local path
// map, lock, cache and remote-operation boundary.
func canonicalPathKey(value string) string {
	return strings.Trim(normalizePathNFC(value), "/")
}

#!/bin/sh
set -eu

repo_root=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
output=${1:-dist/agent-memory-fuse-linux-amd64}
case "$output" in
  /*) ;;
  *) output="$repo_root/$output" ;;
esac

version=$(tr -d '\r\n' < "$repo_root/VERSION")
commit=$(git -C "$repo_root" rev-parse HEAD)
source_epoch=$(git -C "$repo_root" show -s --format=%ct HEAD)
case "$version" in
  ''|*[!A-Za-z0-9._-]*) echo "invalid VERSION" >&2; exit 1 ;;
esac
case "$commit" in
  ''|*[!0-9a-f]*) echo "invalid git commit" >&2; exit 1 ;;
esac
case "$source_epoch" in
  ''|*[!0-9]*) echo "invalid source epoch" >&2; exit 1 ;;
esac

if [ -n "$(git -C "$repo_root" status --porcelain --untracked-files=normal)" ]; then
	echo "release builds require a clean worktree" >&2
	exit 1
fi
expected_sdk_commit=$(tr -d '\r\n' < "$repo_root/SDK_COMMIT")
expected_sdk_short=$(printf '%.12s' "$expected_sdk_commit")
sdk_module_version=$(awk '$1 == "github.com/aliyun/tablestore-agent-storage-sdk/go" { print $2; exit }' "$repo_root/go.mod")
case "$sdk_module_version" in
  *-"$expected_sdk_short") ;;
  *) echo "go.mod SDK version $sdk_module_version does not match $expected_sdk_short" >&2; exit 1 ;;
esac
expected_sdk_digest=$(tr -d '\r\n' < "$repo_root/SDK_VENDOR_SHA256")
actual_sdk_digest=$("$repo_root/scripts/sdk-vendor-digest.sh")
if [ "$actual_sdk_digest" != "$expected_sdk_digest" ]; then
  echo "vendored SDK digest is $actual_sdk_digest, expected $expected_sdk_digest" >&2
  exit 1
fi

mkdir -p "$(dirname -- "$output")"
(
  cd "$repo_root"
  GOTOOLCHAIN=go1.26.6 CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -mod=vendor -trimpath -buildvcs=true \
      -ldflags="-s -w -X main.version=$version -X main.gitCommit=$commit -X main.sourceEpoch=$source_epoch" \
      -o "$output" ./cmd/agent-memory-fuse
)

go version -m "$output"
checksum_file="$output.sha256"
output_dir=$(dirname -- "$output")
output_name=$(basename -- "$output")
if command -v sha256sum >/dev/null 2>&1; then
  (cd "$output_dir" && sha256sum "$output_name") | tee "$checksum_file"
else
  (cd "$output_dir" && shasum -a 256 "$output_name") | tee "$checksum_file"
fi

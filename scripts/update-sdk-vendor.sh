#!/bin/sh
set -eu

repo_root=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
sdk_root="$repo_root/../tablestore-agent-storage-sdk"
expected=$(tr -d '\r\n' < "$repo_root/SDK_COMMIT")
actual=$(git -C "$sdk_root" rev-parse HEAD)
expected_short=$(printf '%.12s' "$expected")
module_version=$(awk '$1 == "github.com/aliyun/tablestore-agent-storage-sdk/go" { print $2; exit }' "$repo_root/go.mod")
replace_line='replace github.com/aliyun/tablestore-agent-storage-sdk/go => ../tablestore-agent-storage-sdk/go'

if [ "$actual" != "$expected" ]; then
  echo "SDK checkout is $actual, expected $expected" >&2
  exit 1
fi
case "$module_version" in
  *-"$expected_short") ;;
  *) echo "go.mod SDK version $module_version does not match $expected_short" >&2; exit 1 ;;
esac
if ! grep -Fqx "$replace_line" "$repo_root/go.mod"; then
  echo "go.mod SDK replace path does not match $sdk_root/go" >&2
  exit 1
fi
if [ -n "$(git -C "$sdk_root" status --porcelain --untracked-files=normal)" ]; then
  echo "SDK checkout must be clean" >&2
  exit 1
fi

cd "$repo_root"
GOTOOLCHAIN=go1.26.6 go mod vendor
./scripts/sdk-vendor-digest.sh > SDK_VENDOR_SHA256
echo "vendored SDK commit $expected with digest $(cat SDK_VENDOR_SHA256)"

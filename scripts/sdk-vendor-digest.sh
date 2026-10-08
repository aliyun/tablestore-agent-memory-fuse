#!/bin/sh
set -eu

repo_root=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
sdk_vendor="$repo_root/vendor/github.com/aliyun/tablestore-agent-storage-sdk/go"
if [ ! -d "$sdk_vendor" ]; then
  echo "vendored SDK directory is missing" >&2
  exit 1
fi

hash_stream() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum | awk '{print $1}'
  else
    shasum -a 256 | awk '{print $1}'
  fi
}

(
  cd "$sdk_vendor"
  find . -type f -print | LC_ALL=C sort | while IFS= read -r file; do
    if command -v sha256sum >/dev/null 2>&1; then
      digest=$(sha256sum "$file" | awk '{print $1}')
    else
      digest=$(shasum -a 256 "$file" | awk '{print $1}')
    fi
    printf '%s  %s\n' "$digest" "$file"
  done
) | hash_stream

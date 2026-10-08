#!/bin/sh
# 纯本地回归：不启动 FUSE、不访问网络、不读取真实凭证。

SCRIPT_DIR=$(CDPATH='' cd -P "$(dirname "$0")" && pwd)
REAL_SCRIPT=$SCRIPT_DIR/test-real.sh
POLICY_SOURCE=$(awk '
  /^# LOOPBACK_HELPER_BEGIN$/ { copy = 1; next }
  /^# LOOPBACK_HELPER_END$/ { copy = 0 }
  copy { print }
' "$REAL_SCRIPT")
eval "$POLICY_SOURCE"

PASS=0
FAIL=0
pass() { PASS=$((PASS + 1)); printf 'PASS %s\n' "$1"; }
fail() { FAIL=$((FAIL + 1)); printf 'FAIL %s: %s\n' "$1" "$2"; }

command -v configure_http_transport >/dev/null 2>&1 || {
  fail policy-interface "configure_http_transport is missing"
  exit 1
}
command -v run_fuse_mount >/dev/null 2>&1 || {
  fail mount-interface "run_fuse_mount is missing"
  exit 1
}

while IFS='|' read -r name endpoint opt_in want_status want_flag; do
  [ -n "$name" ] || continue
  if [ "$opt_in" = unset ]; then
    unset MEM_ALLOW_INSECURE_HTTP
  else
    MEM_ALLOW_INSECURE_HTTP=$opt_in
    export MEM_ALLOW_INSECURE_HTTP
  fi
  if configure_http_transport "$endpoint" >/dev/null 2>&1; then
    got_status=allow
  else
    got_status=reject
  fi
  [ -n "$ALLOW_INSECURE_HTTP_ARG" ] && got_flag=$ALLOW_INSECURE_HTTP_ARG || got_flag=-
  if [ "$got_status" = "$want_status" ] && [ "$got_flag" = "$want_flag" ]; then
    pass "$name"
  else
    fail "$name" "status=$got_status flag=$got_flag want status=$want_status flag=$want_flag"
  fi
done <<'CASES'
https-lower|https://example.test|unset|allow|-
https-upper|HTTPS://example.test|1|allow|-
localhost-lower|http://localhost:8080|unset|allow|-
localhost-upper|HTTP://LOCALHOST:8080|0|allow|-
ipv4-loopback|http://127.0.0.2:8080|unset|allow|-
ipv6-compressed|http://[::1]:8080|0|allow|-
ipv6-expanded|http://[0:0:0:0:0:0:0:1]:8080|unset|allow|-
mapped-dotted|http://[::ffff:127.0.0.1]:8080|0|allow|-
mapped-hex|http://[::ffff:7f00:1]:8080|unset|allow|-
remote-ipv4-unset|http://192.0.2.1:8080|unset|reject|-
remote-ipv4-zero|http://192.0.2.1:8080|0|reject|-
remote-ipv4-opt-in|http://192.0.2.1:8080|1|allow|--allow-insecure-http
remote-ipv6-zero|http://[2001:db8::1]:8080|0|reject|-
remote-ipv6-opt-in|HTTP://[2001:db8::1]:8080|1|allow|--allow-insecure-http
remote-hostname-zero|http://gateway.example.test:8080|0|reject|-
remote-hostname-opt-in|http://gateway.example.test:8080/path|1|allow|--allow-insecure-http
mapped-remote-dotted|http://[::ffff:192.0.2.1]:8080|1|allow|--allow-insecure-http
mapped-remote-hex|http://[::ffff:c000:201]:8080|unset|reject|-
userinfo|http://user@gateway.example.test:8080|1|reject|-
query|http://gateway.example.test:8080?x=1|1|reject|-
fragment|http://gateway.example.test:8080#fragment|1|reject|-
bad-port|http://gateway.example.test:bad|1|reject|-
empty-port|http://gateway.example.test:|1|reject|-
port-overflow|http://gateway.example.test:65536|1|reject|-
empty-host|http:///path|1|reject|-
unbracketed-ipv6|http://::1|1|reject|-
malformed-bracket|http://[::1:8080|1|reject|-
invalid-ipv4|http://999.0.0.1:8080|1|reject|-
unsupported-scheme|ftp://gateway.example.test|1|reject|-
CASES

# 两个真实调用点必须都经过同一 argv 构造函数，且各出现一次。
rw_paths=$(awk '/^[[:space:]]*run_fuse_mount rw / { count++ } END { print count + 0 }' "$REAL_SCRIPT")
ro_paths=$(awk '/^[[:space:]]*run_fuse_mount ro / { count++ } END { print count + 0 }' "$REAL_SCRIPT")
if [ "$rw_paths" -eq 1 ]; then pass rw-call-path; else fail rw-call-path "count=$rw_paths want 1"; fi
if [ "$ro_paths" -eq 1 ]; then pass ro-call-path; else fail ro-call-path "count=$ro_paths want 1"; fi

# 替换最底层 exec，只捕获真实 argv 构造结果。
invoke_fuse_mount() {
  CAPTURE_ALLOW=0
  CAPTURE_READ_ONLY=0
  CAPTURE_CREDENTIAL_STDIN=0
  for argument do
    case "$argument" in
      --allow-insecure-http) CAPTURE_ALLOW=$((CAPTURE_ALLOW + 1)) ;;
      --read-only) CAPTURE_READ_ONLY=$((CAPTURE_READ_ONLY + 1)) ;;
      --credential-stdin) CAPTURE_CREDENTIAL_STDIN=$((CAPTURE_CREDENTIAL_STDIN + 1)) ;;
    esac
  done
}

BIN=/not-executed
MEM_ENDPOINT=http://192.0.2.1:8080
MEM_INSTANCE=test-instance
MEM_REGION=test-region
MEM_STORE=test-store
MEM_SCOPE='test-app#test-tenant#test-agent#test-run'
MEM_ALLOW_INSECURE_HTTP=1
export BIN MEM_ENDPOINT MEM_INSTANCE MEM_REGION MEM_STORE MEM_SCOPE MEM_ALLOW_INSECURE_HTTP
configure_http_transport "$MEM_ENDPOINT" >/dev/null 2>&1 || fail mount-policy "remote opt-in rejected"

run_fuse_mount rw test-session /test-rw
if [ "$CAPTURE_ALLOW" -eq 1 ] && [ "$CAPTURE_READ_ONLY" -eq 0 ] && [ "$CAPTURE_CREDENTIAL_STDIN" -eq 1 ]; then
  pass rw-argv
else
  fail rw-argv "allow=$CAPTURE_ALLOW readOnly=$CAPTURE_READ_ONLY credentialStdin=$CAPTURE_CREDENTIAL_STDIN"
fi

run_fuse_mount ro test-session-ro /test-ro
if [ "$CAPTURE_ALLOW" -eq 1 ] && [ "$CAPTURE_READ_ONLY" -eq 1 ] && [ "$CAPTURE_CREDENTIAL_STDIN" -eq 1 ]; then
  pass ro-argv
else
  fail ro-argv "allow=$CAPTURE_ALLOW readOnly=$CAPTURE_READ_ONLY credentialStdin=$CAPTURE_CREDENTIAL_STDIN"
fi

unset MEM_ALLOW_INSECURE_HTTP
printf 'HTTP POLICY RESULT: PASS=%d FAIL=%d\n' "$PASS" "$FAIL"
[ "$FAIL" -eq 0 ]

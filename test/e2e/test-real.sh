#!/bin/sh
# agent-memory-fuse 真实后端 e2e：对真实 Agent Memory 服务跑 FUSE 语义套件。
# 需要环境变量 MEM_ENDPOINT/MEM_REGION/MEM_AK/MEM_SK/MEM_INSTANCE/MEM_STORE/MEM_SCOPE，
# $BIN 下有 agent-memory-fuse.bin / fsops.bin / memctl.bin。
# 与 mock 套件的差异：无故障注入（429/403/LOCK_CONFLICT 已由 mock 套件覆盖），
# 跳过 2000 文件配额灌满；新增服务端 canonical 路径形态与跨客户端一致性断言。
BIN=${BIN:-/tmp/fuse-test}
M=${MNT:-/tmp/fuse-test/rm1}
RO=${RO_MNT:-/tmp/fuse-test/rm2}
F=$BIN/fsops.bin
MC=$BIN/memctl.bin
LOG=/tmp/agent-memory-fuse-real.log
PASS=0; FAIL=0
chk() {
  case "$3" in
    *"$2"*) echo "PASS  $1"; PASS=$((PASS+1)) ;;
    *) echo "FAIL  $1 | want[$2] got[$3]"; FAIL=$((FAIL+1)) ;;
  esac
}
do_umount() {
  if command -v fusermount >/dev/null 2>&1; then fusermount -u "$1"; else umount "$1"; fi
}

# LOOPBACK_HELPER_BEGIN
# 只按 URL authority 中的字面地址判断，不做 DNS 解析，与 Go client 的语义一致。
valid_http_port() {
  case "$1" in ''|*[!0-9]*) return 1 ;; esac
  [ "$1" -le 65535 ] 2>/dev/null
}

classify_http_authority() {
  authority=$1
  case "$authority" in ''|*@*) return 2 ;; esac
  bracketed=0
  case "$authority" in
    \[*\]*)
      bracketed=1
      host=${authority#\[}
      host=${host%%\]*}
      suffix=${authority#*\]}
      case "$suffix" in
        '') ;;
        :*)
          port=${suffix#:}
          valid_http_port "$port" || return 2
          ;;
        *) return 2 ;;
      esac
      ;;
    \[*|*\]*) return 2 ;;
    *)
      host=${authority%%:*}
      if [ "$host" != "$authority" ]; then
        port=${authority#*:}
        valid_http_port "$port" || return 2
      fi
      ;;
  esac
  [ -n "$host" ] || return 2

  printf '%s\n' "$host" | awk -v bracketed="$bracketed" '
    function decimal_octet(value) {
      return value ~ /^(0|[1-9][0-9]?[0-9]?)$/ && value + 0 <= 255
    }
    function valid_ipv4(value, parts, count, i) {
      count = split(value, parts, "[.]")
      if (count != 4) return 0
      for (i = 1; i <= 4; i++) if (!decimal_octet(parts[i])) return 0
      return 1
    }
    function ipv4_loopback(value, parts) {
      if (!valid_ipv4(value)) return 0
      split(value, parts, "[.]")
      return parts[1] == "127"
    }
    function valid_hostname(value, labels, count, i) {
      if (length(value) > 253 || value !~ /^[A-Za-z0-9.-]+$/) return 0
      count = split(value, labels, "[.]")
      for (i = 1; i <= count; i++) {
        if (length(labels[i]) > 63 || labels[i] !~ /^[A-Za-z0-9]([A-Za-z0-9-]*[A-Za-z0-9])?$/) return 0
      }
      return 1
    }
    function zero_group(value) { return value ~ /^0{1,4}$/ }
    function one_group(value) { return value ~ /^0{0,3}1$/ }
    function hex_group(value) {
      return value ~ /^[0-9A-Fa-f][0-9A-Fa-f]?[0-9A-Fa-f]?[0-9A-Fa-f]?$/
    }
    function last_colon(value, i) {
      for (i = length(value); i > 0; i--) if (substr(value, i, 1) == ":") return i
      return 0
    }
    function mapped_prefix(value, marker, before, after, combined, parts, count, i) {
      if (value !~ /^[0-9A-Fa-f:]+$/) return 0
      marker = index(value, "::")
      if (marker) {
        before = substr(value, 1, marker - 1)
        after = substr(value, marker + 2)
        if (after == "" || index(after, "::")) return 0
        combined = before == "" ? after : before ":" after
        count = split(combined, parts, ":")
        if (count >= 6) return 0
      } else {
        count = split(value, parts, ":")
        if (count != 6) return 0
      }
      for (i = 1; i < count; i++) if (!zero_group(parts[i])) return 0
      return tolower(parts[count]) == "ffff"
    }
    function mapped_ipv6_loopback(value, separator, tail, remainder, first, prefix) {
      separator = last_colon(value)
      if (!separator) return 0
      tail = substr(value, separator + 1)
      remainder = substr(value, 1, separator - 1)
      if (index(tail, ".")) return mapped_prefix(remainder) && ipv4_loopback(tail)

      separator = last_colon(remainder)
      if (!separator) return 0
      first = substr(remainder, separator + 1)
      prefix = substr(remainder, 1, separator - 1)
      return mapped_prefix(prefix) && first ~ /^7[fF][0-9A-Fa-f][0-9A-Fa-f]$/ && hex_group(tail)
    }
    function valid_ipv6(value, marker, before, after, combined, parts, count, units, i) {
      if (value !~ /^[0-9A-Fa-f:.]+$/) return 0
      marker = index(value, "::")
      if (marker) {
        before = substr(value, 1, marker - 1)
        after = substr(value, marker + 2)
        if (index(after, "::")) return 0
        combined = before == "" ? after : (after == "" ? before : before ":" after)
      } else {
        combined = value
      }
      count = combined == "" ? 0 : split(combined, parts, ":")
      units = 0
      for (i = 1; i <= count; i++) {
        if (index(parts[i], ".")) {
          if (i != count || !valid_ipv4(parts[i])) return 0
          units += 2
        } else {
          if (!hex_group(parts[i])) return 0
          units++
        }
      }
      return marker ? units < 8 : units == 8
    }
    function ipv6_loopback(value, marker, before, after, combined, parts, count, i) {
      if (value !~ /^[0-9A-Fa-f:]+$/) return 0
      marker = index(value, "::")
      if (marker) {
        before = substr(value, 1, marker - 1)
        after = substr(value, marker + 2)
        if (after == "" || index(after, "::")) return 0
        combined = before == "" ? after : before ":" after
        count = split(combined, parts, ":")
        if (count >= 8) return 0
      } else {
        count = split(value, parts, ":")
        if (count != 8) return 0
      }
      for (i = 1; i < count; i++) if (!zero_group(parts[i])) return 0
      return one_group(parts[count])
    }
    {
      if (bracketed) {
        if (!valid_ipv6($0)) exit 2
        if (ipv6_loopback($0) || mapped_ipv6_loopback($0)) exit 0
        exit 1
      }
      if ($0 ~ /^[0-9.]+$/) {
        if (!valid_ipv4($0)) exit 2
        exit ipv4_loopback($0) ? 0 : 1
      }
      if (!valid_hostname($0)) exit 2
      exit tolower($0) == "localhost" ? 0 : 1
    }
  '
}

configure_http_transport() {
  endpoint=$1
  ALLOW_INSECURE_HTTP_ARG=
  case "$endpoint" in
    *\?*|*\#*)
      echo "ABORT: MEM_ENDPOINT must not contain a query or fragment"
      return 1
      ;;
  esac
  case "$endpoint" in
    [hH][tT][tT][pP][sS]://*) endpoint_scheme=https ;;
    [hH][tT][tT][pP]://*) endpoint_scheme=http ;;
    *)
      echo "ABORT: MEM_ENDPOINT must use http:// or https://"
      return 1
      ;;
  esac
  endpoint_remainder=${endpoint#*://}
  endpoint_authority=${endpoint_remainder%%/*}
  classify_http_authority "$endpoint_authority"
  authority_class=$?
  if [ "$authority_class" -eq 2 ]; then
    echo "ABORT: MEM_ENDPOINT has an invalid or unsupported authority"
    return 1
  fi
  if [ "$endpoint_scheme" = https ] || [ "$authority_class" -eq 0 ]; then
    return 0
  fi
  if [ "${MEM_ALLOW_INSECURE_HTTP:-}" = 1 ]; then
    ALLOW_INSECURE_HTTP_ARG=--allow-insecure-http
    return 0
  fi
  echo "ABORT: non-loopback HTTP requires MEM_ALLOW_INSECURE_HTTP=1 (test-only opt-in)"
  return 1
}

invoke_fuse_mount() {
  exec "$@"
}

run_fuse_mount() {
  mount_mode=$1
  mount_session=$2
  mountpoint=$3
  set -- "$BIN/agent-memory-fuse.bin" mount --endpoint "$MEM_ENDPOINT" --instance "$MEM_INSTANCE"
  if [ -n "$ALLOW_INSECURE_HTTP_ARG" ]; then
    set -- "$@" "$ALLOW_INSECURE_HTTP_ARG"
  fi
  set -- "$@" --region "$MEM_REGION" --credential-stdin \
    --store "$MEM_STORE" --scope "$MEM_SCOPE" --session "$mount_session"
  case "$mount_mode" in
    rw) ;;
    ro) set -- "$@" --read-only ;;
    *) echo "ABORT: invalid mount mode"; return 1 ;;
  esac
  set -- "$@" "$mountpoint"
  invoke_fuse_mount "$@"
}
# LOOPBACK_HELPER_END

echo "### agent-memory-fuse REAL-BACKEND e2e: endpoint=$MEM_ENDPOINT store=$MEM_STORE scope=$MEM_SCOPE ###"

# --- 必填环境变量 fail-fast ---
# 缺任何一个都不会白跑真实写入:memctl 自己就要这七个变量(MEM_REGION 是因为下面无条件
# export MEM_SIGNV4=1 才成为必需),所以最晚在 seed put 那步就死了。问题是那步只吐一句
# "seed put failed",而 purge/del 把 stderr 吞掉了,真因不可见。前置成具名 ABORT 只为
# 把"缺哪个变量"直接说出来。
for v in MEM_ENDPOINT MEM_REGION MEM_AK MEM_SK MEM_INSTANCE MEM_STORE MEM_SCOPE; do
  eval "[ -n \"\$$v\" ]" || { echo "ABORT: $v required"; exit 1; }
done
# 默认仍强制 HTTPS；非回环 HTTP 只允许在显式标记的受控测试环境中使用。
configure_http_transport "$MEM_ENDPOINT" || exit 1
# memctl 默认 v2 而 FUSE 客户端已固定 v4:显式对齐两侧签名版本,否则在只收 v4 的地域上
# memctl 会先在 seed 阶段 ABORT,把真正要测的 FUSE 行为掩盖掉。
MEM_SIGNV4=1; export MEM_SIGNV4

# --- 清场 + 种子 ---
$MC purge bulk/ > /dev/null 2>&1
for f in probe.md slash.md seed.md a.md big.md big2.md bad.md cas.md ghost.md moved.md dir1/in.md r.md; do
  $MC del "$f" > /dev/null 2>&1
done
$MC put seed.md hello-seed > /dev/null || { echo "ABORT: seed put failed"; exit 1; }
echo "seeding 520 files (concurrency 16, six 100-row pages)..."
$MC seed 520 bulk/

mkdir -p $M
printf 'aksk:%s:%s\n' "$MEM_AK" "$MEM_SK" | (
  unset MEM_AK MEM_SK
  run_fuse_mount rw "sess-real-$$" "$M"
) > "$LOG" 2>&1 &
MNT_PID=$!
ready=0
i=0; while [ $i -lt 100 ]; do
  grep -q " $M " /proc/mounts && ready=1 && break
  sleep 0.3; i=$((i+1))
done
chk "mount visible in /proc/mounts" "1" "$ready"
# 挂载失败时把日志打出来:endpoint/region/凭证任一有问题都在 fs.Mount 之前退出,
# 只 grep mounted 会什么都不显示,让所有后续断言连锁失败且看不到真因。
[ "$ready" = 1 ] || { echo "--- mount log ($LOG) ---"; cat $LOG; }
grep mounted $LOG

# --- 基础读写（每个 flush/close 都是真实网络提交）---
chk "initial ls sees seed+bulk"  "n=2 [bulk/ seed.md]"  "$($F ls $M)"
chk "read cross-client seed"     'content="hello-seed"' "$($F read $M/seed.md)"
chk "bulk pagination count=520"  "520"                  "$(ls $M/bulk | wc -l)"
chk "create new file"            "open=OK write=OK close=OK" "$($F write $M/a.md 'v1 content')"
chk "read back new file"         'content="v1 content"' "$($F read $M/a.md)"
chk "stat size"                  "size=10"              "$($F stat $M/a.md)"
chk "overwrite O_TRUNC"          "close=OK"             "$($F write $M/a.md 'v2')"
chk "read after overwrite"       'content="v2"'         "$($F read $M/a.md)"
chk "append O_APPEND"            "close=OK"             "$($F append $M/a.md '+more')"
chk "read after append"          'content="v2+more"'    "$($F read $M/a.md)"
chk "truncate to 2"              "truncate=OK"          "$($F truncate $M/a.md 2)"
chk "read after truncate"        'content="v2"'         "$($F read $M/a.md)"
chk "touch timestamp no-op"      "touch=OK"             "$($F touch $M/a.md)"
chk "fsync commits"              "fsync=OK close=OK"    "$($F fsync $M/a.md 'fsynced')"

# --- 跨客户端一致性 + 服务端 canonical 形态 ---
chk "sdk sees agent-memory-fuse write" '"content": "fsynced"' "$($MC get a.md)"
chk "server canonical has slash" '"path": "/a.md"'      "$($MC get a.md)"

# --- 目录语义 ---
chk "write into missing dir"     "open=ENOENT"          "$($F write $M/no/such/x.md hi)"
chk "mkdir"                      "mkdir=OK"             "$($F mkdir $M/dir1)"
chk "create inside new dir"      "close=OK"             "$($F write $M/dir1/in.md inside)"
chk "ls new dir"                 "n=1 [in.md]"          "$($F ls $M/dir1)"
chk "rename across dirs"         "rename=OK"            "$($F rename $M/dir1/in.md $M/moved.md)"
chk "read moved file"            'content="inside"'     "$($F read $M/moved.md)"
chk "rename onto existing"       "rename=OK"            "$($F rename $M/moved.md $M/a.md)"
chk "read after overwrite-rename" 'content="inside"'    "$($F read $M/a.md)"
chk "rm file"                    "rm=OK"                "$($F rm $M/a.md)"
chk "read deleted -> ENOENT"     "read=ENOENT"          "$($F read $M/a.md)"

# --- 边界错误码 ---
chk "write exactly 100KiB ok"    "close=OK"             "$($F write $M/big.md size:102400)"
chk "write 100KiB+1 -> EFBIG"    "EFBIG"                "$($F write $M/big2.md size:102401)"
chk "invalid utf8 -> EILSEQ"     "EILSEQ"               "$($F write $M/bad.md badutf8)"

# --- 真实 latestVersionId 资源版本冲突 ESTALE（第二客户端经 SDK 并发改写）---
$F write $M/cas.md "local-v1" > /dev/null
rm -f /tmp/notify
$F stale $M/cas.md "local-v2" /tmp/notify > /tmp/stale.out &
STALE_PID=$!
sleep 0.5
$MC put cas.md server-side-change > /dev/null
touch /tmp/notify
wait $STALE_PID
chk "concurrent resource version -> ESTALE" "close=ESTALE" "$(cat /tmp/stale.out)"
sleep 1.2
chk "post-conflict read = server" 'content="server-side-change"' "$($F read $M/cas.md)"

# --- create 撞服务端已有路径 EEXIST ---
$MC put ghost.md server-only > /dev/null
chk "create over server file -> EEXIST" "EEXIST"        "$($F write-excl $M/ghost.md race)"
chk "exclusive create collision preserves server content" '"content": "server-only"' "$($MC get ghost.md)"

# --- statfs 配额可见 ---
chk "df -i shows 2000 inode quota" "2000"               "$(df -i $M 2>/dev/null | tail -1)"

# --- 卸载 ---
do_umount $M
chk "umount clean"               "0"                    "$(grep -c " $M " /proc/mounts)"
sleep 1
if kill -0 $MNT_PID 2>/dev/null; then
  chk "agent-memory-fuse exits after umount" "exited" "still-running"; kill $MNT_PID
else
  chk "agent-memory-fuse exits after umount" "exited" "exited"
fi

# --- --read-only 挂载 ---
mkdir -p $RO
printf 'aksk:%s:%s\n' "$MEM_AK" "$MEM_SK" | (
  unset MEM_AK MEM_SK
  run_fuse_mount ro "sess-real-ro-$$" "$RO"
) > /tmp/agent-memory-fuse-real-ro.log 2>&1 &
i=0; while [ $i -lt 100 ]; do grep -q " $RO " /proc/mounts && break; sleep 0.3; i=$((i+1)); done
chk "ro-mount read works"        'content="hello-seed"' "$($F read $RO/seed.md)"
chk "ro-mount write -> EROFS"    "EROFS"                "$($F write $RO/w.md nope)"
chk "ro-mount rm -> EROFS"       "EROFS"                "$($F rm $RO/seed.md)"
do_umount $RO

# --- 清理 ---
# big2.md：EFBIG 用例的短写残留——102401 字节被内核按 MaxWrite 拆成 102400+1
# 两次 WRITE，第二次 EFBIG，close 提交已成功的 102400 字节前缀（POSIX 短写语义）。
echo "cleaning up..."
$MC purge bulk/ | tail -1
for f in seed.md big.md big2.md cas.md ghost.md a.md; do $MC del "$f" > /dev/null 2>&1; done
chk "store clean after purge"    "total=0"              "$($MC list | tail -1)"

echo "==================================="
echo "REAL-BACKEND RESULT: PASS=$PASS FAIL=$FAIL"
echo "==================================="
[ $FAIL -eq 0 ]

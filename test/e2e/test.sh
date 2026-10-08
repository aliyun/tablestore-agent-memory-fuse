#!/bin/sh
# agent-memory-fuse FUSE 语义测试。
# Pod 模式（默认）: 在 unshare -U -r -m 内运行，经 fusemnt 直接 mount(2)。
# 本机模式: USE_FUSEMNT=0 BIN=... MNT=... RO_MNT=... 直接跑，agent-memory-fuse 走 fusermount。
BIN=${BIN:-/tmp}
M=${MNT:-/mnt/mem}
RO=${RO_MNT:-/mnt/ro}
USE_FUSEMNT=${USE_FUSEMNT:-1}
F=$BIN/fsops.bin
LOG=/tmp/agent-memory-fuse-run.log
PASS=0; FAIL=0

chk() { # chk <名称> <期望片段> <实际输出>
  case "$3" in
    *"$2"*) echo "PASS  $1"; PASS=$((PASS+1)) ;;
    *) echo "FAIL  $1 | want[$2] got[$3]"; FAIL=$((FAIL+1)) ;;
  esac
}
ctl() { wget -q -O- "http://127.0.0.1:8770/$1" 2>/dev/null; }
do_mount() { # do_mount <mountpoint> <agent-memory-fuse args...>
  target=$1; shift
  if [ "$USE_FUSEMNT" = "1" ]; then
    $BIN/fusemnt.bin "$target" $BIN/agent-memory-fuse.bin "$@"
  else
    $BIN/agent-memory-fuse.bin "$@" "$target"
  fi
}
do_umount() { # fusermount 可用则用之，否则直接 umount（userns 内可用）
  if command -v fusermount >/dev/null 2>&1; then fusermount -u "$1"; else umount "$1"; fi
}

echo "### agent-memory-fuse FUSE semantics test on $(uname -r) ###"
ctl "__reset" > /dev/null
ctl "__put?path=seed.md&content=hello-seed" > /dev/null
ctl "__seed?n=600&prefix=bulk/" > /dev/null   # 六个 100 行页面，覆盖 Refresh 分页

mkdir -p $M
printf '%s\n' 'sts:test-old-ak:test-old-sk:test-old-token' | do_mount $M mount \
  --endpoint http://127.0.0.1:8770 --instance test \
  --region test-region \
  --credential-stdin --store memstore_t \
  --scope app#tenant#agent#run --session sess-test > $LOG 2>&1 &
MNT_PID=$!

ready=0
i=0; while [ $i -lt 50 ]; do
  grep -q " $M " /proc/mounts && ready=1 && break
  sleep 0.2; i=$((i+1))
done
chk "mount visible in /proc/mounts" "1" "$ready"

# --- 基础读写 ---
chk "initial ls sees seed+bulk"  "n=2 [bulk/ seed.md]"  "$($F ls $M)"
chk "read seeded file"           'content="hello-seed"' "$($F read $M/seed.md)"
chk "request before STS rotation uses old tuple" \
  "test-old-ak test-old-token" "$(ctl __lastauth)"
printf '%s' '{"accessKeyId":"test-new-ak","accessKeySecret":"test-new-sk","stsToken":"test-new-token"}' | \
  $BIN/agent-memory-fuse.bin rotate-credential --mountpoint "$M"
sleep 1.2
chk "read after STS rotation without remount" \
  'content="hello-seed"' "$($F read $M/seed.md)"
chk "request after STS rotation uses new tuple" \
  "test-new-ak test-new-token" "$(ctl __lastauth)"
chk "bulk pagination count=600"  "600"                  "$(ls $M/bulk | wc -l)"
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

# --- latestVersionId 资源版本冲突 ESTALE ---
$F write $M/cas.md "local-v1" > /dev/null
rm -f /tmp/notify
$F stale $M/cas.md "local-v2" /tmp/notify > /tmp/stale.out &
STALE_PID=$!
sleep 0.5
ctl "__put?path=cas.md&content=server-side-change" > /dev/null
touch /tmp/notify
wait $STALE_PID
chk "concurrent resource version -> ESTALE" "close=ESTALE" "$(cat /tmp/stale.out)"
sleep 1.2  # 让 1s TTL 过期，重新拉取服务端内容
chk "post-conflict read = server" 'content="server-side-change"' "$($F read $M/cas.md)"

# --- create 撞已有路径 EEXIST ---
ctl "__put?path=ghost.md&content=server-only" > /dev/null
chk "create over server file -> EEXIST" "EEXIST"        "$($F write-excl $M/ghost.md race)"
chk "exclusive create collision preserves server content" '"Path":"/ghost.md","Content":"server-only"' "$(ctl "__dump")"

# --- 配额 ENOSPC ---
CUR=$(ctl "__count")
ctl "__maxfiles?v=$((CUR+1))" > /dev/null
chk "one more file within quota" "close=OK"             "$($F write $M/q1.md fits)"
chk "quota exceeded -> ENOSPC"   "ENOSPC"               "$($F write $M/q2.md overflow)"
ctl "__maxfiles?v=2000" > /dev/null

# --- 瞬时错误重试 ---
ctl "__fault?action=UpdateItem&status=429&code=THROTTLED&times=2" > /dev/null
chk "429x2 retried -> success"   "close=OK"             "$($F write $M/big.md retry-ok)"
ctl "__fault?action=UpdateItem&status=429&code=THROTTLED&times=3" > /dev/null
chk "429x3 exhausts -> EAGAIN"   "EAGAIN"               "$($F write $M/big.md retry-fail)"
ctl "__fault?action=UpdateItem&status=409&code=LOCK_CONFLICT&times=1" > /dev/null
chk "LOCK_CONFLICT not replayed -> EAGAIN" "EAGAIN" "$("$F" write "$M/big.md" lock-fail)"
sleep 1.2  # 过 TTL 后重读，确认单次冲突未推进服务端资源版本
chk "LOCK_CONFLICT leaves content unchanged" 'content="retry-ok"' "$("$F" read "$M/big.md")"
chk "next independent write succeeds" "close=OK" "$("$F" write "$M/big.md" lock-ok)"

# --- 鉴权失败 EACCES（STS 过期路径）---
ctl "__put?path=auth.md&content=auth-probe" > /dev/null
ctl "__fault?action=GetItem&status=403&code=AUTH_FAILED&times=1" > /dev/null
chk "403 -> EACCES"              "EACCES"               "$($F read $M/auth.md)"
chk "auth failure logged once"   "1"                    "$(grep -c 'file memory auth failed' $LOG)"

# --- 服务端只读 EROFS ---
ctl "__readonly?v=1" > /dev/null
chk "readonly store -> EROFS"    "EROFS"                "$($F write $M/big.md ro-write)"
ctl "__readonly?v=0" > /dev/null

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

# --- --read-only 挂载开关 ---
ctl "__reset" > /dev/null; ctl "__put?path=r.md&content=ro" > /dev/null
mkdir -p $RO
printf '%s\n' 'aksk:test-ak:test-sk' | do_mount $RO mount \
  --endpoint http://127.0.0.1:8770 --instance test \
  --region test-region \
  --credential-stdin --store memstore_t \
  --scope app#tenant#agent#run --session sess-test --read-only > /tmp/agent-memory-fuse-ro.log 2>&1 &
i=0; while [ $i -lt 50 ]; do grep -q " $RO " /proc/mounts && break; sleep 0.2; i=$((i+1)); done
chk "ro-mount read works"        'content="ro"'         "$($F read $RO/r.md)"
chk "ro-mount write -> EROFS"    "EROFS"                "$($F write $RO/w.md nope)"
chk "ro-mount rm -> EROFS"       "EROFS"                "$($F rm $RO/r.md)"
do_umount $RO

echo "==================================="
echo "RESULT: PASS=$PASS FAIL=$FAIL"
echo "==================================="
[ $FAIL -eq 0 ]

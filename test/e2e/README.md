# agent-memory-fuse e2e 验证工具链

FUSE 语义级端到端验证套件，覆盖 fusermount 回退路径、userns DirectMount
直挂路径与真实服务端三种环境。

## 组件

| 组件 | 作用 |
|---|---|
| `mockfilemem/` | 最终 Item 协议的有状态内存 mock：实现 basic/full view、canonical 路径、稳定 item ID、每次写/重命名推进资源版本、最大 100 条分页和 opaque `nextToken`；同时支持 V4 请求验签与响应签名（**v4-only**，收到 v2 签名一律 403；`MOCK_REGION` 覆盖期望的 signregion，默认 `test-region`）以及 `__fault` `__put` `__seed` `__readonly` `__maxfiles` 等控制端点 |
| `memctl/` | 直连真实服务端的 CLI（经 SDK）：store 列表/创建/删除、item 增删改查、并发种子/清理；内容读使用 full view，List 每页 100 条并跟随 `nextToken`，对已有 item 执行 `put`/`del` 时每次都重读最新 item ID 和版本后提交，并整体退避重试 LOCK_CONFLICT/429；这只是测试数据准备与清理的协调策略，不代表 FUSE mutation 会重放 write。配置走 `MEM_ENDPOINT/MEM_AK/MEM_SK/MEM_INSTANCE/MEM_STORE/MEM_SCOPE` 环境变量，v4 签名加 `MEM_SIGNV4=1 MEM_REGION=...`；memctl 自身默认 v2，而 FUSE 客户端**固定** V4，所以 `test-real.sh` 无条件 `export MEM_SIGNV4=1` 对齐两侧，真实套件必须提供 `MEM_REGION` |
| `fsops/` | 语义探针：逐系统调用打印 errno 名。必需——agent-memory-fuse 在 close/flush 提交，shell 重定向会吞掉 close 错误 |
| `fusemnt/` | 无 fusermount 环境的挂载垫片（open /dev/fuse + mount(2)，经 go-fuse 魔法挂载点 `/dev/fd/3` 移交）。agent-memory-fuse 开启 DirectMount 后不再必需，保留作参考 |
| `test.sh` | signed mock 挂载全语义套件（51 个运行时断言）：读写/目录/EFBIG/EILSEQ/ESTALE/EAGAIN/EEXIST/ENOSPC/429 重试/EACCES/EROFS/statfs/资源版本冲突/STS 在线轮转/卸载；首刷遍历 `seed.md` 和 600 个 bulk item，共 601 项、七页（六个 100 条满页加一个 partial 页）。`USE_FUSEMNT=0 BIN=... MNT=... RO_MNT=...` 本机跑；无特权挂载权限的容器环境用 `unshare -U -r -m sh test.sh` |
| `test-real.sh` | 真实后端挂载套件（41 个运行时断言）：用 memctl 替代 mock 控制端点，挂载凭证经 stdin 传递，跳过故障注入与配额灌满；验证服务端 canonical 路径、`seed.md` 和 520 个 bulk item 共 521 项的六页 List、跨客户端（SDK↔FUSE）一致性，以及资源版本下的写、重命名和删除语义 |

## 最终 Item 协议的验证层次

精确 wire request shape 与 client facade 的重试／冲突判定由 `internal/client/client_test.go` 验证：

- FUSE 内部相对路径转为带前导斜杠的 canonical item 路径，非空 List prefix 按路径段对齐。
- Lookup 和无 handle 的文件 Getattr 使用 basic-view Stat，open/read 使用 full-view Get，Create 使用 `AddItem` full view；递归 List 每页 100 条并跟随 `nextToken`。
- 写内容使用稳定 `itemId` + `content` + `precondition.expectedVersionId`；Rename 使用同一 ID、目标 `path`、`overwrite` 和版本前置条件；Delete 使用 item ID 和版本前置条件。Rename 响应中的新 `latestVersionId` 会被保存，即使内容不变也能识别后续 stale handle。
- mutation 收到首个 `LOCK_CONFLICT` 后不重放 write，而是按 item ID 做一次逻辑 authoritative Get（该安全读可以按预算重试）：目标状态匹配则确认成功；版本和目标均未变化时保留原冲突并映射 `EAGAIN`；版本已变化但未达到目标时映射 `ESTALE`；Delete 精确 item 已不存在可确认成功。已准入 mutation 的 deadline 到期后也只用新的有界 context 做安全 Get，不重放 write；主动 cancel 仍立即停止。Create 响应没有 owner 候选 itemId 时，即使权威路径内容相同也不认领 `O_EXCL` 成功，避免把另一写入者的同内容文件误判成自己创建。
- 安全读 List/Stat/Read 对 429/500/503/504 和 `LOCK_CONFLICT` 使用客户端既有的有界恢复策略；mutation 仅对明确未执行的 429 自动重试，不重放 `LOCK_CONFLICT` 或模糊的 500/transport 结果。

FUSE filesystem/cache/concurrency 语义由 `internal/fs/filesystem_test.go` 与 `internal/fs/root_test.go` 验证：

- mount 先用完整分页 List 建立初始 scope 快照。常规 TTL 完整刷新只由 Readdir/`EnsureFresh` 触发；成功发布时，正常并发的 `EnsureFresh` 合并为一次扫描。若快照因 mutation generation 或 observation epoch 竞速而被丢弃，则调度一个合并的后台恢复 worker，最多再做两次完整分页 List。
- 同路径、同 mutation generation 的 cold Stat 会合并，不同路径的 Stat 与 full-view Open 可以并行；完整文件写按路径 keyed lock 串行，rename 同时锁源和目标，所有 Update/Rename/Delete 继续使用版本前置条件。
- 尚未首次 flush 的 Create 只有本地占位、没有 itemId：unlink 本地清除并禁止旧 handle 复活；rename 到空目标本地迁移后首次在新路径 Add，目标已占用则返回 `EEXIST`，绝不发送空 itemId。
- 默认 `MetadataTTL=5s`、`NegativeTTL=1s`、`DirectoryTTL=10s`；negative cache 有界为 4096 项，过期或观察 epoch 变化的旧结果不会回写到新快照。
- `statfs` 仍按常规 `MaxFiles=2000` 展示；本地 Create 只为并发和快照滞后保留 100 个 slack、最多到 2100，真实服务端 `QUOTA_EXCEEDED` 始终权威。这不是服务端配额变更。

三个 TTL 的 CLI 默认值、内核 Entry/Attr/Negative timeout 与 `--allow-insecure-http` 参数传递由 `cmd/agent-memory-fuse/main_test.go` 验证；真实 E2E 脚本的 HTTP 默认拒绝、回环规则和显式测试开关另由 `test/e2e/test-real-http-policy-test.sh` 验证。

signed mock 与真实挂载 E2E 验证的是上述协议在 FUSE 层呈现的最终行为：多页首刷、内容读写、重命名/删除、跨客户端一致性和 stale-version `ESTALE`。mock 套件另通过单次故障注入验证：一次 mutation 收到版本未变的 `LOCK_CONFLICT` 时返回 `EAGAIN` 且服务端内容不变；故障已消费后，下一次独立 write 才成功。

## 用法示例

```bash
# 构建（同主模块）
go build -o /tmp/e2e/mockfilemem.bin ./test/e2e/mockfilemem
go build -o /tmp/e2e/memctl.bin ./test/e2e/memctl
go build -o /tmp/e2e/fsops.bin ./test/e2e/fsops
CGO_ENABLED=0 go build -o /tmp/e2e/agent-memory-fuse.bin ./cmd/agent-memory-fuse

# mock 套件（本机）
/tmp/e2e/mockfilemem.bin &   # 监听 127.0.0.1:8770
USE_FUSEMNT=0 BIN=/tmp/e2e MNT=/tmp/e2e/m1 RO_MNT=/tmp/e2e/m2 sh test/e2e/test.sh

# 真实后端套件默认要求 MEM_ENDPOINT 使用 https://。
export MEM_ENDPOINT=https://... MEM_REGION=... MEM_AK=... MEM_SK=... MEM_INSTANCE=... \
       MEM_STORE=... MEM_SCOPE='app#tenant#agent#run'
BIN=/tmp/e2e sh test/e2e/test-real.sh

# 仅限隔离、受控的测试网关：非回环 HTTP 必须显式二次确认，脚本才会传
# --allow-insecure-http。不要在生产环境使用此开关。
export MEM_ENDPOINT=http://192.0.2.1:8080 MEM_ALLOW_INSECURE_HTTP=1
BIN=/tmp/e2e sh test/e2e/test-real.sh
```

`MEM_ALLOW_INSECURE_HTTP=1` 只让脚本为**非回环** HTTP 的读写两次挂载显式传入 `--allow-insecure-http`；未设置或不是 `1` 时脚本会在真实写入前拒绝这类 endpoint。字面 `localhost`、`127.0.0.0/8`、`::1` 回环 HTTP 无需该变量；判断不做 DNS 解析。该开关只适用于隔离、受控测试，明文传输会暴露凭证与数据，生产验收必须使用 HTTPS。API Key 还有 SDK `AllowInsecureAPIKey` 的独立保护，而 mount CLI 不提供该入口，因此 API Key 挂载仍必须使用 HTTPS。HTTP 重定向一律拒绝。

`test-real.sh` 使用三个缓存 TTL 的 CLI 默认值。直接复现挂载或调参时，可在 `agent-memory-fuse mount` 上使用正数 duration：

```bash
agent-memory-fuse mount ... \
  --metadata-ttl 5s \
  --negative-ttl 1s \
  --directory-ttl 10s \
  /tmp/e2e/memory
```

这些 TTL 没有对应的真实 E2E 环境变量；不要把凭证写入命令行或文档，真实脚本会通过 stdin 把 AK/SK 交给 mount。

## 服务端行为要点（真实套件依赖）

- **路径 canonical 形态**：服务端存储并返回**带前导斜杠**（`probe.md` → `/probe.md`）；agent-memory-fuse 内部索引保持挂载点相对路径，client 边界统一转换为带前导斜杠的服务端路径。
- **并发锁冲突**：同 scope 高并发写可能触发 `409 LOCK_CONFLICT`（"concurrent mutation in flight"）。由于服务端目前没有操作级幂等键，agent-memory-fuse 不会在同一次 mutation 内自动重放 write；权威回查无法确认成功时返回 `EAGAIN`，由调用方发起下一次独立操作。
- **EFBIG 短写**：写 100KiB+1 时内核按 `MaxWrite=102400` 拆分 WRITE，首笔恰好到上限成功、次笔 EFBIG，close 提交已成功的 102400 字节前缀（POSIX 短写语义，文件未超限）。
- **签名**：客户端固定 v4，`MEM_REGION` 必须精确匹配实例地域（错配返回 `OTSAuthFailed` 并回显 serverRegion）。
- **传输**：真实套件要求 `MEM_ENDPOINT` 使用 `https://`；明文 HTTP 仅限隔离测试网关（见上文 `MEM_ALLOW_INSECURE_HTTP`）。

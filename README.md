# agent-memory-fuse

将 Agent Memory 服务中的文件记忆挂载为 Linux 目录，通过文件操作读取、写入、重命名和删除记忆文本。

每个挂载点对应一个实例、一个 File Memory Store 和一个精确 Scope。单文件最大 **100 KiB**，内容必须是 **UTF-8 文本**。本工具不是通用 POSIX 文件系统，空目录不会持久化，不支持软链接、硬链接、ACL、稀疏文件、业务 xattr 或跨 session 文件锁。

当前版本为 Beta，长期运行仍有 RSS 缓慢增长的已知问题，见[运行限制](#运行限制)。

## 快速开始

### 1. 准备环境

- Linux，具备可访问的 `/dev/fuse` 和挂载权限。安装 `fusermount3` 或 `fusermount`，供挂载回退和 CLI 卸载使用；容器还需映射 `/dev/fuse` 并配置允许挂载的权限。
- Go 1.26.6 或更高版本，用于源码构建。
- 已支持下文 Item API 的 Agent Memory 服务，以及已创建的 `storageMode=filemem` Store。FUSE 不负责创建服务实例或 Store。
- 该实例的 HTTPS endpoint、地域和访问凭证。挂载初始化需要列举 Scope；读写挂载还需要相应的 Item 读写权限。

### 2. 构建

在仓库根目录执行：

```bash
go build -mod=vendor -o agent-memory-fuse ./cmd/agent-memory-fuse
```

`-mod=vendor` 使用仓库内的依赖快照，普通构建无需同级 SDK 源码目录。带版本信息和校验和的 Linux/amd64 制品见[发布构建](#发布构建)。

### 3. 挂载

先按[凭证说明](#凭证)准备凭证文件，将下面的实例、地域、Store、Scope 和 session 替换为自己的值。示例 endpoint 仅展示格式，不表示对应地域已经开通该服务。

```bash
mkdir -p "$HOME/mnt/memory"

./agent-memory-fuse mount \
  --endpoint https://your-instance.cn-hangzhou.ots.aliyuncs.com \
  --instance your-instance \
  --region cn-hangzhou \
  --credential-file /run/secrets/agent-memory-fuse/credential \
  --store your-filemem-store \
  --scope 'app#tenant#agent#run' \
  --session your-session \
  "$HOME/mnt/memory"
```

**`mount` 在前台运行。** 看到 `mounted` 日志后，在另一个终端访问挂载目录。仅需读取时，在挂载点参数前添加 `--read-only`。

`--endpoint`、`--instance`、`--store` 和 `--session` 均必填，包括只读挂载。所有选项都应放在最后的挂载点参数之前。

### 4. 卸载

关闭挂载目录中的文件，让其他进程退出该目录，等待在途写入结束。在另一个终端执行：

```bash
./agent-memory-fuse umount "$HOME/mnt/memory"
```

卸载成功后，前台挂载进程会退出。若提示 busy，用 `fuser -vm "$HOME/mnt/memory"` 定位占用者后重试；不要直接杀掉仍在写入的挂载进程。

## 凭证

### 文件或标准输入

`--credential-file` 和 `--credential-stdin` 必须且只能选择一个。挂载不接受命令行明文凭证，也不从环境变量读取凭证。

凭证文件必须由当前有效用户拥有，是直接引用的普通文件，不能是软链接，group/others 权限均须为 0（建议 `0400` 或 `0600`）。建议通过 tmpfs 或受控 Secret volume 提供，不要将真实凭证写进仓库、脚本或 shell 历史。

Kubernetes 投射 Secret 的文件通常通过软链接提供；先由受控 init/sidecar 原子复制为满足 owner/mode 约束的普通文件，再交给 `--credential-file`。

文件和 stdin 的输入上限均为 64 KiB，支持以下文本格式：

```text
apikey:<key>
aksk:<access-key-id>:<access-key-secret>
sts:<access-key-id>:<access-key-secret>:<security-token>
```

AK/SK 和 STS 也支持 JSON。下面仅为格式示例；`stsToken` 为空或省略表示 AK/SK，非空表示 STS：

```json
{"accessKeyId":"...","accessKeySecret":"...","stsToken":"..."}
```

使用 stdin 时，将挂载示例中的 `--credential-file ...` 替换为 `--credential-stdin`，由自己的凭证程序向挂载进程的 stdin 写入上述格式。凭证程序写完必须关闭管道；默认 30 秒内未收到完整输入则挂载失败，可用正数 duration 参数 `--credential-stdin-timeout` 调整。超时不会把已读取的部分凭证写入日志。

### 签名和传输

| 凭证类型 | 签名 | `--region` | 传输要求 |
|---|---|---|---|
| AK/SK、STS | 固定 OTS V4，不回退 V2 | 必填，必须匹配实例地域 | 默认 HTTPS |
| API Key | 不使用请求签名 | 可省略 | 必须 HTTPS |

仅支持 V2 签名的服务无法通过 AK/SK 或 STS 挂载。升级旧部署时，需要补齐 `--region` 并将非回环 HTTP endpoint 切换到 HTTPS。

本地 mock 可使用字面回环地址（`localhost`、`127.0.0.0/8`、`::1`）的 HTTP，判断不做 DNS 解析。非回环 HTTP 默认拒绝，仅隔离测试可显式使用 `--allow-insecure-http`。该开关不能放宽 API Key 的 HTTPS 要求；CLI 未提供 SDK 的 `AllowInsecureAPIKey` 开关。所有 HTTP 重定向均被拒绝。

### STS 在线轮转

使用 STS 挂载后，可在不卸载的情况下原子替换完整的 AK/SK/token 三元组。将新凭证 JSON 写入 `rotate-credential` 的 stdin，三个字段均不能为空：

```bash
your-credential-provider | \
  ./agent-memory-fuse rotate-credential --mountpoint "$HOME/mnt/memory"
```

`your-credential-provider` 是示意名称，需替换为自己的凭证获取程序，并输出上文的 JSON 格式。工具不内置凭证获取或自动续期；修改原凭证文件也不会自动刷新挂载。轮转继续使用挂载时的 V4 签名和地域配置，不适用于 AK/SK 或 API Key 挂载。

轮转通过挂载根目录的私有 xattr 发送一次性控制消息，不持久化该 xattr。成功仅表示凭证已在本地安装，不会请求服务端验证；无效或过期凭证可能在后续访问时返回 `EACCES`。控制通道假设能访问挂载点的进程均可信；执行轮转的容器需包含本工具的二进制。

## Scope 与挂载参数

`--scope` 格式为 `app#tenant#agent#run`，四段对应 AppID、TenantID、AgentID、RunID。各段两侧空白会被去除；任一段为空、为通配符 `*` 或包含 `/` 都会被拒绝。完全省略 `--scope` 时，四段均显式使用 `__default__`；显式传入空字符串不会使用默认值。

| 参数 | 默认值 | 作用 |
|---|---|---|
| `--metadata-ttl` | `5s` | 文件元数据缓存，同时设置内核 Entry/Attr timeout |
| `--negative-ttl` | `1s` | 不存在路径的缓存，同时设置内核 Negative timeout |
| `--directory-ttl` | `10s` | 目录完整快照的刷新间隔，由目录读取触发 |
| `--mutation-timeout` | `1m` | 单次远端新建、更新、重命名或删除的写操作时限 |
| `--allow-other` | 关闭 | 允许挂载用户之外的所有本机用户（含 root）访问挂载点 |

TTL 和 timeout 参数均要求正数 duration，例如 `--metadata-ttl 7s`。跨客户端修改的可见性受缓存影响，不应假设所有操作立即可见。

`--allow-other` 用于 Agent 进程与挂载进程不同用户的场景（如沙箱内多用户）。注意：非 root 挂载走 fusermount 时要求 `/etc/fuse.conf` 启用 `user_allow_other`，否则挂载失败；开启后数据隔离完全依赖容器/沙箱边界。

写操作时限从本地校验和路径锁获取完成后开始计算，不包含等待路径锁的时间。已发出的远端写不会因后续 FUSE 中断或父 context 取消而中止；写超时后仍可能额外执行一次受 HTTP timeout（CLI 默认 30 秒）约束的安全回查。因此 `--mutation-timeout` 不是整个文件系统调用的总耗时上限。

## 文件系统语义

### 读写与目录

- 挂载前通过完整分页 List 建立整个 Scope 的初始快照。目录是文件路径前缀，空目录只在本次挂载进程中存在。
- 打开文件会拉取全文；写入和截断先修改句柄缓冲区，`flush`/`fsync` 同步提交全文。`release` 只清理句柄，不提交或重试；应用应检查同步和关闭文件时的错误。
- 不同路径的读写可并行，同一路径的写入串行；重命名同时锁定源和目标路径。服务端版本前置条件防止并发写覆盖其他客户端的修改。
- 重命名或删除后，旧句柄提交返回 `ESTALE`，不会重建文件。尚未首次提交的新文件只在本地处理 unlink/rename：移动到空目标后首次提交在新路径创建，覆盖已有远端目标则返回 `EEXIST`。
- `statfs` 展示 2000 个文件的常规上限，空闲数不小于 0。本地新建预检为并发和缓存滞后留有 100 个余量，最多允许本地索引达到 2100；这不改变服务端配额，`QUOTA_EXCEEDED` 仍是权威结果。

### 缓存刷新

Lookup 和无句柄的文件 Getattr 在元数据缓存过期后只查询单个文件，同路径、同一变更代次的请求会合并；不存在路径的缓存最多 4096 项。

目录快照的常规刷新由 Readdir 在目录缓存过期后触发，并合并并发扫描。如果扫描期间发生变更，旧快照不会覆盖新状态，而会调度一个合并的后台任务，最多再尝试两次完整分页 List。

### 常见错误

| 错误 | 含义 |
|---|---|
| `ESTALE` | 句柄或资源版本已过期，例如版本冲突对应的 `PRECONDITION_FAILED` |
| `EAGAIN` | 可重试的暂时失败，例如回查后仍未发生版本变化的 `LOCK_CONFLICT` |
| `EEXIST` | 目标路径已存在（`PATH_EXISTS`），或未提交的新文件试图覆盖远端目标 |
| `EFBIG` / `EILSEQ` | 内容超过 100 KiB / 不是合法 UTF-8 |
| `ENOSPC` | 本地创建预检超限，或服务端返回 `QUOTA_EXCEEDED` |
| `EACCES` | 服务端返回 401/403，包括凭证失效或权限不足 |
| `EROFS` | 只读挂载或服务端只读 Store |

## Item API 与失败恢复

客户端通过 Go SDK 调用以下 RPC 端点，无 `/fs` 前缀：

| 操作 | 端点 | 请求要点 |
|---|---|---|
| 列目录 | `/ListItems` | 递归分页，每页 100 条，持续跟随 `nextToken` |
| 查询元数据、读取 | `/GetItem` | 元数据使用默认 `view=basic`，内容使用 `view=full` |
| 新建 | `/AddItem` | 路径、全文内容和 `view=full` |
| 更新、重命名 | `/UpdateItem` | 稳定 `itemId` 和 `precondition.expectedVersionId`；更新传内容，重命名传目标 `path` 和 `overwrite` |
| 删除 | `/DeleteItem` | `itemId` 和版本前置条件 |

公共请求字段为 `type="memoryfile"`、`memoryStoreName` 和四段 `scope`；写请求携带 `sessionId` 用于版本溯源。挂载内使用相对路径，发往服务端时补前导 `/`；非空 List 前缀按路径段对齐，例如 `notes` 转为 `/notes/`。同内容更新、同路径重命名可以是 no-op，资源版本不一定推进。

### 重试与回查

1. **读操作**：对 429/500/503/504 和精确的 409 `LOCK_CONFLICT` 最多重试两次，默认退避为 100ms、500ms。
2. **写操作**：只自动重试明确未执行的 429。锁冲突、5xx 服务端错误、传输失败或不可信响应不自动重放写，而使用写开始时的同一代凭证进行一次逻辑回查；回查本身可按安全读预算重试。429 重试耗尽不回查。
3. **结果确认**：更新内容或重命名路径已与目标匹配时确认成功，不要求版本推进；未达到目标时，仅精确锁冲突且版本未变才保留原冲突，其他情况返回 `PRECONDITION_FAILED`。删除目标已不存在则确认成功；仍存在且版本未变则保留原错误，版本已变则返回 `PRECONDITION_FAILED`。未取得有效回查证据时保留原错误和 requestId。
4. **新建归属**：只有异常成功响应提供的候选 itemId、目标路径和内容均与权威回查一致时，才可确认创建成功。仅同路径同内容不足以证明当前客户端创建成功；缺少归属证据时保留原错误，候选 itemId 或内容不匹配则返回 `PATH_EXISTS`。未提交句柄可显式重试，后续独立创建可能得到 `EEXIST`。
5. **取消与超时**：调用前已失效或主动取消的 context 不发起或继续回查；已准入写操作自身 deadline 到期时，可用新的、有 HTTP 超时约束的 context 仅做安全 Get，不重放写。

## 错误日志与退出码

mount 失败和运行期远端操作失败共用一种两行式输出：第一行是带时间戳的固定前缀（人读），第二行以 `<事件名> json: ` 开头、其后为纯 JSON。采集端按事件名分发，剥掉前缀即可解析；`event` 字段（`mount_error` / `mutation_error`）是事件判别器。

```text
initial directory load failed
mount_error json: {"event":"mount_error","operation":"mount","class":"config","reason":"store_not_found","exit_code":4,"advice":"...","http_status":404,"service_code":"OTSObjectNotExist","request_id":"00065c0c-..."}

file memory mutation error
mutation_error json: {"event":"mutation_error","operation":"read","class":"auth","reason":"auth_rejected","path_sha256":"ab3f...","category":"service","errno":13,"http_status":403,"service_code":"OTSAuthFailed","request_id":"00065...","message":"service request failed"}
```

`class` 是统一处置分类，mount 失败与运行期日志由同一个来源推出，不会漂移：

| class | 含义 | mount 退出码 | 运行期对应 errno |
|---|---|---|---|
| `retryable` | 瞬时失败（限流/5xx/网络/超时/锁冲突），重试有意义 | 2 | `EAGAIN` |
| `auth` | 凭证被拒/过期/RAM 拒绝，重试无用 | 3 | `EACCES` |
| `config` | 参数/契约/TLS/store 不存在/只读库，改配置 | 4 | `EINVAL`、`EROFS` |
| `rejected` | 确定性业务拒绝（超限/编码/配额/冲突/已存在），盲目重试无意义 | 4 | `EFBIG`、`EILSEQ`、`ENOSPC`、`ESTALE`、`EEXIST` |
| `unknown` | 未分类 | 1 | `EIO` |

mount 失败按 class 对应退出码退出，供 systemd 或容器 supervisor 决策：2 可重启重试；3/4 重启无用，应告警并检查凭证或配置；1 人工排查。mount 事件独有 `exit_code` 和 `advice` 字段；运行期事件独有 `errno`、`path_sha256`（路径只输出哈希）、`category` 和 `sdk_kind`。`reason` 为固定枚举；仅 mount 的 `unknown` 类附带原始错误 `message`。`403 OTSValidationFail` 归为 `config`（参数/契约校验失败）而非 `auth`。

## 运行限制

已有的 24 小时压力测试无业务失败，结束后真实环境用例仍通过；但末段 RSS 仍观察到约 `+370.9 KiB/hour`，尚未形成平台期。该数据是特定测试环境的观测值，不是内存增长上限或性能承诺。长期内存稳定性仍是生产发布的阻塞项。

- 监控进程 RSS、FD、线程数和挂载状态，在内存达到环境阈值前受控卸载、重启和重新挂载。
- 出现持续加速增长、OOM、挂载丢失、数据错误或清理失败时，停止新挂载和扩量，受控卸载后回退到上一个已验证制品，重新挂载并验证数据可见性。
- FUSE 不拥有远端数据生命周期，回滚二进制不得删除 Store、Scope 或远端 Item。

## 开发与测试

单元测试和静态检查无需真实服务凭证：

```bash
go test -mod=vendor ./...
GOMAXPROCS=2 go test -mod=vendor -race -p=1 ./... -count=1
go vet -mod=vendor ./...
```

挂载级测试需要 Linux FUSE 环境。真实服务测试会写入和删除测试路径，必须使用新建、独占的临时 Store 和 Scope；不要针对业务数据运行。不同服务版本或地域应先在目标环境验证。

| 工具 | 说明 |
|---|---|
| [E2E 套件](test/e2e/README.md) | 本地签名 mock、真实服务挂载、语义探针与数据准备工具 |
| [fusesoak](test/soak/fusesoak/README.md) | 混合操作长时间稳定性测试 |

## 发布构建

在干净的 Git 工作区中执行：

```bash
./scripts/build-release.sh
./dist/agent-memory-fuse-linux-amd64 version
```

脚本固定 Go 1.26.6、Linux/amd64、vendored 依赖和 `-trimpath`，校验 SDK commit、Go module 版本和 `SDK_VENDOR_SHA256`，输出二进制及同目录的 `.sha256` 校验和文件。`version` 显示注入的版本、Git commit 和源码时间戳；普通 `go build` 的这些字段为 `dev` / `unknown`。

SDK 快照的精确提交记录在 `SDK_COMMIT`。维护依赖时，需将同级 `../tablestore-agent-storage-sdk` checkout 切到该提交并保持干净，确认 `go.mod` 版本匹配，再运行 `./scripts/update-sdk-vendor.sh` 更新 vendor 和内容摘要。`go.mod` 的本地 `replace` 用于这一维护流程，普通构建继续使用 `-mod=vendor`。

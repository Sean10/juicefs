# JuiceFS 访问日志 + 文件清单采集到 Kafka（Vector 方案）

适用版本：JuiceFS 社区版 **1.3.x / 1.4.x**（两者访问日志格式完全一致，一套方案通用），
覆盖两种主要部署形态：**物理机 FUSE 挂载**、**Kubernetes CSI（Mount Pod 模式）**。

```text
                      ┌──────────── 每个节点 ────────────┐
 应用进程/Pod ──FUSE──▶ juicefs mount / CSI Mount Pod   │
                      │   <挂载点>/.accesslog (虚拟文件)   │
                      │          │ 每个挂载点一个读线程     │
                      │  juicefs_accesslog_reader.py      │  发现挂载点、pid→进程/Pod、卷名
                      │          │ JSON 行（物理机: stdout │
                      │          │  / k8s: unix socket）    │
                      │  Vector: VRL 解析 → 过滤           │
                      │          → read/write 按 fh 聚合   │──▶ Kafka  juicefs-accesslog
                      └──────────────────────────────────┘          juicefs-accesslog-dlq（解析失败）

 每周清单（二选一）:
   单机   juicefs dump → juicefs-inventory.py                     → 文件 + Kafka juicefs-inventory
   分布式 N × jfs-inventory（共享 Redis 目录队列，直接读元数据引擎） → 每个 worker 一个分片
 分析: 访问日志 (volume, inode) ⋈ 清单 (volume, inode) → 路径级热点 / 冷数据 / 谁在访问
```

## 目录

| 路径 | 说明 |
| --- | --- |
| `reader/juicefs_accesslog_reader.py` | 读取端（Python 3.6+，仅标准库）：发现挂载点、读取 `.accesslog`、补充进程名/Pod/PVC/卷名，输出 JSON 行 |
| `reader/test_juicefs_accesslog_reader.py` | 读取端单元测试（`python3 -m unittest`） |
| `vector/parse_accesslog.vrl` | VRL 解析器（覆盖全部 op 的结构化字段） |
| `vector/vector.yaml` | Vector 管道：解析 → 过滤 → 聚合 → Kafka（含 DLQ、磁盘缓冲、Prometheus 指标） |
| `vector/source-exec.yaml` / `vector/source-socket.yaml` | 输入：物理机由 Vector 拉起读取端（exec）/ k8s 读取端 sidecar 经 unix socket 推送 |
| `vector/vector-tests.yaml` | Vector 单元测试（`vector test`） |
| `baremetal/` | 物理机：Vector 环境变量、systemd override、清单 cron |
| `k8s/vector-daemonset.yaml` | CSI 场景：读取端 + Vector 的 DaemonSet |
| `k8s/csi-mountpod-hostpid.yaml` | CSI 全局配置：Mount Pod 开启 hostPID（Pod 级归属所需） |
| `k8s/inventory-cronjob.yaml` | CSI 场景：单机清单 CronJob（dump + Python，无需构建） |
| `k8s/inventory-distributed.yaml` | CSI 场景：分布式清单（Indexed Job + 协调用 Redis） |
| `k8s/lab-juicefs-sc-pvc.yaml` | 实验用 StorageClass/PVC/读写负载 |
| `kustomization.yaml` | `kubectl apply -k .` 部署 DaemonSet + CronJob（自动生成 ConfigMap） |
| `inventory/juicefs-inventory.{py,sh}` | 单机清单：`juicefs dump` JSON → 扁平记录（ndjson/csv） |
| `inventory/jfs-inventory/` | 分布式清单 worker（Go，复用 JuiceFS `pkg/meta`，支持所有元数据引擎）+ Dockerfile + 测试 |
| `tools/gen-workload.sh` / `tools/analyze.py` | 构造负载 / 日志与清单关联分析示例 |
| `kafka/server.properties` | 实验用单节点 KRaft Kafka（数据 5 分钟过期） |

## 1. 关键结论（基于源码和实测）

1. **1.3.0 与 1.4.0 无差异**：`pkg/vfs` 中所有 `logit(...)` 格式串在 `v1.3.0` 与 `v1.4.0` 完全相同，
   `pkg/vfs/accesslog.go` 无改动；实测两个版本同时挂载，同一解析器 0 条解析失败。
2. **Linux FUSE 挂载没有 `--access-log` 参数**（两个版本都只有 gateway / webdav / Windows 支持写文件）。
   物理机和 CSI 都只能读 **`<挂载点>/.accesslog`** 这个虚拟文件。
3. 访问日志的行为（`pkg/vfs/accesslog.go`），直接决定了采集设计：
   - **只有在有人打开 `.accesslog` 时才产生日志**；没有读者时零开销，Vector 停了日志就丢了（不会积压）。
   - 每个读者有 **10240 行的内存缓冲，读得慢就静默丢行**，所以读取端必须不阻塞
     （本方案 Kafka sink 使用磁盘缓冲 + `drop_newest`，不会反压到读取端）。
   - 空闲时每秒返回一行 `#` 心跳（读取端已过滤）。
   - 时间戳是 **juicefs 进程所在时区的本地时间，不带时区**（CSI Mount Pod 默认为 UTC），用 `JUICEFS_TZ` 指定。
   - `read`/`write` 等只有 **inode，没有路径**；`lookup/create/mkdir/unlink/rename` 带 `父 inode + 名字`。
     所以要做路径级分析必须配合文件清单（第 5 节）。
   - `pid` 是调用者在 **juicefs 进程 PID 命名空间** 中的 pid。CSI Mount Pod 默认不是 hostPID，
     **应用 Pod 的请求全部记为 `pid:0`**（实测），需要开启 hostPID 才能归属到 Pod（第 4 节）。
   - 读者打开的 `.accesslog` 会让挂载点处于 busy：手工 `umount /jfs` 会报 `target is busy`，
     需先停 Vector 或用 `juicefs umount --force`/`umount -l`。juicefs 进程收到 SIGTERM 时自己做 lazy umount，
     读取脚本在挂载点消失后 ≤10s 关闭句柄，实测 juicefs 进程 6s 内正常退出、CSI Mount Pod 回收不受影响。
4. **开销**（4 vCPU 虚拟机，本地盘作为对象存储，单客户端 ~2.5GiB/s 的 CPU 瓶颈场景，`juicefs bench -p 4`）：

   | 场景 | 大文件写 | 大文件读 | 小文件写 | 小文件读 | stat |
   | --- | --- | --- | --- | --- | --- |
   | 无读者 | 2595 MiB/s | 1989 MiB/s | 1755 /s | 3321 /s | 27822 /s |
   | 仅 `cat .accesslog > /dev/null` | 2014 MiB/s | 2063 MiB/s | 1544 /s | 2719 /s | 30297 /s |
   | 完整管道（读取端 + Vector，同机抢 CPU，多次测量） | 1353~1585 MiB/s | 1251~1660 MiB/s | 1285~1488 /s | 2778~3139 /s | 25113~29172 /s |

   大文件吞吐的下降主要来自 juicefs 为每个 128KiB 请求格式化日志，以及 Vector 和 juicefs 抢 4 个核；
   元数据/小文件操作差异在噪声范围内。生产环境对象存储一般是网络瓶颈，影响会小很多，但建议在目标机型上复测，
   并给 Vector 设置 CPU 限制。同一时段 juicefs 自身 `fuse_ops_total` 增加 159,747，读取端送出 160,084 条
   （含内部操作），**未观察到丢行**。

## 2. Kafka 消息格式

每条消息是一个 JSON 对象，key 为 `inode`。read/write 默认按 **(挂载点, op, inode, fh, pid, uid)** 聚合
（5s 无新请求或文件关闭后输出），一次顺序读写大文件从上百条变成 1 条；其它操作逐条输出。

| 字段 | 说明 |
| --- | --- |
| `timestamp` | 操作时间（聚合记录为最后一次请求时间），RFC3339 UTC |
| `ts_ingest` | Vector 收到的时间 |
| `cluster` / `hostname` | `JFS_CLUSTER` / 节点名 |
| `mountpoint` / `volume` / `subdir` | 挂载点、卷名、挂载子目录（从 `<挂载点>/.config` 读取） |
| `pv` / `pvc` | CSI：PV 名（来自挂载路径）与 `namespace/pvc`（sidecar 映射） |
| `pid` / `comm` / `pod_uid` / `pod_namespace` / `pod_name` | 调用者进程（尽力解析，见第 4 节） |
| `uid` / `gid` | 调用者用户 |
| `op` / `ok` / `status` / `duration_us` | 操作、是否成功、`OK` 或 errno 文本、耗时（微秒，聚合记录为总和） |
| `inode` / `fh` | 目标 inode / 文件句柄 |
| `parent_inode` / `name` | lookup/create/mkdir/mknod/symlink/link/unlink/rmdir/rename |
| `new_parent_inode` / `new_name` | rename |
| `bytes` / `offset` / `count` | read/write/copy_file_range：字节数、起始偏移、合并的请求数 |
| `mode` / `symlink_target` / `file_size` / `open_flags` | 按 op 解析出的属性 |
| `args` / `result` | 原始参数与返回值（非聚合记录） |
| `aggregated` | 是否为聚合记录 |
| `raw` | 原始日志行（`JFS_KEEP_RAW=true` 时保留） |

示例（实测输出）：

```json
{"aggregated":false,"args":"923116,报告,v1.txt,-rw-r--r--:0100644","cluster":"lab-baremetal","count":1,"duration_us":175,"fh":131254,"file_size":0,"gid":0,"hostname":"cursor","inode":923120,"mode":"0100644","mountpoint":"/jfs","name":"报告,v1.txt","ok":true,"op":"create","parent_inode":923116,"pid":36971,"result":"(923120,[-rw-r--r--:0100644,1,0,0,1792682577,1792682577,1792682577,0])","status":"OK","timestamp":"2026-10-22T15:22:57.858289Z","ts_ingest":"2026-10-22T15:22:58.233813571Z","uid":0,"volume":"labfs"}
{"aggregated":true,"bytes":8388608,"cluster":"lab-baremetal","count":64,"duration_us":6050,"fh":240,"gid":0,"hostname":"cursor","inode":53,"mountpoint":"/jfs","offset":0,"ok":true,"op":"write","pid":8382,"status":"OK","timestamp":"2026-10-22T14:51:42.669803Z","uid":0,"volume":"labfs"}
{"aggregated":true,"bytes":53,"cluster":"lab-k3s","comm":"sh","count":1,"duration_us":25,"fh":10294,"inode":4097,"mountpoint":"/var/lib/juicefs/volume/pvc-b7d046b5-746b-449f-a149-ef6a01551476-nmfeem","offset":16430,"ok":true,"op":"write","pid":21903,"pod_name":"reader-7b6c7cbf8c-9m2kn","pod_namespace":"default","pod_uid":"e14b07e0-5d9b-4be3-aa8b-7f8fe916a0a7","pv":"pvc-b7d046b5-746b-449f-a149-ef6a01551476","pvc":"default/data-b","status":"OK","subdir":"pvc-b7d046b5-746b-449f-a149-ef6a01551476","timestamp":"2026-10-22T15:22:56.458136Z","uid":0,"volume":"k8sfs"}
```

解析失败的行原样进入 `<topic>-dlq`（含 `metadata.dropped` 原因）。调整需求时常见的改法：

- 不想要 read/write 聚合：在 `vector.yaml` 中把 `jfs_accesslog_route.io` 直接接到 `jfs_accesslog_final`。
- 量太大：在 `jfs_accesslog_filtered` 的条件里去掉 `flush`/`release`/`releasedir`/`opendir`/`getattr` 等噪声操作
  （实测负载中 `flush`+`release` 约占三分之一）。

## 3. 物理机部署

```bash
# 1) 安装 Vector（官方 deb/rpm 或 tar 包）；读取端需要 python3 >= 3.6（只用标准库）
# 2) 部署配置
sudo mkdir -p /etc/vector /var/lib/vector
sudo cp vector/{vector.yaml,source-exec.yaml,parse_accesslog.vrl} reader/juicefs_accesslog_reader.py /etc/vector/
sudo cp baremetal/vector.env /etc/default/vector          # 修改 KAFKA_BOOTSTRAP / JUICEFS_TZ / MOUNT_PREFIXES
sudo mkdir -p /etc/systemd/system/vector.service.d
sudo cp baremetal/vector-service-override.conf /etc/systemd/system/vector.service.d/override.conf  # 以 root 运行
sudo systemctl daemon-reload && sudo systemctl enable --now vector

# 3) 校验
(cd reader && python3 -m unittest -v test_juicefs_accesslog_reader)
JFS_VECTOR_DIR=/etc/vector KAFKA_BOOTSTRAP=localhost:9092 vector test /etc/vector/vector.yaml /etc/vector/source-exec.yaml vector/vector-tests.yaml
curl -s localhost:9598/metrics | grep jfs_accesslog              # 各组件收发计数
tail /var/log/juicefs-accesslog-reader.log                       # 读取端日志，每分钟一行 lines/sent/dropped 统计
kcat -b <kafka> -t juicefs-accesslog -C -o end
```

说明：

- `vector.env` 里的 `VECTOR_CONFIG=/etc/vector/vector.yaml,/etc/vector/source-exec.yaml` 让打包的 `vector.service` 同时加载两个文件。
- 读取 `.accesslog` 需要 root（文件属主为 juicefs 进程用户，权限 0400），所以 Vector（以及它拉起的读取端）用 root 运行。
- 新挂载的卷、重新挂载（升级/重启）都会在 `RESCAN_INTERVAL`（默认 10s）内自动接上；同一挂载被 bind 到多个路径时按设备号去重，避免重复。
- 读取端绝不阻塞 JuiceFS：输出慢或断开时丢弃并计数（`dropped`），而不是让 JuiceFS 侧 10240 行缓冲溢出。
  读取端只在输出连上之后才打开 `.accesslog`，Vector 不在时 JuiceFS 不会产生日志开销。
- `comm`（进程名）是在日志到达时从 `/proc/<pid>/comm` 读取的，**短命进程（`dd`、`ls`、`cat` 等）多数已退出，解析不到**；长驻服务进程可以解析。
- 手工排查：`sudo python3 /etc/vector/juicefs_accesslog_reader.py --kube off | head`。

### 3.1 为什么读取端用 Python

读取端负责需要主机权限的部分（挂载点发现、读虚拟文件、`/proc` 和 cgroup、k8s API），解析/聚合/投递留给 Vector：

- 只用 Python 标准库（3.6+），物理机上直接运行，k8s 用官方 `python:3.12-slim` 镜像 + ConfigMap，**不需要自己构建镜像**；
- 有 13 个单元测试（mountinfo、两种 cgroup 驱动、`.config`、跨 read 的行拼接、进程/Pod 解析、socket 重连、端到端 `main()`）；
- 性能：单核约 27 万行/s，`juicefs bench` 时约 3 万行/s 的峰值只占约 10% CPU；同期 JuiceFS 统计 159,747 次操作，读取端送出 160,084 条（含内部操作），无丢行。

如果想要单一静态二进制、更低开销，可以用 Go 重写同样的逻辑（`inventory/jfs-inventory` 已经示范了在本仓库内构建独立工具），代价是需要构建和分发镜像。

## 4. Kubernetes（JuiceFS CSI）部署

### 4.1 CSI 下挂载点在哪里（实测 CSI v0.33.1）

- 每个 PV 一个 Mount Pod，命令为 `mount.juicefs META /jfs/<pv>-<随机6位> -o subdir=<pv>`，
  通过 Bidirectional 传播出现在宿主机 **`/var/lib/juicefs/volume/<pv>-<随机>`**，根目录下就有 `.accesslog`。
- kubelet 给应用 Pod 的目录 `/var/lib/kubelet/pods/<uid>/volumes/kubernetes.io~csi/<pv>/mount` 是同一挂载的 bind（同设备号），**也能看到 `.accesslog`**。
- 默认情况下应用 Pod 的请求在日志里都是 `pid:0`。

### 4.2 可选采集方式对比

| 方式 | 结论 |
| --- | --- |
| **A. Vector DaemonSet 读 `/var/lib/juicefs/volume/*/.accesslog`（推荐）** | `hostPath` + `mountPropagation: HostToContainer`，不改 Mount Pod 和应用；随 Mount Pod 创建/销毁自动接入/退出。已在 k3s 实测 |
| B. 宿主机 systemd Vector 读同一目录 | 和物理机配置相同，只需 `MOUNT_PREFIXES=/var/lib/juicefs/volume/`；已实测可用，并能从 cgroup 解析 pod_uid，但拿不到 Pod 名/PVC 名（没有 API 访问） |
| C. 宿主机读 kubelet bind 目录 | 可行但不推荐：路径跟随应用 Pod 生命周期变化，一个 PV 被多个 Pod 使用时会有多份相同的 bind，重复打开就会重复采集（每个读者各得一份全量） |
| D. Mount Pod / 应用 Pod 里加 sidecar | 不推荐：CSI 的 Mount Pod 由 CSI 控制器生成，改造成本高；每个应用都要改 |

### 4.3 部署（方式 A）

```bash
# 1)（可选，需要 Pod 级归属时）Mount Pod 开启 hostPID。注意这会覆盖 CSI 全局配置，先合并已有配置。
#    只对之后新建的 Mount Pod 生效（实测：应用缩容到 0 → Mount Pod 回收 → 扩容后 hostPID=true）。
kubectl apply -f k8s/csi-mountpod-hostpid.yaml

# 2) 修改 k8s/vector-daemonset.yaml 中的 KAFKA_BOOTSTRAP / JFS_CLUSTER 后部署
kubectl apply -k .
kubectl -n juicefs-logging get pods
```

DaemonSet 里有两个容器，都用官方镜像，代码来自 kustomize 生成的 ConfigMap：

- `reader`：`python:3.12-slim` 运行 `juicefs_accesslog_reader.py`。挂载 `/var/lib/juicefs/volume`（HostToContainer）和宿主机 `/proc`（只读，pid → cgroup → pod_uid），
  用 ServiceAccount 直接调 API（每 30s 拉本节点 Pod 和 PV → PVC 映射）补充 `pod_namespace`/`pod_name`/`pvc`，
  通过共享 emptyDir 里的 unix socket 把 JSON 行推给 Vector。不需要 privileged。
- `vector`：`timberio/vector:0.50.0-debian`，`socket` source 监听该 unix socket，磁盘缓冲在 `/var/lib/vector-juicefs`。

两者生命周期解耦：实测 Vector 进程被杀重启后，读取端约 1.2s 自动重连，期间的记录计入 `dropped`（日志里每分钟一行统计）。

归属能力（实测）：

| 维度 | 是否可得 |
| --- | --- |
| PV / PVC / 卷 / 子目录 | 始终可得 |
| Pod（`pod_uid`/`pod_name`） | 需要 hostPID；**长驻进程**可得（如应用主进程），短命进程（`cat`、`dd`）多数已退出解析不到；新建 Pod 最多约 60s 内名字为空（只有 uid） |
| `release`/`releasedir` 等内核发起的操作 | 内核本身就记为 pid 0 |

如果某个 PV 只被一个工作负载使用（最常见），按 `pvc` 归属已经足够；需要精确到 Pod 时再开 hostPID。

## 5. 文件清单（inventory）

两种实现，输出相同的记录格式（逐字段对比一致）：

`volume, snapshot, inode, parent_inode, path, name, depth, type, size, mode, uid, gid, nlink, atime, mtime, ctime, symlink`

| | 单机：`juicefs dump` + `juicefs-inventory.py` | 分布式：`jfs-inventory`（Go） |
| --- | --- | --- |
| 原理 | 官方 dump 出 JSON，Python 流式转换 | 直接用 JuiceFS `pkg/meta` 遍历目录树（`Readdir` 带属性），多 worker 共享 Redis 目录队列 |
| 引擎 | 全部 | 全部（Redis / SQL / TiKV / etcd / FDB ...） |
| 部署 | 无需构建（官方 mount 镜像自带 juicefs + python3） | 需要构建二进制/镜像（提供 Dockerfile） |
| 4.4M 条目实测 | 81.7s（5.4 万条/s，瓶颈是 Python 转换） | 单 worker 9.4s；4 worker 8.5s（52 万条/s） |
| 适用 | 千万级以内 | 亿级以上，或可扩展的元数据引擎 |

### 5.1 分布式设计

类似 RGW 按 bucket index shard 并行列举，但 JuiceFS 的元数据是目录树而非扁平索引，按 inode 区间静态切分需要每种引擎各写一套扫描，
还得事后用目录表拼路径。这里用**目录粒度的工作窃取**，与引擎无关、天然处理倾斜：

1. 第一个 worker（`SETNX`）把根目录放进队列（Redis list），`pending=1`；
2. 每个 worker 的多个协程循环：`BLPOP` 取一个目录 → `Readdir`（带属性）→ 每个条目写一条记录到本地分片 →
   先 `INCRBY pending` 再 `LPUSH` 子目录 → 当前目录 `pending-1`；
3. 队列空且 `pending == 0` 时整棵树完成，所有 worker 退出；各分片互不重叠，并集就是完整清单；
4. `-wait-workers N`：worker 先报到并等待 N 个到齐（或超时）再开始，避免 Pod 先后启动导致第一个 worker 包揽全部；
5. 有 worker 中途死掉时，它持有的目录永远完成不了，其余 worker 在 `-stall-timeout`（默认 10 分钟）内无进展就失败退出，整体用新 job id 重跑（周级批处理，不做断点续跑）。

实测（4.4M 条目，Redis 元数据，单台 4 vCPU，所有 worker 和 Redis 在同一台机器上）：

| 方式 | 耗时 | 条/s | 元数据 Redis CPU |
| --- | --- | --- | --- |
| `juicefs dump` + Python | 81.7s | 54k | 23% |
| `juicefs dump` 单独 | 21.7s | 205k | 76% |
| jfs-inventory 1 worker × 1 协程 | 17.9s | 249k | 33% |
| 2 worker × 1 协程 | 10.6s | 421k | 52% |
| 4 worker × 1 协程 | 9.2s | 485k | 59% |
| 1 worker × 16 协程 | 9.4s | 471k | 57% |
| 4 worker × 4 协程 | 8.5s | 526k | 64% |
| k3s Indexed Job 4 Pod × 8 协程 | 每个 ~7.6s | 分片 99 万 / 106 万 / 107 万 / 132 万 | |

结论：

- **瓶颈在元数据引擎**。单个 Redis 是单线程，按上面的数据每条记录约 1.2µs Redis CPU，上限约 80 万条/s，
  1 个 jfs-inventory worker 已经能用到一半以上；对 Redis 来说 2~3 个 worker 就到顶（10 亿条目约 20~30 分钟），**分布式的意义主要在 TiKV / FoundationDB 这类可横向扩展的引擎**，或者单台客户端 CPU/网络成为瓶颈时。
- 在 Redis 上，不分布式只换成 jfs-inventory 单机，也比 dump + Python 快 5~9 倍。
- 对 Redis 元数据，建议对**从库**执行（`-meta` 指向 replica），避免影响线上。
- 已知限制：单个目录的条目由一个协程一次读完（与 `juicefs dump` 相同），千万级单目录会占较多内存；清单不是时间点快照（与 dump 相同）。

### 5.2 物理机

```bash
# 单机（无需构建）
sudo mkdir -p /opt/juicefs-accesslog-kafka && sudo cp -r inventory /opt/juicefs-accesslog-kafka/
sudo cp baremetal/juicefs-inventory.cron /etc/cron.d/juicefs-inventory   # 修改 META_URL / VOLUME / KAFKA
sudo META_URL=redis://127.0.0.1:6379/1 VOLUME=labfs KAFKA_BOOTSTRAP=172.30.0.2:9092 \
  /opt/juicefs-accesslog-kafka/inventory/juicefs-inventory.sh

# jfs-inventory：在仓库根目录构建（与 juicefs 相同的工具链），单机或多机
go build -ldflags "-s -w" -o jfs-inventory ./deploy/accesslog-kafka/inventory/jfs-inventory
jfs-inventory -meta redis://10.0.0.1:6379/1 -threads 16 -o inv.ndjson.gz
# 多台机器各执行一次（同一个 -job，-wait-workers 为机器数），每台得到一个分片
jfs-inventory -meta tikv://pd:2379/jfs -queue redis://coord:6379/0 -job 20261025 -wait-workers 3 \
  -worker $(hostname) -o part-$(hostname).ndjson.gz
```

单机脚本输出 `$OUT_DIR/<volume>/inventory-<UTC时间>.ndjson.gz`（默认保留 35 天），设置 `KAFKA_BOOTSTRAP` 时用 `kcat` 逐条写入 `juicefs-inventory` topic；
jfs-inventory 的分片同样可以 `gzip -dc part.ndjson.gz | kcat -P -b <kafka> -t juicefs-inventory`。

### 5.3 Kubernetes

```bash
kubectl -n juicefs-logging create secret generic juicefs-inventory \
  --from-literal=metaurl=redis://172.30.0.2:6379/3 --from-literal=volume=k8sfs
kubectl apply -k .     # 两个 CronJob 都会创建，按需删掉一个
kubectl -n juicefs-logging create job --from=cronjob/juicefs-inventory inv-manual               # 单机
kubectl -n juicefs-logging create job --from=cronjob/juicefs-inventory-distributed inv-dist     # 分布式
```

- 单机（`k8s/inventory-cronjob.yaml`）：init 容器用 `juicedata/mount:ce-v1.4.0`（自带 juicefs 和 python3）做 dump + 转换，主容器用 `edenhill/kcat` 写入 Kafka。
- 分布式（`k8s/inventory-distributed.yaml`）：一个无持久化的 `redis:7-alpine` 作为目录队列；CronJob 生成 Indexed Job（`completions = parallelism = WAIT_WORKERS = 4`），
  `JOB_ID` 取自 Pod 标签 `batch.kubernetes.io/job-name`（同一次运行相同、每次运行不同），Pod 之间软反亲和分散到不同节点；每个 Pod 的分片由 kcat 容器写入 Kafka。
  实验环境用 hostPath 挂载节点上构建好的二进制，生产环境用 `inventory/jfs-inventory/Dockerfile` 构建镜像替换（注释已标明）。
  注意每个 Pod 申请 1 CPU：节点资源不足时部分 Pod 会 Pending，先启动的 worker 等满 `-wait-timeout`（2 分钟）后照常完成（实测会退化为单 worker，结果仍然完整）。
- 默认每周日 02:00（Asia/Shanghai）执行。CSI 动态 PV 在卷里是 `/<pv名>/...` 子目录，清单路径的第一级就是 PV 名，可以直接和访问日志的 `pv` 字段对应。

### 5.4 生产建议

清单是周级全量快照，量大时更适合**落对象存储/数仓**（Hive/Iceberg/ClickHouse），Kafka topic 只做传输；
ndjson.gz 分片可以直接并行导入。

## 6. 日志与清单关联分析

关联键是 **`(volume, inode)`**：

1. read/write 等事件 → 查清单得到路径；
2. 清单之后新建的文件 → 用日志里的 `create/mkdir/lookup`（`parent_inode + name → inode`）递归补路径；
3. CSI 卷以子目录挂载，日志中 `parent_inode=1` 指的是 `subdir` 根目录。

`tools/analyze.py` 是一个可运行的参考实现（全量加载到内存，仅用于演示逻辑），在 k3s 实验数据上的输出（节选）：

```text
== 1539 events, 241 inventory records, 0 read/write events with unresolved path
== ops: requests, average latency, p99 latency (only for non-aggregated records)
  flush                 330   avg    188.8us  p99   1937us
  read                  275   avg    119.0us  p99      -
  ...
== directories (depth 2): read / write
     10.0MiB    10.0MiB  /pvc-9f956028-06fe-419b-b612-f0020ac03fb8/day-0
== who (pvc/mount, pod/process): read / write / requests
          0B     1.0KiB       20  default/data-b reader-7b6c7cbf8c-zkzvx
== cold data: 230 of 236 inventoried files (230.0MiB) not accessed in this log window
```

数仓中的等价 SQL（以 ClickHouse 为例，表名示意）：

```sql
-- 最近 7 天按目录统计读流量，并给出冷数据占比
WITH acc AS (
  SELECT volume, inode, sum(bytes) AS read_bytes
  FROM juicefs_accesslog
  WHERE op = 'read' AND timestamp > now() - INTERVAL 7 DAY
  GROUP BY volume, inode
)
SELECT arrayStringConcat(arraySlice(splitByChar('/', i.path), 1, 3), '/') AS dir,
       count() AS files, sum(i.size) AS bytes,
       sum(acc.read_bytes) AS read_bytes_7d,
       countIf(acc.inode = 0) / count() AS cold_ratio
FROM juicefs_inventory AS i
LEFT JOIN acc ON acc.volume = i.volume AND acc.inode = i.inode
WHERE i.snapshot = (SELECT max(snapshot) FROM juicefs_inventory) AND i.type = 'regular'
GROUP BY dir ORDER BY bytes DESC LIMIT 50;
```

## 7. 实验环境复现

本目录的所有内容都在以下环境中跑通（单机）：

| 组件 | 版本 / 配置 |
| --- | --- |
| JuiceFS | 1.4.0（`/jfs`，Redis db1）+ 1.3.0（`/jfs13`，Redis db2）同时挂载，`--storage file` 本地盘 |
| Redis | 7.0，`redis://172.30.0.2:6379` |
| Kafka | 4.2.2 KRaft 单节点，`kafka/server.properties`，`retention.ms=300000`（5 分钟过期，无消费者） |
| Vector | 0.50.0（物理机 tar 包；k8s `timberio/vector:0.50.0-debian`） |
| k3s | v1.36，`--flannel-backend host-gw` |
| JuiceFS CSI | v0.33.1，Mount Pod 镜像 `juicedata/mount:ce-v1.4.0`，2 个 PVC，3 个读写 Pod |
| 清单测试数据 | `juicefs mdtest` 生成 89 万 / 445 万条目的卷（Redis db5 / db6）；协调队列 Redis 单独跑在 6380 端口 |

```bash
# Kafka（KRaft，单节点）
sed 's/${KAFKA_ADVERTISED_HOST}/<本机IP>/' kafka/server.properties > /opt/kafka.properties
bin/kafka-storage.sh format --standalone -t $(bin/kafka-storage.sh random-uuid) -c /opt/kafka.properties
bin/kafka-server-start.sh /opt/kafka.properties
bin/kafka-topics.sh --bootstrap-server <IP>:9092 --create --topic juicefs-accesslog \
  --partitions 3 --config retention.ms=300000 --config segment.ms=60000

# JuiceFS（Redis 元数据 + 本地盘）
juicefs format --storage file --bucket /var/lib/jfs-lab/bucket/ redis://127.0.0.1:6379/1 labfs
juicefs mount -d redis://127.0.0.1:6379/1 /jfs

# 负载 + 验证
sudo tools/gen-workload.sh /jfs/demo 2
kcat -b <IP>:9092 -t juicefs-accesslog -C -o beginning -e -q | jq -r .op | sort | uniq -c

# k3s + CSI（无 systemd 的环境需要先 mount --make-rshared /）
kubectl apply -f https://raw.githubusercontent.com/juicedata/juicefs-csi-driver/v0.33.1/deploy/k8s.yaml
kubectl apply -f k8s/lab-juicefs-sc-pvc.yaml
kubectl apply -f k8s/csi-mountpod-hostpid.yaml && kubectl apply -k .
```

## 8. 已知限制

- 名字中包含逗号时，`symlink`（名字与目标都可能含逗号）和 `rename` 的拆分是尽力而为，原始 `args` 保留在消息中；`lookup/create/mkdir/unlink/rmdir` 能正确处理逗号。
- 访问日志包含文件名、uid 等信息，属于敏感数据，注意 Kafka 的访问控制。
- 读取端与 Vector 之间没有持久化：Vector 重启期间（实测约 1~2s）的记录会被丢弃并计数，读取端自动重连。需要更强保证时，可以给读取端加一个写本地滚动文件的输出，再用 Vector `file` source 采集。
- `pid` → Pod 的解析只对仍在运行的进程有效（见第 4 节）。

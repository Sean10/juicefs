# JuiceFS 访问日志 + 文件清单采集到 Kafka（Vector 方案）

适用版本：JuiceFS 社区版 **1.3.x / 1.4.x**（两者访问日志格式完全一致，一套方案通用），
覆盖两种主要部署形态：**物理机 FUSE 挂载**、**Kubernetes CSI（Mount Pod 模式）**。

```text
                      ┌──────────── 每个节点 ────────────┐
 应用进程/Pod ──FUSE──▶ juicefs mount / CSI Mount Pod   │
                      │   <挂载点>/.accesslog (虚拟文件)   │
                      │          │ cat（每个挂载点一个）    │
                      │  juicefs-accesslog-reader.sh      │  发现挂载点、pid→进程/Pod、卷名
                      │          │ stdout（TSV）           │
                      │  Vector: exec → VRL 解析 → 过滤    │
                      │          → read/write 按 fh 聚合   │──▶ Kafka  juicefs-accesslog
                      └──────────────────────────────────┘          juicefs-accesslog-dlq（解析失败）

 每周 CronJob / cron:  juicefs dump(元数据引擎) → juicefs-inventory.py → 文件 + Kafka juicefs-inventory
 分析: 访问日志 (volume, inode) ⋈ 清单 (volume, inode) → 路径级热点 / 冷数据 / 谁在访问
```

## 目录

| 路径 | 说明 |
| --- | --- |
| `vector/juicefs-accesslog-reader.sh` | 发现本机所有 JuiceFS 挂载点并持续读取 `.accesslog`，补充进程名、Pod、PVC、卷名 |
| `vector/parse_accesslog.vrl` | VRL 解析器（覆盖全部 op 的结构化字段） |
| `vector/vector.yaml` | Vector 管道：exec → 解析 → 过滤 → 聚合 → Kafka（含 DLQ、磁盘缓冲、Prometheus 指标） |
| `vector/vector-tests.yaml` | Vector 单元测试（`vector test`） |
| `baremetal/` | 物理机：Vector 环境变量、systemd override、清单 cron |
| `k8s/vector-daemonset.yaml` | CSI 场景：Vector DaemonSet（含 Pod/PVC 映射 sidecar） |
| `k8s/csi-mountpod-hostpid.yaml` | CSI 全局配置：Mount Pod 开启 hostPID（Pod 级归属所需） |
| `k8s/inventory-cronjob.yaml` | CSI 场景：每周文件清单 CronJob |
| `k8s/lab-juicefs-sc-pvc.yaml` | 实验用 StorageClass/PVC/读写负载 |
| `kustomization.yaml` | `kubectl apply -k .` 部署 DaemonSet + CronJob（自动生成 ConfigMap） |
| `inventory/` | 清单脚本：`juicefs dump` JSON → 扁平记录（ndjson/csv） |
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
   | 完整 Vector 管道（同机抢 CPU） | 1553~1585 MiB/s | 1407~1660 MiB/s | 1408~1488 /s | 3096~3139 /s | 25113~29172 /s |

   大文件吞吐的下降主要来自 juicefs 为每个 128KiB 请求格式化日志，以及 Vector 和 juicefs 抢 4 个核；
   元数据/小文件操作差异在噪声范围内。生产环境对象存储一般是网络瓶颈，影响会小很多，但建议在目标机型上复测，
   并给 Vector 设置 CPU 限制。同一时段 juicefs 自身 `fuse_ops_total` 增加 159,967，读取端收到 162,780 行
   （含另一挂载点和内部操作），**未观察到丢行**。

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
# 1) 安装 Vector（官方 deb/rpm，或 tar 包），需要 bash / perl / awk（所有 Debian/Ubuntu/RHEL 都自带）
# 2) 部署配置
sudo mkdir -p /etc/vector /var/lib/vector
sudo cp vector/{vector.yaml,parse_accesslog.vrl,juicefs-accesslog-reader.sh} /etc/vector/
sudo chmod +x /etc/vector/juicefs-accesslog-reader.sh
sudo cp baremetal/vector.env /etc/default/vector          # 修改 KAFKA_BOOTSTRAP / JUICEFS_TZ / MOUNT_PREFIXES
sudo mkdir -p /etc/systemd/system/vector.service.d
sudo cp baremetal/vector-service-override.conf /etc/systemd/system/vector.service.d/override.conf  # 以 root 运行
sudo systemctl daemon-reload && sudo systemctl enable --now vector

# 3) 校验
JFS_VECTOR_DIR=/etc/vector KAFKA_BOOTSTRAP=localhost:9092 vector test /etc/vector/vector.yaml vector/vector-tests.yaml
curl -s localhost:9598/metrics | grep jfs_accesslog              # 各组件收发计数
kcat -b <kafka> -t juicefs-accesslog -C -o end
```

说明：

- 读取 `.accesslog` 需要 root（文件属主为 juicefs 进程用户，权限 0400），所以 Vector 用 root 运行。
- 新挂载的卷、重新挂载（升级/重启）都会在 `RESCAN_INTERVAL`（默认 10s）内自动接上；同一挂载被 bind 到多个路径时按设备号去重，避免重复。
- `comm`（进程名）是在日志到达时从 `/proc/<pid>/comm` 读取的，**短命进程（`dd`、`ls`、`cat` 等）多数已退出，解析不到**；长驻服务进程可以解析。
- Vector 的 `exec` source 不记录脚本的 stderr；排查读取问题可以手工执行 `sudo /etc/vector/juicefs-accesslog-reader.sh | head`。

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

DaemonSet 里有两个容器：

- `vector`：官方 `timberio/vector:0.50.0-debian` 镜像（自带 bash/perl/awk，不需要自己做镜像），
  挂载 `/var/lib/juicefs/volume`（HostToContainer）、宿主机 `/proc`（只读，用于 pid → cgroup → pod_uid）、
  `/var/lib/vector-juicefs`（磁盘缓冲）。不需要 privileged。
- `k8s-meta`：`alpine/kubectl`，每 30s 把本节点 Pod（uid → namespace/name）和 PV → PVC 映射写到共享 emptyDir，读取脚本据此补充 `pod_namespace`/`pod_name`/`pvc`。

归属能力（实测）：

| 维度 | 是否可得 |
| --- | --- |
| PV / PVC / 卷 / 子目录 | 始终可得 |
| Pod（`pod_uid`/`pod_name`） | 需要 hostPID；**长驻进程**可得（如应用主进程），短命进程（`cat`、`dd`）多数已退出解析不到；新建 Pod 最多约 60s 内名字为空（只有 uid） |
| `release`/`releasedir` 等内核发起的操作 | 内核本身就记为 pid 0 |

如果某个 PV 只被一个工作负载使用（最常见），按 `pvc` 归属已经足够；需要精确到 Pod 时再开 hostPID。

## 5. 文件清单（inventory）

### 5.1 方案

- 用 **`juicefs dump`（JSON 格式）** 直接从元数据引擎导出，不经过 FUSE、不需要挂载；
  `juicefs-inventory.py` 按行流式解析（内存恒定），输出每个路径一条记录：

  `volume, snapshot, inode, parent_inode, path, name, depth, type, size, mode, uid, gid, nlink, atime, mtime, ctime, symlink`

- 和 `find` 结果逐条对比，inode+路径集合一致；中文、空格、逗号、`%`、引号、换行、名为 `entries`/`attr` 的文件、硬链接（每个路径一条，同 inode）、软链接目标均已验证。
- 性能参考（本实验环境，Redis）：89 万条目，dump 5s（JSON 292MB），转换 14s（约 6 万条/s，单核），gzip 后 19MB。
  一亿文件量级转换约 30 分钟，周级任务可接受。
- 注意：`--binary` 格式不能流式解析，不要用；Redis 元数据量大时 dump 会给 Redis 带来读压力，建议在业务低峰执行，或者对从库执行。
  `META_PASSWORD` 环境变量可避免密码出现在命令行。

### 5.2 物理机：cron

```bash
sudo mkdir -p /opt/juicefs-accesslog-kafka && sudo cp -r inventory /opt/juicefs-accesslog-kafka/
sudo cp baremetal/juicefs-inventory.cron /etc/cron.d/juicefs-inventory   # 修改 META_URL / VOLUME / KAFKA
# 手工跑一次
sudo META_URL=redis://127.0.0.1:6379/1 VOLUME=labfs KAFKA_BOOTSTRAP=172.30.0.2:9092 \
  /opt/juicefs-accesslog-kafka/inventory/juicefs-inventory.sh
```

输出 `$OUT_DIR/<volume>/inventory-<UTC时间>.ndjson.gz`（默认保留 35 天），设置了 `KAFKA_BOOTSTRAP` 时用 `kcat` 逐条写入 `juicefs-inventory` topic。

### 5.3 Kubernetes：CronJob

```bash
kubectl -n juicefs-logging create secret generic juicefs-inventory \
  --from-literal=metaurl=redis://172.30.0.2:6379/3 --from-literal=volume=k8sfs
kubectl apply -k .                                                   # 包含 k8s/inventory-cronjob.yaml
kubectl -n juicefs-logging create job --from=cronjob/juicefs-inventory inv-manual   # 立即跑一次
```

init 容器用 `juicedata/mount:ce-v1.4.0`（自带 juicefs 和 python3）做 dump + 转换，主容器用 `edenhill/kcat` 写入 Kafka。
默认每周日 02:00（Asia/Shanghai）执行。CSI 动态 PV 在卷里是 `/<pv名>/...` 子目录，清单路径的第一级就是 PV 名，可以直接和访问日志的 `pv` 字段对应。

### 5.4 生产建议

清单是周级全量快照，量大时更适合**落对象存储/数仓**（Hive/Iceberg/ClickHouse），Kafka topic 只做传输；
本方案的 ndjson.gz 文件可以直接导入。

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
- 读取端与 Vector 之间没有持久化：Vector 重启期间（通常 <1s）的日志会丢失，读取端重连后继续。需要更强保证时，可以把读取脚本改为写本地滚动文件，再用 Vector `file` source 采集。
- `pid` → Pod 的解析只对仍在运行的进程有效（见第 4 节）。

# 射电观测接收机时间对账数据湖（Receiver-Time Reconciliation Lake）

纯 Python 标准库实现（无任何第三方依赖、无需联网、无需 pip），接入设备班报、排班日志、
结算台账三类本地数据，核对各项目实际占用的接收机时间，并给出每个允许配对的可行占用区间。

---

## 1. 它解决什么问题

- **设备班报**：GB18030 编码 CSV，按**观测夜**分区，记录每台接收机每夜的总占用**秒数**。
- **排班日志**：UTF-8 JSON Lines，按**观测夜**分区，记录项目*可能*使用的接收机与观测夜。
- **结算台账**：Parquet，按**结算期**分区，记录项目在该结算期实际占用的**毫秒数**（行内含观测夜）。
- 设备与排班使用不同的接收机标识，由对照文件 `receiver_map.json` 统一。

系统只允许排班中出现的（项目, 接收机, 观测夜）**配对**，并要求同时满足两侧合计：

- 设备合计：`(接收机, 观测夜)` 的总占用毫秒；
- 项目合计：`(项目, 结算期)` 的总占用毫秒。

共享任一合计的配对通过并查集连成一个**核对组**，组可以跨夜。对每个组：

| 状态 | 含义 |
| --- | --- |
| `pending`（待补） | 组内任一合计缺失 |
| `isolated`（整组隔离） | 合计齐全但不存在满足所有合计的整数分配 |
| `feasible`（可行） | 存在可行分配；逐配对给出最小/最大占用毫秒，二者相等即为确定值 |

实际占用一律按**整毫秒**计（设备秒数 ×1000），全程整数运算，结果精确。

---

## 2. 目录结构

```
recon/
  parquet.py     零依赖 Parquet 读写（手写 Thrift Compact，gzip/无压缩，INT64/BYTE_ARRAY）
  maxflow.py     整数 Dinic 最大流 + 有下界环流可行性 + 配对 min/max 精确边界
  storage.py     内容寻址 blob、SQLite 台账、原子版本目录与 current 指针（快照隔离）
  parsers.py     GB18030 CSV / UTF-8 JSONL / Parquet 解析、校验、隔离记录
  reconcile.py   配对生成、并查集分组（可跨夜）、合计核对、min/max 计算
  pipeline.py    整批替换接入、隔离作废、受影响组增量重算、幂等发布
  query.py       只读查询与可加租约的旧版本 Snapshot
  retention.py   最近 N 版保留、引用确认、读/写租约保护与可恢复 GC
  gc.py          retention.py 的常用别名入口
  fixtures.py    自带离线夹具生成器
  selftest.py    端到端自检
recon_cli.py     统一入口（适配 python3 -I 隔离模式）
tests/run_all_tests.py   一键测试（78 项断言，约 0.4 秒）
examples/receiver_map.json
docs/FORMATS.md  输入输出格式详细说明
```

---

## 3. 快速开始（一条命令，80 秒内给出可读结果）

无需安装任何东西。在仓库根目录执行：

```bash
python3 -I tests/run_all_tests.py
```

或查看带完整业务输出的端到端演示：

```bash
python3 -I recon_cli.py demo
```

两者都在约 0.1 秒内完成，自动生成临时夹具、跑通接入/重跑/查询/隔离/崩溃恢复/离线对账，
结束后清理临时目录。加 `--keep` 可保留临时数据湖路径以便人工查看。

> 使用 `-I`（隔离模式）运行：忽略环境变量与当前目录，避免被不可信目录中的同名模块影响。

---

## 4. 输入文件格式

### 4.1 设备班报（GB18030 CSV，按观测夜分区）

路径示例：`device/night=N1/part.csv`

```csv
night,receiver_code,occupied_seconds
N1,DEV-A,10000
N1,DEV-B,4000
```

- `receiver_code` 必须能在对照文件中映射到规范接收机，否则该行进隔离区；
- `occupied_seconds` 必须是非负整数，否则隔离；内部 ×1000 转毫秒。

### 4.2 排班日志（UTF-8 JSON Lines，按观测夜分区）

路径示例：`schedule/night=N1/part.jsonl`

```json
{"project": "ALPHA", "receivers": ["R-A", "R-B"], "nights": ["N1"]}
{"project": "BETA",  "receivers": ["R-A"],        "nights": ["N1"]}
```

- 每行一个对象；`project` 字符串、`receivers` 与 `nights` 为非空字符串数组；
- 笛卡尔积生成允许配对 `(project, receiver, night)`。

### 4.3 结算台账（Parquet，按结算期分区）

路径示例：`ledger/period=P1/part.parquet`，列：

| 列 | 类型 | 说明 |
| --- | --- | --- |
| `project` | BYTE_ARRAY(UTF-8) | 项目标识 |
| `settlement_period` | BYTE_ARRAY | 结算期（分区键） |
| `night` | BYTE_ARRAY | 该条占用对应的观测夜 |
| `occupy_ms` | INT64 | 实际占用毫秒，非负整数 |

同一 `(project, settlement_period)` 的多行自动累加成一个项目合计。

### 4.4 接收机对照（JSON）

```json
{ "DEV-A": "R-A", "DEV-B": "R-B", "DEV-C": "R-C" }
```

也支持 `[{"device_code": "DEV-A", "receiver": "R-A"}, ...]` 形式。

---

## 5. 输出（发布版本）

每次重跑生成一个**不可变版本** `versions/v<id>/`，并原子切换 `current` 指针：

```
<lake>/
  blobs/<sha256 前2位>/<sha256>                 原始文件的内容寻址副本
  versions/v<id>/
    current.json                               版本清单（run、组状态计数）
    groups/<group_id>.json                     每个核对组的完整结果
    quarantine/<qid>.json                      被拒绝记录及原因
  current -> versions/v<id>                    原子符号链接（不支持时为指针文件）
  locks/                                       读共享/写与 GC 排他的 flock 文件
  state.db                                     SQLite：接入/运行/租约/GC 台账
```

组结果文档（`groups/<group_id>.json`）关键字段：

```json
{
  "group_id": "g858c4901fc1601da",
  "status": "feasible",
  "nights": ["N1", "N2"],
  "pairs": ["ALPHA|R-A|N1", "..."],
  "totals": { "device": [...], "project": [...] },
  "by_night": {
    "N1": [
      {"project": "ALPHA", "receiver": "R-A", "night": "N1",
       "min_ms": 5000000, "max_ms": 9000000,
       "determined": false, "value_ms": null}
    ],
    "N2": [
      {"project": "ALPHA", "receiver": "R-A", "night": "N2",
       "min_ms": 6000000, "max_ms": 6000000,
       "determined": true, "value_ms": 6000000}
    ]
  },
  "lineage": {
    "files": ["/abs/path/.../part.csv", "..."],
    "records": [{"source": "device", "source_path": "...", "location": 2, "ref": [...]}]
  },
  "input_hash": "...", "result_hash": "..."
}
```

- `min_ms == max_ms` 时 `determined=true` 且 `value_ms` 为确定值；否则为可行区间。
- `lineage.records` 精确定位到来源文件与原始位置（CSV/JSONL 为 1 起始行号，Parquet 为行号）。

---

## 6. 命令行用法

全局参数：`--lake <数据湖目录>`（默认 `.lake` 或环境变量 `RECON_LAKE`）、
`--receiver-map <对照.json>`（默认内置夹具对照）。

```bash
# 接入一个来源的一个分区文件（按 来源+分区 整批替换）
python3 -I recon_cli.py --lake ./lake ingest \
    --source device --partition N1 --file /data/device/night=N1/part.csv
python3 -I recon_cli.py --lake ./lake ingest \
    --source schedule --partition N1 --file /data/schedule/night=N1/part.jsonl
python3 -I recon_cli.py --lake ./lake ingest \
    --source ledger --partition P1 --file /data/ledger/period=P1/part.parquet

# 全量重算并发布
python3 -I recon_cli.py --lake ./lake rerun

# 增量重算：只重算受影响组（其它组结果与血缘保持不变）
python3 -I recon_cli.py --lake ./lake rerun --changed device=N3
python3 -I recon_cli.py --lake ./lake rerun --changed ledger=P1,P2

# 查询
python3 -I recon_cli.py --lake ./lake query                 # 全部配对，表格
python3 -I recon_cli.py --lake ./lake query --night N1      # 按夜
python3 -I recon_cli.py --lake ./lake query --project ALPHA --format json
python3 -I recon_cli.py --lake ./lake query --status isolated
python3 -I recon_cli.py --lake ./lake groups                # 组列表
python3 -I recon_cli.py --lake ./lake group g858c4901fc1601da
python3 -I recon_cli.py --lake ./lake lineage g858c4901fc1601da
python3 -I recon_cli.py --lake ./lake quarantine
python3 -I recon_cli.py --lake ./lake summary
python3 -I recon_cli.py --lake ./lake snapshot

# 只保留最近 3 个发布版，回收确认无引用的旧快照/原件副本
python3 -I recon_cli.py --lake ./lake retention --keep 3
python3 -I recon_cli.py --lake ./lake gc 2 --dry-run   # 预演；gc/cleanup/retain 同义
```

Python API 可用 `lake.open_snapshot(1)` 固定并共享锁定旧版；上下文退出后该版才可回收：

```python
with lake.open_snapshot(1) as old:
    old.query_pairs()       # 可暂停在旧版并继续读完
    lake.retain_versions(2) # 同时清理其它无引用版本和 blob
```

接入非法文件时进程以退出码 `2` 结束，打印隔离原因，**该次替换作废**，旧分区保留：

```bash
python3 -I recon_cli.py --lake ./lake ingest --source device \
    --partition N3 --file /data/bad/night=N3/part.csv
# REPLACEMENT VOIDED: device/N3 had 1 rejected record(s); previous partition retained.
```

---

## 7. 关键正确性与运维保证

1. **整批替换、隔离作废**：按 `(来源, 分区)` 整体替换；只要有一条标识或数值无法转换，
   该分区全部不入库，拒绝记录连同原因进入隔离区，旧数据保持不变。
2. **只允许排班配对**：配对集合完全由排班日志决定，设备/台账无法创造新配对。
3. **同时满足两侧合计**：可行性建模为有上下界的整数流（Dinic + 超级源汇环流），
   min/max 经精确边界搜索得到，随机网络已用穷举法对照验证。
4. **共享合计成组、可跨夜**：并查集按共享设备合计或项目结算合计连通。
5. **增量重跑**：仅重算输入发生变化的组；输入未变的组沿用已发布结果，
   `input_hash`/`result_hash` 不变，业务结果与血缘稳定。
6. **幂等**：blob 内容寻址，相同输入产生相同哈希；重复重跑结果一致。
7. **快照隔离**：先写完整版本目录，再原子切换 `current`；跨夜查询只读一个完整版本。
8. **崩溃恢复**：发布中途停机，`current` 仍指向上一完整版本；可直接再次重跑发布新版本。
9. **离线对账**：原始文件下线后，所有已接收分区仍以 blob 形式可重算、可对账。
10. **血缘可查**：每组可追溯用到的输入文件与每条原始记录位置。
11. **发布保留与安全回收**：`retention --keep N` 保留最近 N 个完整发布版和
    `current`；活动读租约固定的旧版、保留版 `input_blobs`/血缘中的原件、
    当前 `ingests` 中已接收（含尚未重跑发布）的分区、活动写租约目标均不删除。
12. **只回收可确认无引用原件**：每个接入 blob 的来源/分区追加记录在
    `ingest_history`；只有来源明确、且不被任何受保护版本、当前分区或租约引用的
    blob 才删除。磁盘上存在但来源/归属不明的旧 blob 一律保留并在报告
    `unknown_blobs` 中列出。
13. **旧查询不被清理打断**：`lake.open_snapshot(vid)` /
    `QueryService(lake, version_id=vid)` 建立 SQLite 租约和 per-version
    共享 `flock`；GC 用非阻塞排他锁二次确认。暂停在旧版期间，该版及其 blob
    受保护，其它废弃版本/blob 仍可清理；关闭快照后租约释放。
14. **中断可续跑**：GC 删除项写入 `gc_runs/gc_items`，每个删除动作幂等；
    中途异常或进程退出后，再次执行 `retention`/`gc` 会调和未完成日志并继续。

---

## 8. 自带夹具场景

`recon/fixtures.py` 生成的数据覆盖：

- 跨 N1/N2 的可行组：N1 为 2×2 配对产生真实 min<max 区间，N2 为确定值；
- N3 的 GAMMA 组：初始缺设备合计 → `pending`，摄入修正文件后变 `feasible`；
- N4 的 DELTA 组：设备合计与项目合计冲突 → `isolated`；
- 一个非法 N3 设备文件：触发隔离与替换作废；
- 保留/GC 夹具：连续发布 5 版并替换原件，覆盖旧版流式查询暂停、租约保护下
  清理其它废弃版本与 blob、未发布分区/活动写租约/不明归属 blob 保护，以及
  注入删除中断后的日志续跑。

可通过 `python3 -I recon_cli.py demo --keep` 保留生成的目录人工检查。

# 输入输出格式规范（FORMATS）

本文档给出三类输入、接收机对照、以及数据湖输出的精确定义。所有时间统一为**整数毫秒**。

## 1. 设备班报 device

- 编码：**GB18030**；分隔符：逗号；首行为表头；按观测夜分区（一个分区一个 CSV）。
- 必含列：

| 列 | 类型 | 校验 |
| --- | --- | --- |
| `night` | 字符串 | 非空，观测夜标识 |
| `receiver_code` | 字符串 | 必须存在于接收机对照，否则隔离 |
| `occupied_seconds` | 整数文本 | 非负整数；负数/非整数隔离；入库时 `×1000` 得毫秒 |

- 空行忽略；列数不足、缺列、GB18030 解码失败均使整个分区进入隔离流程。
- 原始位置：CSV 1 起始行号（表头为第 1 行）。

## 2. 排班日志 schedule

- 编码：**UTF-8**；JSON Lines，每行一个 JSON 对象；按观测夜分区。
- 字段：

| 字段 | 类型 | 校验 |
| --- | --- | --- |
| `project` | 字符串 | 非空 |
| `receivers` | 字符串数组 | 非空，元素均为非空字符串（规范接收机标识） |
| `nights` | 字符串数组 | 非空，元素均为非空字符串 |

- 行不是合法 JSON、不是对象、字段缺失或类型错误 → 该行隔离，整个分区替换作废。
- 允许配对 = `project × receivers × nights` 的笛卡尔积。
- 原始位置：JSONL 1 起始行号。

## 3. 结算台账 ledger

- 格式：**Parquet**（本系统自带读写器，支持 gzip 或无压缩、PLAIN 编码、扁平 required schema）。
- 物理类型：字符串列为 `BYTE_ARRAY`(UTF-8)，整数列为 `INT64`。
- 必含列：

| 列 | Parquet 类型 | 含义/校验 |
| --- | --- | --- |
| `project` | BYTE_ARRAY | 非空项目标识 |
| `settlement_period` | BYTE_ARRAY | 非空结算期，分区键 |
| `night` | BYTE_ARRAY | 非空观测夜，用于关联设备侧 |
| `occupy_ms` | INT64 | 非负整数毫秒；负数/非整数隔离 |

- 同一 `(project, settlement_period)` 的多行求和为单一项目合计。
- 原始位置：Parquet 行号（0 起始）。
- 与主流引擎互通：只读扁平 required、INT32/INT64/BYTE_ARRAY、PLAIN、gzip/无压缩的文件。

## 4. 接收机对照 receiver_map

JSON 对象 `{设备代码: 规范接收机}`，或对象数组
`[{"device_code": ..., "receiver": ...}]`。设备代码即设备班报中的 `receiver_code`，
规范接收机即排班日志 `receivers` 中使用的标识。

## 5. 内部规范化记录

解析通过后统一为：

- device：`{night, receiver_code, receiver(规范), occupied_ms(=秒×1000)}`
- schedule：`{project, receivers[], nights[]}`
- ledger：`{project, settlement_period, night, occupy_ms}`

## 6. 数据湖输出

### 6.1 Blob

`blobs/<sha256[:2]>/<sha256>`：每个接入文件的不可变内容寻址副本，原始文件离线后仍可据此重算。

### 6.2 版本目录 `versions/v<id>/`

- `current.json`：版本清单（run_id、发布时间、组计数、状态计数、组 id 列表、接入报告）。
- `groups/<group_id>.json`：组结果，含 `status`、`nights`、`pairs`、`totals`、
  `pair_results`、`by_night`、`lineage`、`input_hash`、`result_hash`。
- `quarantine/<qid>.json`：被拒绝记录，含 `source/partition/source_path/location/raw/reason`。

### 6.3 current 指针

`current` 为指向最新完整版本目录的符号链接；文件系统不支持符号链接时退化为含绝对路径的
原子指针文件。读取方只解析该指针，因此读到的永远是一个完整版本。

### 6.4 SQLite `state.db`

- `ingests(source, partition)`：当前生效分区的 blob、来源路径、字节数、接入时间、run。
- `runs(run_id)`：运行生命周期。
- `group_versions(group_id, version_id)`：每组的输入/结果哈希与血缘版本。

## 7. 状态语义

| status | 触发条件 | 配对输出 |
| --- | --- | --- |
| `pending` | 引用的设备合计或项目合计缺失 | 不计算，给 `missing_totals` |
| `isolated` | 合计齐全但有界整数流无可行解 | 不计算，给原因与合计快照 |
| `feasible` | 存在可行整数分配 | 每配对 `[min_ms, max_ms]`，相等为确定值 |

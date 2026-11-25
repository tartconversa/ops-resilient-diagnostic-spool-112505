# 诊断日志采集代理

标准库 Python 3.11 实现：在网关本地接收可重放诊断记录，按磁盘/来源预算决定接纳、采样、限流或显式记缺口，再用稳定 `record_id` 向 `spool_peer.py` 或兼容 `/batch`、`/received` 的收集端重传。

所有关键结论都落在 SQLite：**是否接纳、哪些 ID 已被收集端确认、哪些片段可重传、哪些范围在等迟到片段、哪些片段已实际缺失**。代理退出后用同一个数据库重开，不会把“发过但未确认”当成“已送达”。

## 快速验证

```sh
python -m unittest discover -s tests -v

# 使用仓库 materials 跑确定性演练，并打印值班员最关心的 48ms 视图
python rehearse_agent.py /tmp/rehearsal-report.json
# 等价入口
python -m collector_agent rehearse --report /tmp/rehearsal-report.json
```

演练覆盖：

- `f-201`、`w-401` 首批稳定 ID 完整确认；
- 旧批次 `[f-202, w-402]` 在 `at_ms=20` 遇到 `reply=false`，收集端实际持久化 `f-202` 后断连；
- `at_ms=47` 通过 `GET /received` 对账出部分确认，再补送 `w-402`；
- `health/h2` 的常规记录 `h-21` 因来源预算/积压被拒并限流，接纳结果与缺口查询一致；
- `worker/t40` 在 `at_ms=46` 显示第 3 片等待，`w-403` 在 48ms 到达后愈合；
- 同一 `f-203` 在 54ms 重放不产生重复记录；
- 最终 `worker/t40`、`front/t20`、`front/t21` 均严格交付完整，没有可重传 ID。

## 真实双进程联调

```sh
# 终端 1：合成收集端
python spool_peer.py --port 8099 --state /tmp/collector.sqlite3

# 终端 2：采集代理 HTTP 入口；后台线程会 flush 并周期性 /received 对账
python -m collector_agent serve \
  --db /tmp/agent.sqlite3 \
  --collector http://127.0.0.1:8099 \
  --ingest-port 8100
```

另一个终端：

```sh
curl -s http://127.0.0.1:8100/status | python -m json.tool
curl -s -X POST http://127.0.0.1:8100/reconcile \
  -H 'Content-Type: application/json' -d '{}' | python -m json.tool

# 不开 HTTP 入口时也可直接查持久化状态/对账
python -m collector_agent status --db /tmp/agent.sqlite3 \
  --collector http://127.0.0.1:8099
```

HTTP 入口包括 `POST /ingest`、`/disk`、`/sources`、`/flush`、`/reconcile`、`/limit-source`，以及 `GET /status`、`/gaps`、`/retransmittable`。

## Python API

```python
from collector_agent import CollectionAgent, HttpTransport, Policy

agent = CollectionAgent(
    "/tmp/agent.sqlite3",
    transport=HttpTransport("http://127.0.0.1:8099"),
    policy=Policy(
        routine_sample_rate=1.0,   # 磁盘平稳时可调成 0.1 等策略值
        gap_wait_ms=10_000,
        collector_stall_ms=30_000,
        max_pending_age_ms=30_000,
    ),
)

agent.register_source(
    "worker",
    horizon_ms=36,
    protected_types=["error", "end"],
    reserve_units=26,
)
agent.apply_disk_snapshot(
    at_ms=40, spool_limit=49, total_free=24, online_floor=18
)

admission = agent.ingest(record, now_ms=41)
batch = agent.flush(at_ms=47)             # 断连时状态是 indeterminate
remote = agent.reconcile_remote()         # GET /received，接住晚到的部分确认
status = agent.status(now_ms=48)
```

常用别名：`CollectorAgent`、`DiagnosticAgent`、`Collector`、`agent.receive/handle/process`、`agent.send_batch`、`agent.ack/mark_accepted`、`agent.snapshot/report`。

## 对账口径

### 接纳结果

每条输入返回稳定决策之一：

- `accepted`：已在代理 SQLite 中持久化并占用本地配额；
- `duplicate`：稳定 `record_id` 已收，重放不产生第二份证据；
- `sampled`：常规记录按采样策略主动省略，状态中记为已知 `sampled`；
- `rate_limited`：来源已因积压/预算/收集端停滞被暂停常规流量；
- `rejected`：预算不足，关键记录也未能留下；同一事务写入实际缺口。

### 磁盘和来源预算

可接纳余量同时受两层约束：

```text
disk_headroom   = total_free - online_floor
spool_headroom  = spool_limit - 当前未回收 spool 占用
admit_headroom  = min(disk_headroom, spool_headroom)
source_headroom = reserve_units - 当前来源未回收占用
```

收到新的磁盘快照时，`total_free` 以快照为准；快照之后的代理写入减少 `free_units`，回收则加回。

回收优先级：

1. 已被收集端确认的记录可安全释放元数据以外的本地占用；
2. 磁盘/在线空间紧张时，先淘汰未确认 routine，再淘汰未保护 detail；
3. 最后才可能牺牲受保护的 detail/end/error，但会在同一事务写入 `missing` 缺口和边界；
4. 为容纳一条最终仍被来源预算拒绝的记录而做的来源内淘汰会回滚，避免无谓制造缺口。

### 错误回看和范围完整性

- `error`、`end` 等 `protected_types` 不会被主动采样。
- `error` 到达时，同一 `source/trace` 中满足 `horizon_ms` 的非 routine 临时记录会提升为受保护记录；迟到但属于错误 scope 的分片也按关键记录处理。
- `end.part=N` 声明最高片段号。只有 `1..N` 每一片都处于“本地仍在”或“收集端已确认”时，`evidence_complete` 才为真。
- 缺片先为 `waiting`；超过 `gap_wait_ms` 后转为实际 `missing`。迟到分片可将缺口愈合。
- 采样省略、拒收、淘汰分别保留为 `sampled` 或 `missing`，因此代理不会在缺片时把范围报成完整。
- 顶层 `complete` 采用最严格口径：**证据齐且所有稳定 ID 均被收集端确认**。另外提供 `evidence_complete` 和 `delivered_complete`，避免混淆“已拼齐”和“已送达”。

缺口边界示例：

```json
{
  "scope": "worker/t40",
  "from_part": 3,
  "to_part": 3,
  "status": "waiting",
  "reason": "late_part",
  "before_part": 2,
  "after_part": 4
}
```

### 部分确认、断连和重启

- `flush()` 只把响应 `accepted` 列出的 ID 标记为收集端已接收；同批其他 ID 仍可重传。
- 连接断开时批次为 `indeterminate`：已经发出不等于对端未收，所有 ID 仍保留在可重传集合。
- `reconcile_remote()` 调 `GET /received` 或接收一份已知已确认 ID，按稳定 `record_id` 对账；收集端重复 ID 只记一次。
- 全量 `/received` 对账还会结清历史 `indeterminate` 批次中哪些 item 实际已确认。
- 只有 `collector_acked=1` 的记录会被自动回收；发过但未确认的记录不会删除。
- 所有状态在 SQLite 中。关闭并以同一个 `--db` 重开后，`status()`、`retransmittable_record_ids`、`actual_missing` 和 scope phase 使用完全相同的计算口径。

## 用新的演练资料继续

准备一个符合 `materials/README.md` 字段的新目录，包含 `sources.jsonl`、`disk.jsonl`、`records.jsonl`、`endpoint.jsonl`，然后做通用回放：

```sh
python -m collector_agent replay /path/to/new-feed --report /tmp/new.json
```

内置的 `rehearse` 是按当前 `materials` 的固定时点编排的讲解型演练；新的任意日志流请用 `replay`，也可以直接调用 `ingest()`、`apply_disk_snapshot()`、`reconcile_remote()` 接入。

## 浏览原始资料

```sh
python materials.py
python materials.py records
python materials.py endpoint
```

字段定义见 [`materials/README.md`](materials/README.md)。`spool_peer.py` 的 `/batch` 按 `endpoint.jsonl` 的 `max_accept`、`reply` 模拟部分确认和持久化后断连；`/received` 提供收集端稳定 ID 全量集合。

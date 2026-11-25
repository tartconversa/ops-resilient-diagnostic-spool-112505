"""使用仓库自带 materials 跑一遍确定性运维演练。"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .agent import CollectionAgent, Policy, ScriptedTransport


def _by_id(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {row["record_id"]: row for row in rows}


def run_material_rehearsal(
    db_path: str | Path = ":memory:",
    *,
    materials_root: str | Path | None = None,
    policy: Policy | None = None,
) -> dict[str, Any]:
    """按精心安排的对账时点运行资料流。

    时间点与 materials/*.jsonl 对齐：
    - t=4 首批稳定 ID 被收集端完整确认；
    - t=20 旧批次在无响应中断开，收集端实际只持久化第一个 ID；
    - t=29 health 因来源预算/积压进入 routine 限流，h-21 被一致地记为缺口；
    - t=47 先通过 /received 对账发现旧批次的部分确认，再补送剩余 ID；
    - t=48 迟到的 w-403 终于可接纳，此时值班状态保留三类信息。
    """

    root = Path(materials_root) if materials_root is not None else Path(__file__).resolve().parent.parent / "materials"
    sources = list(_read_named("sources", root))
    disk_rows = list(_read_named("disk", root))
    records = list(_read_named("records", root))
    endpoint_rows = list(_read_named("endpoint", root))
    by_id = _by_id(records)

    transport = ScriptedTransport(endpoint_rows)
    agent = CollectionAgent(
        db_path,
        transport=transport,
        policy=policy or Policy(
            gap_wait_ms=100,
            collector_stall_ms=15,
            max_pending_age_ms=20,
            backlog_high_watermark=0.85,
            # 固定演练要显式展示 47ms 的晚到 /received 对账，因此不在接纳前自动查询。
            reconcile_before_eviction=False,
        ),
    )
    timeline: list[dict[str, Any]] = []

    def note(label: str, value: dict[str, Any]) -> dict[str, Any]:
        event = {"label": label, **value}
        timeline.append(event)
        return event

    agent.load_sources(sources)
    agent.apply_disk_snapshot(disk_rows[0])

    agent.ingest(by_id["f-201"])
    agent.ingest(by_id["w-401"])
    first = agent.flush(at_ms=4, now_ms=4, record_ids=["f-201", "w-401"])
    note("first_batch_acked", first)

    agent.ingest(by_id["h-11"])
    agent.ingest(by_id["f-202"])
    agent.ingest(by_id["w-402"])

    old_batch = agent.flush(at_ms=20, now_ms=20, record_ids=["f-202", "w-402"])
    note("old_batch_disconnected_after_partial_persist", old_batch)

    agent.ingest(by_id["f-203"])
    agent.apply_disk_snapshot(next(row for row in disk_rows if row["at_ms"] == 25))

    health_admission = agent.ingest(by_id["h-21"])
    note("health_routine_rejected_and_limited", {
        "decision": health_admission["decision"],
        "reason": health_admission["reason"],
        "routine_admission": health_admission["source_state"]["routine_admission"],
    })

    agent.ingest(by_id["w-404"])
    agent.apply_disk_snapshot(next(row for row in disk_rows if row["at_ms"] == 40))
    agent.ingest(by_id["w-405"])
    agent.ingest(by_id["f-204"])

    waiting_status = agent.status(now_ms=46)
    key_moment_before_late = {
        "at_ms": 46,
        "health_routine_admission": waiting_status["sources"]["health"]["routine_admission"],
        "t40": next(s for s in waiting_status["scopes"] if s["scope"] == "worker/t40"),
        "retransmittable_record_ids": waiting_status["retransmittable_record_ids"],
        "actual_missing": waiting_status["actual_missing"],
    }
    note("operator_view_waiting_for_late_part", key_moment_before_late)

    partial_reconcile = agent.reconcile_remote(at_ms=47, now_ms=47)
    note("late_partial_ack_reconciled", partial_reconcile)
    retry_rest = agent.flush(at_ms=47, now_ms=47, record_ids=["w-402"])
    note("retransmit_missing_half", retry_rest)

    late_part = agent.ingest(by_id["w-403"])
    key_status = agent.status(now_ms=48)
    operator_view = {
        "at_ms": 48,
        "new_worker_part_decision": late_part["decision"],
        "health_new_routine": key_status["sources"]["health"]["routine_admission"],
        "front_new_routine": key_status["sources"]["front"]["routine_admission"],
        "retransmittable_record_ids": key_status["retransmittable_record_ids"],
        "actual_missing": key_status["actual_missing"],
        "worker_t40": next(s for s in key_status["scopes"] if s["scope"] == "worker/t40"),
        "health_h2": next(s for s in key_status["scopes"] if s["scope"] == "health/h2"),
    }
    note("operator_view_late_part_arrives", operator_view)

    # 值班视图取样后再排空 48 时点的积压；收集端仍处于 max_accept=2 状态，
    # 因此需要多批，稳定 ID 使重发/部分确认不会产生重复证据。
    drain_events = []
    for index in range(3):
        drain = agent.flush(at_ms=48 + index, now_ms=48 + index)
        drain_events.append(drain)
        if drain["sent"] == 0:
            break
    note("drain_after_operator_snapshot", {"batches": drain_events})

    agent.apply_disk_snapshot(next(row for row in disk_rows if row["at_ms"] == 52))
    agent.ingest(by_id["f-203"])  # 同一 record_id 的重放。
    agent.ingest(by_id["f-211"])
    agent.ingest(by_id["f-212"])
    agent.ingest(by_id["f-213"])

    first_final = agent.flush(at_ms=69, now_ms=69)
    note("final_batch_acked", first_final)
    agent.apply_disk_snapshot(next(row for row in disk_rows if row["at_ms"] == 72))
    second_final = agent.flush(at_ms=72, now_ms=72)
    note("later_flush", second_final)

    final_status = agent.status(now_ms=72)
    return {
        "agent": agent,
        "transport": transport,
        "timeline": timeline,
        "operator_view": operator_view,
        "key_moment_before_late": key_moment_before_late,
        "final_status": final_status,
    }


def _read_named(name: str, root: Path):
    # 延迟导入使本模块在被复制/单独引用时错误信息更直接。
    from materials import read_feed

    return read_feed(name, root)


def write_report(result: dict[str, Any], output_path: str | Path) -> None:
    agent = result["agent"]
    try:
        report = {key: value for key, value in result.items() if key not in {"agent", "transport"}}
        Path(output_path).write_text(
            json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True, default=str),
            encoding="utf-8",
        )
    finally:
        agent.close()


run_rehearsal = run_material_rehearsal


def replay_directory(
    root: str | Path,
    db_path: str | Path = ":memory:",
    *,
    policy: Policy | None = None,
    flush_after_record: bool = True,
    reconcile_at_end: bool = True,
    drain_batches: bool = True,
    final_now_ms: int | None = None,
) -> dict[str, Any]:
    """通用 JSONL 日志流回放，不依赖内置演练的固定 record_id。

    目录仍使用 materials README 中定义的四个 feed。调用方也可以直接使用
    CollectionAgent API 对接非文件日志流。
    """

    root = Path(root)
    sources = list(_read_named("sources", root))
    disk_rows = list(_read_named("disk", root))
    records = list(_read_named("records", root))
    endpoint_rows = list(_read_named("endpoint", root))

    transport = ScriptedTransport(endpoint_rows)
    agent = CollectionAgent(db_path, transport=transport, policy=policy or Policy())
    agent.load_sources(sources)

    disk_rows = sorted(disk_rows, key=lambda row: row["at_ms"])
    disk_index = 0
    timeline: list[dict[str, Any]] = []

    def apply_due_disk(until_ms: int) -> None:
        nonlocal disk_index
        while disk_index < len(disk_rows) and disk_rows[disk_index]["at_ms"] <= until_ms:
            row = disk_rows[disk_index]
            timeline.append({"label": "disk_snapshot", "result": agent.apply_disk_snapshot(row)})
            disk_index += 1

    for record in records:
        at_ms = int(record.get("at_ms", 0))
        apply_due_disk(at_ms)
        admission = agent.ingest(record, now_ms=at_ms)
        event: dict[str, Any] = {
            "label": "record",
            "at_ms": at_ms,
            "record_id": record["record_id"],
            "decision": admission["decision"],
        }
        if flush_after_record:
            event["flush"] = agent.flush(at_ms=at_ms, now_ms=at_ms)
        timeline.append(event)

    last_time = max(
        [int(row["at_ms"]) for row in records]
        + [int(row["at_ms"]) for row in disk_rows]
        + [0]
    )
    apply_due_disk(last_time)
    drain: list[dict[str, Any]] = []
    if reconcile_at_end:
        drain.append(agent.reconcile_remote(at_ms=last_time, now_ms=last_time, authoritative=True))
    if drain_batches:
        for _attempt in range(10_000):
            batch = agent.flush(at_ms=last_time, now_ms=last_time)
            drain.append(batch)
            if batch["sent"] == 0:
                break
            if batch["status"] == "indeterminate":
                # 断连后用 /received 全量对账；若没有任何新确认，说明收集端仍不可写，
                # 保留剩余记录为可重传而不是无限重试。
                reconciled = agent.reconcile_remote(
                    at_ms=last_time, now_ms=last_time, authoritative=True
                )
                drain.append(reconciled)
                if not reconciled["newly_acked"]:
                    break
            elif batch["status"] in ("partial", "acknowledged") and batch.get("accepted"):
                continue
            else:
                break
        if reconcile_at_end:
            drain.append(agent.reconcile_remote(at_ms=last_time, now_ms=last_time, authoritative=True))

    now_ms = final_now_ms if final_now_ms is not None else last_time
    status = agent.status(now_ms=now_ms)
    return {
        "agent": agent,
        "transport": transport,
        "timeline": timeline,
        "drain": drain,
        "status": status,
    }


def write_generic_report(result: dict[str, Any], output_path: str | Path) -> None:
    agent = result["agent"]
    try:
        Path(output_path).write_text(
            json.dumps(
                {k: v for k, v in result.items() if k not in {"agent", "transport"}},
                ensure_ascii=False,
                indent=2,
                default=str,
            ),
            encoding="utf-8",
        )
    finally:
        agent.close()

"""持久化采集代理核心：接纳策略、磁盘预算、缺口对账和稳定重传。

仅使用 Python 标准库。所有会改变“是否接纳/是否缺失”结论的状态都在同一个
SQLite 事务中落盘，因此进程退出后可用同一个数据库继续对账。
"""

from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import http.client
import json
import sqlite3
import threading
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path
from typing import Any, Callable, Iterable, Sequence


DECISION_ACCEPTED = "accepted"
DECISION_DUPLICATE = "duplicate"
DECISION_SAMPLED = "sampled"
DECISION_RATE_LIMITED = "rate_limited"
DECISION_REJECTED = "rejected"


class AdmissionDecision:
    """接纳结果的稳定字符串常量。"""

    ACCEPTED = DECISION_ACCEPTED
    DUPLICATE = DECISION_DUPLICATE
    SAMPLED = DECISION_SAMPLED
    RATE_LIMITED = DECISION_RATE_LIMITED
    REJECTED = DECISION_REJECTED

STATE_STORED = "stored"
STATE_SAMPLED = "sampled"
STATE_REJECTED = "rejected"
STATE_EVICTED = "evicted"
STATE_RECLAIMED = "reclaimed"

GAP_WAITING = "waiting"
GAP_MISSING = "missing"
GAP_SAMPLED = "sampled"
GAP_HEALED = "healed"


class AgentError(Exception):
    """代理用法或输入无效。"""


class CollectorUnavailable(Exception):
    """收集端未给出可判定响应；请求仍可能已经被持久接收。"""

    def __init__(self, message: str, *, maybe_received: bool = True):
        super().__init__(message)
        self.maybe_received = maybe_received


@dataclass(frozen=True)
class Policy:
    """可调整的本地策略。

    routine_sample_rate: 磁盘平稳时常规记录的稳定哈希保留概率，1.0 表示全留。
    tight_routine_sample_rate: 磁盘预算紧张时的常规记录保留概率，默认 0。
    gap_wait_ms: 已见 end 后，缺席分片等待迟到重放的宽限时间。
    low_headroom_units: 可用余量不高于该值时视为紧张。
    source_resume_watermark: 来源积压降到 reserve_units 的该比例以下才自动解除限流。
    max_batch_records: 单批最多携带的记录数。
    """

    routine_sample_rate: float = 1.0
    tight_routine_sample_rate: float = 0.0
    gap_wait_ms: int = 10_000
    low_headroom_units: int = 0
    source_resume_watermark: float = 0.5
    max_batch_records: int = 100
    auto_release_acked: bool = True
    backlog_high_watermark: float = 0.9
    max_pending_age_ms: int | None = 30_000
    collector_stall_ms: int | None = 30_000
    reconcile_before_eviction: bool = True

    def __post_init__(self) -> None:
        for name in ("routine_sample_rate", "tight_routine_sample_rate",
                     "backlog_high_watermark", "source_resume_watermark"):
            value = getattr(self, name)
            if not 0.0 <= value <= 1.0:
                raise AgentError(f"{name} 必须位于 [0, 1]")
        if self.gap_wait_ms < 0:
            raise AgentError("gap_wait_ms 不能为负")
        if self.max_batch_records <= 0:
            raise AgentError("max_batch_records 必须为正整数")
        if self.max_pending_age_ms is not None and self.max_pending_age_ms < 0:
            raise AgentError("max_pending_age_ms 不能为负")
        if self.collector_stall_ms is not None and self.collector_stall_ms < 0:
            raise AgentError("collector_stall_ms 不能为负")


@dataclass
class BatchAck:
    replied: bool
    accepted: list[str] = field(default_factory=list)
    http_status: int | None = None
    raw: Any = None


class HttpTransport:
    """标准库 HTTP 传输；断连/超时统一变成不确定结果，而不是“未收到”。"""

    def __init__(self, base_url: str, *, timeout: float = 2.0):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def post_batch(self, at_ms: int, records: Sequence[dict[str, Any]]) -> BatchAck:
        data = json.dumps({"at_ms": at_ms, "records": list(records)}).encode("utf-8")
        request = urllib.request.Request(
            self.base_url + "/batch",
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                payload = json.loads(response.read().decode("utf-8"))
                accepted = payload.get("accepted", [])
                if not isinstance(accepted, list) or not all(isinstance(x, str) for x in accepted):
                    raise CollectorUnavailable("收集端响应缺少 accepted 标识列表")
                return BatchAck(True, accepted, response.status, payload)
        except urllib.error.HTTPError as exc:
            # 合成收集端正常不会返回非 2xx；保留响应体以便调用方决策。
            try:
                payload = json.loads(exc.read().decode("utf-8"))
            except Exception:
                payload = None
            raise CollectorUnavailable(f"HTTP {exc.code}: {payload}", maybe_received=exc.code >= 500) from exc
        except (urllib.error.URLError, TimeoutError, http.client.HTTPException, OSError) as exc:
            raise CollectorUnavailable(f"收集端连接不确定：{exc}") from exc

    def get_received(self) -> list[str]:
        try:
            with urllib.request.urlopen(self.base_url + "/received", timeout=self.timeout) as response:
                payload = json.loads(response.read().decode("utf-8"))
            accepted = payload.get("accepted", [])
            if not isinstance(accepted, list) or not all(isinstance(x, str) for x in accepted):
                raise CollectorUnavailable("/received 返回格式无效", maybe_received=False)
            return accepted
        except (urllib.error.URLError, TimeoutError, http.client.HTTPException, OSError) as exc:
            raise CollectorUnavailable(f"无法查询收集端接收状态：{exc}", maybe_received=False) from exc


class ScriptedTransport:
    """按 endpoint.jsonl 的 max_accept/reply 规则在内存中模拟收集端。

    与 spool_peer.py 保持同一口径：已存在的稳定 ID 一定确认，且不占用新名额。
    """

    def __init__(self, endpoint_states: Iterable[dict[str, Any]] = ()):
        self.states = sorted(endpoint_states, key=lambda row: row["at_ms"])
        self.received: set[str] = set()
        self.posts: list[dict[str, Any]] = []

    def _state(self, at_ms: int) -> dict[str, Any]:
        eligible = [row for row in self.states if row["at_ms"] <= at_ms]
        if not eligible:
            return {"at_ms": at_ms, "max_accept": 10**18, "reply": True}
        return max(eligible, key=lambda row: row["at_ms"])

    def post_batch(self, at_ms: int, records: Sequence[dict[str, Any]]) -> BatchAck:
        state = self._state(at_ms)
        remaining = int(state["max_accept"])
        accepted: list[str] = []
        record_ids = [row["record_id"] for row in records]
        self.posts.append({"at_ms": at_ms, "record_ids": list(record_ids)})
        for record_id in record_ids:
            if record_id in self.received:
                accepted.append(record_id)
            elif remaining > 0:
                self.received.add(record_id)
                accepted.append(record_id)
                remaining -= 1
        if not state["reply"]:
            raise CollectorUnavailable("收集端持久接收后断开且未返回响应")
        return BatchAck(True, accepted, 200, {"accepted": accepted})

    def get_received(self) -> list[str]:
        return sorted(self.received)


def stable_batch_id(record_ids: Sequence[str]) -> str:
    material = "\n".join(record_ids).encode("utf-8")
    return "batch-" + hashlib.sha256(material).hexdigest()[:16]


class CollectionAgent:
    """所有状态以 SQLite 为准的采集代理。"""

    def __init__(
        self,
        db_path: str | Path = ":memory:",
        *,
        transport: Any | None = None,
        policy: Policy | None = None,
        routine_sampler: Callable[[dict[str, Any], dict[str, Any]], bool] | None = None,
        clock: Callable[[], int] | None = None,
    ) -> None:
        self.db_path = str(db_path)
        self.transport = transport
        self.policy = policy or Policy()
        self.routine_sampler = routine_sampler
        self.clock = clock or (lambda: int(time.time() * 1000))
        self._lock = threading.RLock()
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys=ON")
        self._init_schema()
        self._recover_interrupted_batches()

    # ---------- schema / lifecycle ----------

    def _init_schema(self) -> None:
        with self._lock, self.conn:
            self.conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS sources (
                    source TEXT PRIMARY KEY,
                    horizon_ms INTEGER NOT NULL,
                    protected_types TEXT NOT NULL,
                    reserve_units INTEGER NOT NULL,
                    routine_limited INTEGER NOT NULL DEFAULT 0,
                    limit_reason TEXT,
                    limited_at_ms INTEGER,
                    limited_until_ms INTEGER,
                    last_ack_ms INTEGER
                );

                CREATE TABLE IF NOT EXISTS source_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    at_ms INTEGER NOT NULL,
                    source TEXT NOT NULL,
                    event TEXT NOT NULL,
                    reason TEXT
                );

                CREATE TABLE IF NOT EXISTS disk_snapshots (
                    at_ms INTEGER PRIMARY KEY,
                    spool_limit INTEGER NOT NULL,
                    total_free INTEGER NOT NULL,
                    online_floor INTEGER NOT NULL,
                    evicted_json TEXT NOT NULL DEFAULT '[]'
                );

                CREATE TABLE IF NOT EXISTS records (
                    record_id TEXT PRIMARY KEY,
                    scope TEXT NOT NULL,
                    source TEXT NOT NULL,
                    trace TEXT NOT NULL,
                    part INTEGER NOT NULL,
                    kind TEXT NOT NULL,
                    size_units INTEGER NOT NULL,
                    first_seen_ms INTEGER NOT NULL,
                    last_seen_ms INTEGER NOT NULL,
                    received_at_ms INTEGER NOT NULL DEFAULT 0,
                    receive_count INTEGER NOT NULL DEFAULT 1,
                    payload_json TEXT NOT NULL,
                    protected INTEGER NOT NULL DEFAULT 0,
                    protection_reason TEXT,
                    admission TEXT NOT NULL,
                    admit_reason TEXT,
                    local_state TEXT NOT NULL,
                    reclaimed INTEGER NOT NULL DEFAULT 0,
                    collector_acked INTEGER NOT NULL DEFAULT 0,
                    acked_at_ms INTEGER,
                    gap_committed INTEGER NOT NULL DEFAULT 0,
                    gap_reason TEXT,
                    updated_at_ms INTEGER NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_records_scope_part
                    ON records(scope, part);
                CREATE INDEX IF NOT EXISTS idx_records_delivery
                    ON records(collector_acked, local_state, reclaimed);

                CREATE TABLE IF NOT EXISTS admissions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    at_ms INTEGER NOT NULL,
                    record_id TEXT NOT NULL,
                    scope TEXT NOT NULL,
                    part INTEGER NOT NULL,
                    kind TEXT NOT NULL,
                    size_units INTEGER NOT NULL,
                    decision TEXT NOT NULL,
                    reason TEXT,
                    disk_headroom_after INTEGER,
                    source_used_after INTEGER
                );

                CREATE TABLE IF NOT EXISTS gaps (
                    scope TEXT NOT NULL,
                    part INTEGER NOT NULL,
                    record_id TEXT,
                    status TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    first_seen_ms INTEGER NOT NULL,
                    deadline_ms INTEGER,
                    updated_at_ms INTEGER NOT NULL,
                    PRIMARY KEY(scope, part)
                );

                CREATE INDEX IF NOT EXISTS idx_gaps_status ON gaps(status);

                CREATE TABLE IF NOT EXISTS batches (
                    batch_id TEXT PRIMARY KEY,
                    requested_at_ms INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    reply_received INTEGER NOT NULL DEFAULT 0,
                    http_status INTEGER,
                    error TEXT,
                    created_at_ms INTEGER NOT NULL,
                    updated_at_ms INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS batch_items (
                    batch_id TEXT NOT NULL,
                    ordinal INTEGER NOT NULL,
                    record_id TEXT NOT NULL,
                    accepted INTEGER,
                    PRIMARY KEY(batch_id, record_id)
                );

                CREATE TABLE IF NOT EXISTS remote_accepted (
                    record_id TEXT PRIMARY KEY,
                    first_seen_remote_ms INTEGER NOT NULL
                );
                """
            )
            columns = {row["name"] for row in self.conn.execute("PRAGMA table_info(records)").fetchall()}
            if "received_at_ms" not in columns:
                self.conn.execute(
                    "ALTER TABLE records ADD COLUMN received_at_ms INTEGER NOT NULL DEFAULT 0"
                )

    def close(self) -> None:
        with self._lock:
            self.conn.close()

    def __enter__(self) -> "CollectionAgent":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def _recover_interrupted_batches(self) -> None:
        with self._lock, self.conn:
            self.conn.execute(
                "UPDATE batches SET status='indeterminate', updated_at_ms=? "
                "WHERE status='sending'",
                (self.clock(),),
            )

    # ---------- source / disk configuration ----------

    def register_source(
        self,
        source: str,
        *,
        horizon_ms: int = 0,
        protected_types: Sequence[str] = ("error", "end"),
        reserve_units: int = 10**18,
        now_ms: int | None = None,
    ) -> dict[str, Any]:
        self._require_nonempty(source, "source")
        self._require_nonnegative_int(horizon_ms, "horizon_ms")
        self._require_nonnegative_int(reserve_units, "reserve_units")
        types_json = json.dumps(sorted(set(protected_types)), ensure_ascii=False)
        now = self._time(now_ms)
        with self._lock, self.conn:
            self.conn.execute(
                """
                INSERT INTO sources(source, horizon_ms, protected_types, reserve_units)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(source) DO UPDATE SET
                    horizon_ms=excluded.horizon_ms,
                    protected_types=excluded.protected_types,
                    reserve_units=excluded.reserve_units
                """,
                (source, horizon_ms, types_json, reserve_units),
            )
        return self.source_status(source, now_ms=now)

    def load_sources(self, rows: Iterable[dict[str, Any]]) -> None:
        for row in rows:
            self.register_source(
                row["source"],
                horizon_ms=row["horizon_ms"],
                protected_types=row["protected_types"],
                reserve_units=row["reserve_units"],
                now_ms=row.get("at_ms"),
            )

    def limit_source(
        self,
        source: str,
        *,
        reason: str = "backlog",
        retry_after_ms: int | None = None,
        now_ms: int | None = None,
    ) -> dict[str, Any]:
        now = self._time(now_ms)
        until = None if retry_after_ms is None else now + int(retry_after_ms)
        with self._lock, self.conn:
            self._ensure_source_row(self.conn, source, now)
            self.conn.execute(
                """UPDATE sources SET routine_limited=1, limit_reason=?,
                   limited_at_ms=?, limited_until_ms=? WHERE source=?""",
                (reason, now, until, source),
            )
            self._event(self.conn, now, source, "limited", reason)
        return self.source_status(source, now_ms=now)

    def resume_source(self, source: str, *, reason: str = "recovered", now_ms: int | None = None) -> dict[str, Any]:
        now = self._time(now_ms)
        with self._lock, self.conn:
            self.conn.execute(
                """UPDATE sources SET routine_limited=0, limit_reason=NULL,
                   limited_at_ms=NULL, limited_until_ms=NULL WHERE source=?""",
                (source,),
            )
            self._event(self.conn, now, source, "resumed", reason)
        return self.source_status(source, now_ms=now)

    def apply_disk_snapshot(
        self,
        row: dict[str, Any] | None = None,
        *,
        at_ms: int | None = None,
        spool_limit: int | None = None,
        total_free: int | None = None,
        online_floor: int | None = None,
    ) -> dict[str, Any]:
        if row is not None:
            at_ms = row["at_ms"]
            spool_limit = row["spool_limit"]
            total_free = row["total_free"]
            online_floor = row["online_floor"]
        for name, value in (
            ("at_ms", at_ms),
            ("spool_limit", spool_limit),
            ("total_free", total_free),
            ("online_floor", online_floor),
        ):
            if value is None:
                raise AgentError(f"磁盘快照缺少 {name}")
            self._require_nonnegative_int(value, name)  # type: ignore[arg-type]
        assert at_ms is not None and spool_limit is not None
        assert total_free is not None and online_floor is not None
        with self._lock, self.conn:
            self._set_meta("disk_at_ms", str(at_ms))
            self._set_meta("spool_limit", str(spool_limit))
            # 快照是权威实际空闲量；快照之后的写入/回收再增量调整它。
            self._set_meta("free_units", str(total_free))
            self._set_meta("online_floor", str(online_floor))
            self.conn.execute(
                """INSERT OR REPLACE INTO disk_snapshots
                   (at_ms, spool_limit, total_free, online_floor)
                   VALUES (?, ?, ?, ?)""",
                (at_ms, spool_limit, total_free, online_floor),
            )
            capacity = self._ensure_capacity(self.conn, None, 0, protected_needed=True, now=at_ms)
            if capacity["evicted"]:
                self.conn.execute(
                    "UPDATE disk_snapshots SET evicted_json=? WHERE at_ms=?",
                    (json.dumps(capacity["evicted"], ensure_ascii=False), at_ms),
                )
        return self.disk_status(now_ms=at_ms)

    # ---------- ingestion / admission ----------

    def _reconcile_before_pressure_admission(self, normalized: dict[str, Any], now: int) -> None:
        """要淘汰证据前，先向收集端查询可能晚到的稳定确认。"""

        if not self.policy.reconcile_before_eviction or self.transport is None:
            return
        size = int(normalized["size_units"])
        with self._lock:
            disk = self._disk_view(self.conn)
            source_row = self.conn.execute(
                "SELECT reserve_units FROM sources WHERE source=?", (normalized["source"],)
            ).fetchone()
            reserve = int(source_row["reserve_units"]) if source_row else 10**18
            source_used = int(self.conn.execute(
                "SELECT COALESCE(SUM(size_units),0) n FROM records "
                "WHERE source=? AND local_state=? AND reclaimed=0",
                (normalized["source"], STATE_STORED),
            ).fetchone()["n"])
            disk_pressure = (
                disk["known"]
                and int(disk["admit_headroom_units"]) < size
            )
            source_pressure = source_used + size > reserve
        if not (disk_pressure or source_pressure):
            return
        try:
            self.reconcile_remote(now_ms=now, authoritative=True)
        except Exception:
            # 查询失败/传输实现不支持查询时，按本地已知确认状态处理；后续仍有显式淘汰/缺口兜底。
            pass

    def ingest(self, record: dict[str, Any], *, now_ms: int | None = None) -> dict[str, Any]:
        normalized = self._normalize_record(record, now_ms)
        record_id = normalized["record_id"]
        source = normalized["source"]
        at_ms = normalized["at_ms"]
        now = self._time(now_ms if now_ms is not None else at_ms)
        self._reconcile_before_pressure_admission(normalized, now)

        with self._lock, self.conn:
            self._ensure_source_row(self.conn, source, now)
            self._expire_source_limit(self.conn, source, now)
            self._evaluate_backpressure(self.conn, now)
            existing = self.conn.execute(
                "SELECT * FROM records WHERE record_id=?", (record_id,)
            ).fetchone()

            if existing is not None:
                conflict = self._immutable_conflict(existing, normalized)
                if conflict:
                    raise AgentError(f"稳定标识 {record_id} 的不可变字段冲突：{conflict}")
                # 已落地记录重放：绝不产生第二条证据。
                if existing["local_state"] == STATE_STORED or existing["collector_acked"]:
                    self.conn.execute(
                        """UPDATE records SET receive_count=receive_count+1,
                           last_seen_ms=?, updated_at_ms=? WHERE record_id=?""",
                        (now, now, record_id),
                    )
                    self.conn.execute(
                        """INSERT INTO admissions(at_ms, record_id, scope, part, kind,
                               size_units, decision, reason)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                        (now, record_id, normalized["scope"], normalized["part"],
                         normalized["kind"], normalized["size_units"],
                         DECISION_DUPLICATE, "stable_record_id"),
                    )
                    return self.admission_result(
                        DECISION_DUPLICATE, "stable_record_id", normalized, now,
                        extra={"original_decision": existing["admission"]},
                    )
                # 曾被拒收/淘汰的同一 ID 重放，视为一次新的接纳尝试；成功时愈合缺口。
                self.conn.execute(
                    "DELETE FROM records WHERE record_id=?", (record_id,)
                )

            source_state = self.source_status(source, now_ms=now)
            source_row = self.conn.execute("SELECT * FROM sources WHERE source=?", (source,)).fetchone()
            protected_types = set(json.loads(source_row["protected_types"]))
            scope_has_error = self._scope_has_error(self.conn, normalized["scope"])
            is_protected_type = normalized["kind"] in protected_types
            is_routine = normalized["kind"] == "routine" and not is_protected_type
            protected = is_protected_type or scope_has_error

            # 同一 scope/part 的稳定主键应是 record_id；不同 ID 不能覆盖已可用证据。
            same_part = self.conn.execute(
                "SELECT * FROM records WHERE scope=? AND part=? ORDER BY rowid LIMIT 1",
                (normalized["scope"], normalized["part"]),
            ).fetchone()
            if same_part is not None and same_part["record_id"] != record_id:
                part_available = bool(same_part["collector_acked"]) or (
                    same_part["local_state"] == STATE_STORED and not same_part["reclaimed"]
                )
                if part_available:
                    self.conn.execute(
                        """INSERT INTO admissions(at_ms, record_id, scope, part, kind,
                               size_units, decision, reason)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                        (now, record_id, normalized["scope"], normalized["part"],
                         normalized["kind"], normalized["size_units"],
                         DECISION_REJECTED, "conflicting_record_id_for_scope_part"),
                    )
                    return self.admission_result(
                        DECISION_REJECTED,
                        "conflicting_record_id_for_scope_part",
                        normalized,
                        now,
                        extra={"existing_record_id": same_part["record_id"]},
                    )
                # 旧片段只是失败/采样墓碑；允许新的稳定 ID 重放该 part。
                self.conn.execute(
                    "DELETE FROM records WHERE scope=? AND part=?",
                    (normalized["scope"], normalized["part"]),
                )

            remote_preknown = self.conn.execute(
                "SELECT 1 FROM remote_accepted WHERE record_id=?", (record_id,)
            ).fetchone()
            if remote_preknown is not None and existing is None:
                self._insert_remote_delivered_record(self.conn, normalized, now, protected=protected)
                self._heal_gap(self.conn, normalized["scope"], normalized["part"], now)
                self._sync_scope_gaps(self.conn, normalized["scope"], now)
                return self.admission_result(
                    DECISION_DUPLICATE,
                    "already_accepted_by_collector",
                    normalized,
                    now,
                    extra={"collector_acked": True},
                )

            if is_routine and source_state["routine_limited"]:
                return self._reject_record(
                    self.conn, normalized, now,
                    decision=DECISION_RATE_LIMITED,
                    reason=source_row["limit_reason"] or "source_rate_limited",
                    gap_reason="rate_limited",
                    protected=False,
                )

            disk = self._disk_view(self.conn)
            keep_routine = True
            if is_routine:
                keep_routine = self._should_keep_routine(normalized, disk, now)
            if is_routine and not keep_routine:
                return self._sample_record(self.conn, normalized, now)

            capacity = self._ensure_capacity(
                self.conn,
                source,
                normalized["size_units"],
                protected_needed=protected or not is_routine,
                now=now,
            )
            if not capacity["ok"]:
                if is_routine:
                    self._set_source_limited(
                        self.conn, source, now, reason="backlog_or_budget"
                    )
                return self._reject_record(
                    self.conn,
                    normalized,
                    now,
                    decision=DECISION_REJECTED,
                    reason=";".join(capacity["reasons"]) or "no_capacity",
                    gap_reason="no_capacity",
                    protected=protected,
                )

            self._insert_stored_record(self.conn, normalized, now, protected=protected)
            if normalized["kind"] == "error":
                promoted = self._promote_lookback(self.conn, normalized, source_row, now)
            else:
                promoted = []
            self._heal_gap(self.conn, normalized["scope"], normalized["part"], now)
            self._sync_scope_gaps(self.conn, normalized["scope"], now)
            result = self.admission_result(
                DECISION_ACCEPTED,
                "stored_durably",
                normalized,
                now,
                extra={"promoted_record_ids": promoted, "released": capacity["released"],
                       "evicted": capacity["evicted"]},
            )
            self.conn.execute(
                """INSERT INTO admissions(at_ms, record_id, scope, part, kind, size_units,
                       decision, reason, disk_headroom_after, source_used_after)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (now, record_id, normalized["scope"], normalized["part"], normalized["kind"],
                 normalized["size_units"], DECISION_ACCEPTED, "stored_durably",
                 result["disk"]["admit_headroom_units"] if result["disk"]["known"] else None,
                 result["source_state"]["used_units"]),
            )
            return result

    # Friendly aliases for callers/tests that use queue vocabulary.
    receive = ingest
    admit = ingest
    offer = ingest

    def admission_result(
        self,
        decision: str,
        reason: str,
        normalized: dict[str, Any],
        now: int,
        *,
        extra: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        source = normalized["source"]
        disk = self.disk_status(now_ms=now)
        source_status = self.source_status(source, now_ms=now)
        result = {
            "at_ms": now,
            "decision": decision,
            "admitted": decision in (DECISION_ACCEPTED, DECISION_DUPLICATE),
            "record_id": normalized["record_id"],
            "scope": normalized["scope"],
            "source": source,
            "trace": normalized["trace"],
            "part": normalized["part"],
            "kind": normalized["kind"],
            "reason": reason,
            "disk": disk,
            "source_state": source_status,
        }
        if extra:
            result.update(extra)
        return result

    # ---------- normalization / validation ----------

    def _normalize_record(self, record: dict[str, Any], fallback_now: int | None) -> dict[str, Any]:
        if not isinstance(record, dict):
            raise AgentError("记录必须是 JSON 对象")
        record_id = record.get("record_id") or record.get("id")
        source = record.get("source")
        trace = record.get("trace") or record.get("scope_id")
        part = record.get("part")
        kind = record.get("kind")
        size = record.get("size_units", record.get("size", 0))
        at_ms = record.get("at_ms", fallback_now if fallback_now is not None else self.clock())
        self._require_nonempty(record_id, "record_id")
        self._require_nonempty(source, "source")
        self._require_nonempty(trace, "trace")
        self._require_nonempty(kind, "kind")
        self._require_positive_int(part, "part")
        self._require_nonnegative_int(size, "size_units")
        self._require_nonnegative_int(at_ms, "at_ms")
        assert isinstance(record_id, str) and isinstance(source, str)
        assert isinstance(trace, str) and isinstance(kind, str)
        return {
            "record_id": record_id,
            "source": source,
            "trace": trace,
            "scope": record.get("scope") if isinstance(record.get("scope"), str) and "/" in record["scope"]
                     else f"{source}/{trace}",
            "part": int(part),
            "kind": kind,
            "size_units": int(size),
            "at_ms": int(at_ms),
            "payload": dict(record),
        }

    @staticmethod
    def _require_nonempty(value: Any, name: str) -> None:
        if not isinstance(value, str) or not value:
            raise AgentError(f"{name} 必须是非空字符串")

    @staticmethod
    def _require_positive_int(value: Any, name: str) -> None:
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise AgentError(f"{name} 必须是正整数")

    @staticmethod
    def _require_nonnegative_int(value: Any, name: str) -> None:
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise AgentError(f"{name} 必须是非负整数")

    @staticmethod
    def _immutable_conflict(row: sqlite3.Row, normalized: dict[str, Any]) -> str | None:
        for field_name in ("source", "trace", "scope", "part", "kind", "size_units"):
            if row[field_name] != normalized[field_name]:
                return field_name
        return None

    def _time(self, value: int | None) -> int:
        return self.clock() if value is None else value

    # ---------- source/disk views ----------

    def _set_meta(self, key: str, value: str) -> None:
        self.conn.execute(
            "INSERT INTO meta(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )

    def _meta(self, key: str) -> str | None:
        row = self.conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return None if row is None else row["value"]

    def _ensure_source_row(self, conn: sqlite3.Connection, source: str, now: int) -> None:
        row = conn.execute("SELECT 1 FROM sources WHERE source=?", (source,)).fetchone()
        if row is None:
            conn.execute(
                """INSERT INTO sources(source, horizon_ms, protected_types, reserve_units)
                   VALUES (?, 0, ?, ?)""",
                (source, json.dumps(["error", "end"]), 10**18),
            )
            self._event(conn, now, source, "registered_default", "unknown_source_policy")

    def _event(self, conn: sqlite3.Connection, at_ms: int, source: str, event: str, reason: str | None) -> None:
        conn.execute(
            "INSERT INTO source_events(at_ms, source, event, reason) VALUES (?, ?, ?, ?)",
            (at_ms, source, event, reason),
        )

    def _expire_source_limit(self, conn: sqlite3.Connection, source: str, now: int) -> None:
        row = conn.execute("SELECT * FROM sources WHERE source=?", (source,)).fetchone()
        if row and row["routine_limited"] and row["limited_until_ms"] is not None:
            if int(row["limited_until_ms"]) <= now:
                conn.execute(
                    """UPDATE sources SET routine_limited=0, limit_reason=NULL,
                       limited_at_ms=NULL, limited_until_ms=NULL WHERE source=?""",
                    (source,),
                )
                self._event(conn, now, source, "resumed", "backoff_elapsed")

    def _set_source_limited(self, conn: sqlite3.Connection, source: str, now: int, *, reason: str) -> None:
        row = conn.execute("SELECT * FROM sources WHERE source=?", (source,)).fetchone()
        if not row["routine_limited"]:
            conn.execute(
                """UPDATE sources SET routine_limited=1, limit_reason=?, limited_at_ms=?
                   WHERE source=?""",
                (reason, now, source),
            )
            self._event(conn, now, source, "limited", reason)

    def _note_collector_success(self, conn: sqlite3.Connection, now: int) -> None:
        self._set_meta("last_collector_success_ms", str(now))
        self._set_meta("collector_unavailable_since_ms", "")
        self._set_meta("collector_failures", "0")

    def _note_collector_failure(self, conn: sqlite3.Connection, now: int) -> None:
        since_raw = self._meta("collector_unavailable_since_ms")
        if not since_raw:
            self._set_meta("collector_unavailable_since_ms", str(now))
        failures = int(self._meta("collector_failures") or "0") + 1
        self._set_meta("collector_failures", str(failures))
        self._set_meta("last_collector_failure_ms", str(now))

    def collector_health(self, *, now_ms: int | None = None) -> dict[str, Any]:
        now = self._time(now_ms)
        with self._lock:
            last_success_raw = self._meta("last_collector_success_ms")
            since_raw = self._meta("collector_unavailable_since_ms")
            last_failure_raw = self._meta("last_collector_failure_ms")
            last_success = int(last_success_raw) if last_success_raw else None
            unavailable_since = int(since_raw) if since_raw else None
            stalled_for = None
            if unavailable_since is not None:
                stalled_for = max(0, now - unavailable_since)
            elif last_success is not None:
                stalled_for = max(0, now - last_success)
            stalled = (
                self.policy.collector_stall_ms is not None
                and stalled_for is not None
                and stalled_for >= self.policy.collector_stall_ms
            )
            return {
                "last_success_ms": last_success,
                "last_failure_ms": int(last_failure_raw) if last_failure_raw else None,
                "unavailable_since_ms": unavailable_since,
                "stalled_for_ms": stalled_for,
                "stall_threshold_ms": self.policy.collector_stall_ms,
                "stalled": stalled,
                "consecutive_failures": int(self._meta("collector_failures") or "0"),
                "transport_configured": self.transport is not None,
            }

    def evaluate_backpressure(self, *, now_ms: int | None = None) -> dict[str, Any]:
        now = self._time(now_ms)
        with self._lock, self.conn:
            limited = self._evaluate_backpressure(self.conn, now)
            self.conn.commit()
            return {"at_ms": now, "limited_sources": limited,
                    "collector": self.collector_health(now_ms=now)}

    def _evaluate_backpressure(self, conn: sqlite3.Connection, now: int) -> list[str]:
        health = self.collector_health(now_ms=now)
        limited_sources: list[str] = []
        rows = conn.execute("SELECT source, reserve_units FROM sources").fetchall()
        for row in rows:
            source = row["source"]
            reserve = int(row["reserve_units"])
            agg = conn.execute(
                """SELECT
                     COALESCE(SUM(size_units),0) AS pending_units,
                     COUNT(*) AS pending_count,
                     MIN(received_at_ms) AS oldest_pending_ms
                   FROM records
                   WHERE source=? AND collector_acked=0 AND reclaimed=0
                     AND local_state=?""",
                (source, STATE_STORED),
            ).fetchone()
            pending_units = int(agg["pending_units"])
            pending_count = int(agg["pending_count"])
            oldest = agg["oldest_pending_ms"]
            if pending_count == 0:
                continue
            over_watermark = (
                reserve < 10**17
                and pending_units >= reserve * self.policy.backlog_high_watermark
            )
            aged = (
                self.policy.max_pending_age_ms is not None
                and oldest is not None
                and now - int(oldest) >= self.policy.max_pending_age_ms
            )
            if over_watermark or (aged and health["stalled"]):
                reason = "source_backlog" if over_watermark else "collector_stall_backlog"
                self._set_source_limited(conn, source, now, reason=reason)
                limited_sources.append(source)
        return limited_sources

    def _maybe_auto_resume(
        self, conn: sqlite3.Connection, row: sqlite3.Row, pending_units: int, now: int
    ) -> sqlite3.Row:
        """在当前事务内按低水位解除限流，避免嵌套事务提前提交。"""

        if not row["routine_limited"] or row["limited_until_ms"] is not None:
            return row
        if row["last_ack_ms"] is None:
            return row
        reserve = int(row["reserve_units"])
        if pending_units <= reserve * self.policy.source_resume_watermark:
            conn.execute(
                """UPDATE sources SET routine_limited=0, limit_reason=NULL,
                   limited_at_ms=NULL, limited_until_ms=NULL WHERE source=?""",
                (row["source"],),
            )
            self._event(conn, now, row["source"], "resumed", "backlog_drained")
            return conn.execute("SELECT * FROM sources WHERE source=?", (row["source"],)).fetchone()
        return row

    def source_status(self, source: str, *, now_ms: int | None = None) -> dict[str, Any]:
        now = self._time(now_ms)
        with self._lock:
            self._expire_source_limit(self.conn, source, now)
            row = self.conn.execute("SELECT * FROM sources WHERE source=?", (source,)).fetchone()
            if row is None:
                raise AgentError(f"未知来源：{source}")
            agg = self.conn.execute(
                """SELECT
                     COALESCE(SUM(CASE WHEN reclaimed=0 AND local_state=? THEN size_units ELSE 0 END),0)
                       AS used_units,
                     COALESCE(SUM(CASE WHEN collector_acked=0 AND reclaimed=0 AND local_state=?
                       THEN size_units ELSE 0 END),0) AS pending_units,
                     COUNT(CASE WHEN collector_acked=0 AND reclaimed=0 AND local_state=? THEN 1 END)
                       AS pending_count,
                     MIN(CASE WHEN collector_acked=0 AND reclaimed=0 AND local_state=?
                       THEN received_at_ms END) AS oldest_pending_ms
                   FROM records WHERE source=?""",
                (STATE_STORED, STATE_STORED, STATE_STORED, STATE_STORED, source),
            ).fetchone()
            reserve = int(row["reserve_units"])
            pending_units = int(agg["pending_units"])
            row = self._maybe_auto_resume(self.conn, row, pending_units, now)
            limited = bool(row["routine_limited"])
            return {
                "source": source,
                "horizon_ms": int(row["horizon_ms"]),
                "protected_types": json.loads(row["protected_types"]),
                "reserve_units": reserve,
                "used_units": int(agg["used_units"]),
                "pending_units": pending_units,
                "pending_count": int(agg["pending_count"]),
                "oldest_pending_ms": agg["oldest_pending_ms"],
                "routine_admission": "rate_limited" if limited else "open",
                "routine_limited": limited,
                "limit_reason": row["limit_reason"],
                "limited_at_ms": row["limited_at_ms"],
                "limited_until_ms": row["limited_until_ms"],
                "last_ack_ms": row["last_ack_ms"],
            }

    def _disk_view(self, conn: sqlite3.Connection) -> dict[str, Any]:
        spooled = int(conn.execute(
            "SELECT COALESCE(SUM(size_units),0) AS n FROM records "
            "WHERE local_state=? AND reclaimed=0", (STATE_STORED,)
        ).fetchone()["n"])
        limit_raw = self._meta("spool_limit")
        free_raw = self._meta("free_units")
        floor_raw = self._meta("online_floor")
        at_raw = self._meta("disk_at_ms")
        if limit_raw is None or free_raw is None or floor_raw is None:
            return {
                "known": False,
                "at_ms": None,
                "spooled_units": spooled,
                "spool_limit": None,
                "free_units": None,
                "online_floor": None,
                "free_headroom_units": None,
                "spool_headroom_units": None,
                "admit_headroom_units": None,
                "pressure": "unknown",
            }
        limit = int(limit_raw)
        free = int(free_raw)
        floor = int(floor_raw)
        free_headroom = free - floor
        spool_headroom = limit - spooled
        admit_headroom = min(free_headroom, spool_headroom)
        if admit_headroom < 0:
            pressure = "critical"
        elif admit_headroom <= self.policy.low_headroom_units:
            pressure = "tight"
        else:
            pressure = "calm"
        return {
            "known": True,
            "at_ms": int(at_raw) if at_raw is not None else None,
            "spooled_units": spooled,
            "spool_limit": limit,
            "free_units": free,
            "online_floor": floor,
            "free_headroom_units": free_headroom,
            "spool_headroom_units": spool_headroom,
            "admit_headroom_units": admit_headroom,
            "pressure": pressure,
        }

    def disk_status(self, *, now_ms: int | None = None) -> dict[str, Any]:
        with self._lock:
            return self._disk_view(self.conn)

    # ---------- sampling / eviction ----------

    def _should_keep_routine(self, normalized: dict[str, Any], disk: dict[str, Any], now: int) -> bool:
        if self.routine_sampler is not None:
            return bool(self.routine_sampler(normalized, disk))
        rate = self.policy.routine_sample_rate
        if disk["known"] and disk["pressure"] in ("tight", "critical"):
            rate = self.policy.tight_routine_sample_rate
        if rate >= 1.0:
            return True
        if rate <= 0.0:
            return False
        digest = hashlib.sha256(normalized["record_id"].encode("utf-8")).hexdigest()
        point = int(digest[:16], 16) / float(16**16)
        return point < rate

    def _release_acked(self, conn: sqlite3.Connection, source: str | None = None) -> list[dict[str, Any]]:
        sql = """SELECT record_id, source, size_units FROM records
                 WHERE collector_acked=1 AND reclaimed=0 AND local_state=?"""
        params: list[Any] = [STATE_STORED]
        if source is not None:
            sql += " AND source=?"
            params.append(source)
        rows = conn.execute(sql, params).fetchall()
        released: list[dict[str, Any]] = []
        free_raw = self._meta("free_units")
        free = int(free_raw) if free_raw is not None else None
        for row in rows:
            conn.execute(
                "UPDATE records SET reclaimed=1, local_state=?, updated_at_ms=? WHERE record_id=?",
                (STATE_RECLAIMED, self.clock(), row["record_id"]),
            )
            if free is not None:
                free += int(row["size_units"])
            released.append({"record_id": row["record_id"], "source": row["source"],
                             "size_units": int(row["size_units"]), "reason": "collector_acked"})
        if free is not None:
            self._set_meta("free_units", str(free))
        return released

    def _evict_candidates(
        self,
        conn: sqlite3.Connection,
        need_units: int,
        *,
        source: str | None,
        now: int,
        reason: str,
        include_protected: bool,
    ) -> list[dict[str, Any]]:
        if need_units <= 0:
            return []
        sql = """
            SELECT r.* FROM records r
            WHERE r.local_state=? AND r.reclaimed=0 AND r.collector_acked=0
              AND r.record_id NOT IN (
                  SELECT bi.record_id FROM batch_items bi
                  JOIN batches b ON b.batch_id=bi.batch_id
                  WHERE b.status='sending'
              )
        """
        params: list[Any] = [STATE_STORED]
        if source is not None:
            sql += " AND r.source=?"
            params.append(source)
        if not include_protected:
            sql += " AND r.protected=0 AND r.kind='routine'"
        sql += """
            ORDER BY r.protected ASC,
              CASE WHEN r.kind='routine' THEN 0
                   WHEN r.kind='detail' THEN 1
                   WHEN r.kind='end' THEN 3
                   WHEN r.kind='error' THEN 4
                   ELSE 2 END ASC,
              r.first_seen_ms ASC,
              r.rowid ASC
        """
        rows = conn.execute(sql, params).fetchall()
        free_raw = self._meta("free_units")
        free = int(free_raw) if free_raw is not None else None
        evicted: list[dict[str, Any]] = []
        freed = 0
        for row in rows:
            if freed >= need_units:
                break
            size = int(row["size_units"])
            conn.execute(
                """UPDATE records SET local_state=?, reclaimed=1, gap_committed=1,
                       gap_reason=?, protected=protected, updated_at_ms=?
                   WHERE record_id=?""",
                (STATE_EVICTED, reason, now, row["record_id"]),
            )
            conn.execute(
                """INSERT INTO gaps(scope, part, record_id, status, reason,
                       first_seen_ms, updated_at_ms)
                   VALUES (?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(scope, part) DO UPDATE SET
                       record_id=excluded.record_id,
                       status=?, reason=excluded.reason, updated_at_ms=excluded.updated_at_ms,
                       deadline_ms=NULL""",
                (row["scope"], row["part"], row["record_id"], GAP_MISSING, reason,
                 now, now, GAP_MISSING),
            )
            if free is not None:
                free += size
            freed += size
            evicted.append({"record_id": row["record_id"], "scope": row["scope"],
                            "part": int(row["part"]), "size_units": size, "reason": reason})
        if free is not None:
            self._set_meta("free_units", str(free))
        return evicted

    def _ensure_capacity(
        self,
        conn: sqlite3.Connection,
        source: str | None,
        size: int,
        *,
        protected_needed: bool,
        now: int | None = None,
    ) -> dict[str, Any]:
        now = self._time(now)
        released = (
            self._release_acked(conn)
            if self.policy.auto_release_acked
            else []
        )
        evicted: list[dict[str, Any]] = []
        reasons: list[str] = []

        # 全机磁盘/在线空间约束优先。这里的回收即使最终仍无法接纳新记录也保留，
        # 因为释放空间本身就是磁盘紧张时需要执行的动作。
        disk = self._disk_view(conn)
        global_need = 0
        if disk["known"]:
            assert disk["admit_headroom_units"] is not None
            global_need = max(0, size - int(disk["admit_headroom_units"]))
        if global_need > 0:
            evicted.extend(self._evict_candidates(
                conn, global_need, source=None, now=now,
                reason="disk_or_online_floor", include_protected=True,
            ))

        disk = self._disk_view(conn)
        disk_ok = True
        if disk["known"]:
            assert disk["admit_headroom_units"] is not None
            if int(disk["admit_headroom_units"]) < size:
                disk_ok = False
                reasons.append("disk_or_online_floor_full")

        # 来源预算是第二道约束。若尝试淘汰后仍放不下这条新记录，回滚这些
        # 来源内淘汰，避免为了一次最终拒绝而白白把旧证据改成缺口。
        source_ok = True
        if source is not None and disk_ok:
            row = conn.execute("SELECT reserve_units FROM sources WHERE source=?", (source,)).fetchone()
            reserve = int(row["reserve_units"]) if row else 10**18
            used = int(conn.execute(
                "SELECT COALESCE(SUM(size_units),0) AS n FROM records "
                "WHERE source=? AND local_state=? AND reclaimed=0",
                (source, STATE_STORED),
            ).fetchone()["n"])
            source_need = max(0, used + size - reserve)
            if source_need > 0:
                savepoint = "source_capacity_attempt"
                conn.execute(f"SAVEPOINT {savepoint}")
                source_evicted = self._evict_candidates(
                    conn, source_need, source=source, now=now,
                    reason="source_reserve", include_protected=protected_needed,
                )
                used_after = int(conn.execute(
                    "SELECT COALESCE(SUM(size_units),0) AS n FROM records "
                    "WHERE source=? AND local_state=? AND reclaimed=0",
                    (source, STATE_STORED),
                ).fetchone()["n"])
                if used_after + size <= reserve:
                    conn.execute(f"RELEASE SAVEPOINT {savepoint}")
                    evicted.extend(source_evicted)
                else:
                    conn.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
                    conn.execute(f"RELEASE SAVEPOINT {savepoint}")
                    source_ok = False
                    reasons.append("source_reserve_exceeded")

        return {"ok": disk_ok and source_ok, "reasons": reasons,
                "released": released, "evicted": evicted}

    def _insert_stored_record(
        self, conn: sqlite3.Connection, normalized: dict[str, Any], now: int, *, protected: bool
    ) -> None:
        disk = self._disk_view(conn)
        free = disk["free_units"]
        if free is not None:
            self._set_meta("free_units", str(int(free) - normalized["size_units"]))
        conn.execute(
            """INSERT INTO records(record_id, scope, source, trace, part, kind, size_units,
                   first_seen_ms, last_seen_ms, received_at_ms, receive_count, payload_json,
                   protected, protection_reason, admission, admit_reason, local_state,
                   updated_at_ms)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?)""",
            (
                normalized["record_id"], normalized["scope"], normalized["source"],
                normalized["trace"], normalized["part"], normalized["kind"],
                normalized["size_units"], normalized["at_ms"], now, now,
                json.dumps(normalized["payload"], ensure_ascii=False, sort_keys=True),
                1 if protected else 0,
                "protected_type_or_error_scope" if protected else "provisional_lookback",
                DECISION_ACCEPTED, "stored_durably", STATE_STORED, now,
            ),
        )

    def _insert_remote_delivered_record(
        self, conn: sqlite3.Connection, normalized: dict[str, Any], now: int, *, protected: bool
    ) -> None:
        """收集端已持久同一稳定 ID 时，只保留元数据，不再占用本地配额。"""

        conn.execute(
            """INSERT INTO records(record_id, scope, source, trace, part, kind, size_units,
                   first_seen_ms, last_seen_ms, received_at_ms, receive_count, payload_json,
                   protected, protection_reason, admission, admit_reason, local_state,
                   reclaimed, collector_acked, acked_at_ms, updated_at_ms)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, 1, 1, ?, ?)""",
            (
                normalized["record_id"], normalized["scope"], normalized["source"],
                normalized["trace"], normalized["part"], normalized["kind"],
                normalized["size_units"], normalized["at_ms"], now, now,
                json.dumps(normalized["payload"], ensure_ascii=False, sort_keys=True),
                1 if protected else 0,
                "remote_accepted" if protected else "remote_accepted",
                DECISION_DUPLICATE, "already_accepted_by_collector", STATE_RECLAIMED,
                now, now,
            ),
        )
        conn.execute(
            "UPDATE sources SET last_ack_ms=? WHERE source=?", (now, normalized["source"])
        )

    def _sample_record(self, conn: sqlite3.Connection, normalized: dict[str, Any], now: int) -> dict[str, Any]:
        conn.execute(
            """INSERT INTO records(record_id, scope, source, trace, part, kind, size_units,
                   first_seen_ms, last_seen_ms, payload_json, protected, admission,
                   admit_reason, local_state, updated_at_ms)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?)""",
            (
                normalized["record_id"], normalized["scope"], normalized["source"],
                normalized["trace"], normalized["part"], normalized["kind"],
                normalized["size_units"], normalized["at_ms"], now,
                json.dumps(normalized["payload"], ensure_ascii=False, sort_keys=True),
                DECISION_SAMPLED, "routine_sampling_policy", STATE_SAMPLED, now,
            ),
        )
        conn.execute(
            """INSERT INTO gaps(scope, part, record_id, status, reason,
                   first_seen_ms, updated_at_ms)
               VALUES (?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(scope, part) DO UPDATE SET
                 record_id=excluded.record_id, status=?, reason=excluded.reason,
                 updated_at_ms=excluded.updated_at_ms, deadline_ms=NULL""",
            (normalized["scope"], normalized["part"], normalized["record_id"],
             GAP_SAMPLED, "routine_sampled", now, now, GAP_SAMPLED),
        )
        conn.execute(
            """INSERT INTO admissions(at_ms, record_id, scope, part, kind, size_units,
                   decision, reason) VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (now, normalized["record_id"], normalized["scope"], normalized["part"],
             normalized["kind"], normalized["size_units"], DECISION_SAMPLED,
             "routine_sampling_policy"),
        )
        self._sync_scope_gaps(conn, normalized["scope"], now)
        return self.admission_result(
            DECISION_SAMPLED, "routine_sampling_policy", normalized, now,
            extra={"gap_status": GAP_SAMPLED},
        )

    def _reject_record(
        self,
        conn: sqlite3.Connection,
        normalized: dict[str, Any],
        now: int,
        *,
        decision: str,
        reason: str,
        gap_reason: str,
        protected: bool,
    ) -> dict[str, Any]:
        conn.execute(
            """INSERT INTO records(record_id, scope, source, trace, part, kind, size_units,
                   first_seen_ms, last_seen_ms, payload_json, protected, admission,
                   admit_reason, local_state, gap_committed, gap_reason, updated_at_ms)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)""",
            (
                normalized["record_id"], normalized["scope"], normalized["source"],
                normalized["trace"], normalized["part"], normalized["kind"],
                normalized["size_units"], normalized["at_ms"], now,
                json.dumps(normalized["payload"], ensure_ascii=False, sort_keys=True),
                1 if protected else 0, decision, reason, STATE_REJECTED,
                gap_reason, now,
            ),
        )
        conn.execute(
            """INSERT INTO gaps(scope, part, record_id, status, reason,
                   first_seen_ms, updated_at_ms)
               VALUES (?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(scope, part) DO UPDATE SET
                 record_id=excluded.record_id, status=?, reason=excluded.reason,
                 updated_at_ms=excluded.updated_at_ms, deadline_ms=NULL""",
            (normalized["scope"], normalized["part"], normalized["record_id"],
             GAP_MISSING, gap_reason, now, now, GAP_MISSING),
        )
        conn.execute(
            """INSERT INTO admissions(at_ms, record_id, scope, part, kind, size_units,
                   decision, reason) VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (now, normalized["record_id"], normalized["scope"], normalized["part"],
             normalized["kind"], normalized["size_units"], decision, reason),
        )
        self._sync_scope_gaps(conn, normalized["scope"], now)
        return self.admission_result(
            decision, reason, normalized, now, extra={"gap_status": GAP_MISSING}
        )

    # ---------- lookback / scopes / gaps ----------

    def _scope_has_error(self, conn: sqlite3.Connection, scope: str) -> bool:
        return conn.execute(
            "SELECT 1 FROM records WHERE scope=? AND kind='error' LIMIT 1", (scope,)
        ).fetchone() is not None

    def _promote_lookback(
        self, conn: sqlite3.Connection, error: dict[str, Any], source_row: sqlite3.Row, now: int
    ) -> list[str]:
        horizon = int(source_row["horizon_ms"])
        cutoff = int(error["at_ms"]) - horizon
        rows = conn.execute(
            """SELECT record_id FROM records
               WHERE scope=? AND kind != 'routine' AND protected=0
                 AND first_seen_ms <= ? AND first_seen_ms >= ? AND local_state=?
               ORDER BY part""",
            (error["scope"], error["at_ms"], cutoff, STATE_STORED),
        ).fetchall()
        promoted = [row["record_id"] for row in rows]
        if promoted:
            conn.execute(
                f"UPDATE records SET protected=1, protection_reason='error_lookback', "
                f"updated_at_ms=? WHERE record_id IN ({','.join('?' for _ in promoted)})",
                [now, *promoted],
            )
        return promoted

    def _heal_gap(self, conn: sqlite3.Connection, scope: str, part: int, now: int) -> None:
        conn.execute(
            """UPDATE gaps SET status=?, reason='filled_by_replay', updated_at_ms=?,
                   deadline_ms=NULL WHERE scope=? AND part=? AND status != ?""",
            (GAP_HEALED, now, scope, part, GAP_HEALED),
        )

    def _sync_scope_gaps(self, conn: sqlite3.Connection, scope: str, now: int) -> None:
        end = conn.execute(
            """SELECT part, first_seen_ms FROM records
               WHERE scope=? AND kind='end' ORDER BY part DESC LIMIT 1""",
            (scope,),
        ).fetchone()
        if end is None:
            return
        end_part = int(end["part"])
        rows = {
            int(row["part"]): row
            for row in conn.execute("SELECT * FROM records WHERE scope=?", (scope,)).fetchall()
        }
        end_ms = int(end["first_seen_ms"])  # noqa: F841 (event anchor retained for debugging/SQL)
        # 宽限从代理观察到 end 并同步缺口的当前时间起算；调用方可以用 now_ms 推进演练时钟。
        deadline = now + self.policy.gap_wait_ms
        for part in range(1, end_part + 1):
            row = rows.get(part)
            existing = conn.execute(
                "SELECT * FROM gaps WHERE scope=? AND part=?", (scope, part)
            ).fetchone()
            if row is None:
                if existing is None or existing["status"] == GAP_HEALED:
                    conn.execute(
                        """INSERT INTO gaps(scope, part, status, reason, first_seen_ms,
                               deadline_ms, updated_at_ms)
                           VALUES (?, ?, ?, 'late_part', ?, ?, ?)
                           ON CONFLICT(scope, part) DO UPDATE SET
                               status='waiting', reason='late_part',
                               deadline_ms=excluded.deadline_ms,
                               updated_at_ms=excluded.updated_at_ms""",
                        (scope, part, GAP_WAITING, now, deadline, now),
                    )
                elif existing["status"] == GAP_WAITING and existing["deadline_ms"] is None:
                    conn.execute(
                        "UPDATE gaps SET deadline_ms=? WHERE scope=? AND part=?",
                        (deadline, scope, part),
                    )
            else:
                available = bool(row["collector_acked"]) or (
                    row["local_state"] == STATE_STORED and not row["reclaimed"]
                )
                if available and existing is not None and existing["status"] != GAP_HEALED:
                    self._heal_gap(conn, scope, part, now)
                elif not available and existing is None:
                    if row["local_state"] == STATE_SAMPLED:
                        status, reason = GAP_SAMPLED, "routine_sampled"
                    else:
                        status, reason = GAP_MISSING, row["gap_reason"] or row["local_state"]
                    conn.execute(
                        """INSERT INTO gaps(scope, part, record_id, status, reason,
                               first_seen_ms, deadline_ms, updated_at_ms)
                           VALUES (?, ?, ?, ?, ?, ?, NULL, ?)""",
                        (scope, part, row["record_id"], status, reason, now, now),
                    )

    def _expire_gaps(self, conn: sqlite3.Connection, now: int) -> None:
        conn.execute(
            """UPDATE gaps SET status=?, reason='late_timeout', updated_at_ms=?
               WHERE status=? AND deadline_ms IS NOT NULL AND deadline_ms <= ?""",
            (GAP_MISSING, now, GAP_WAITING, now),
        )

    def gap_report(self, *, now_ms: int | None = None, include_healed: bool = False) -> dict[str, Any]:
        now = self._time(now_ms)
        with self._lock, self.conn:
            for row in self.conn.execute("SELECT DISTINCT scope FROM records").fetchall():
                self._sync_scope_gaps(self.conn, row["scope"], now)
            self._expire_gaps(self.conn, now)
            where = "" if include_healed else "WHERE status != ?"
            params: list[Any] = [] if include_healed else [GAP_HEALED]
            rows = self.conn.execute(
                f"SELECT * FROM gaps {where} ORDER BY scope, part", params
            ).fetchall()
            by_scope: dict[str, list[sqlite3.Row]] = {}
            for row in rows:
                by_scope.setdefault(row["scope"], []).append(row)
            scopes = []
            for scope, gap_rows in sorted(by_scope.items()):
                record_row = self.conn.execute(
                    "SELECT source, trace FROM records WHERE scope=? LIMIT 1", (scope,)
                ).fetchone()
                ranges = self._ranges_for_gaps(self.conn, scope, gap_rows)
                scopes.append({
                    "scope": scope,
                    "source": record_row["source"] if record_row else scope.split("/", 1)[0],
                    "trace": record_row["trace"] if record_row else scope.split("/", 1)[-1],
                    "gaps": ranges,
                    "missing_parts": [int(r["part"]) for r in gap_rows if r["status"] == GAP_MISSING],
                    "waiting_parts": [int(r["part"]) for r in gap_rows if r["status"] == GAP_WAITING],
                    "sampled_parts": [int(r["part"]) for r in gap_rows if r["status"] == GAP_SAMPLED],
                })
            actual_missing = [
                item for scope in scopes for item in scope["gaps"] if item["status"] == GAP_MISSING
            ]
            return {"at_ms": now, "scopes": scopes, "actual_missing": actual_missing}

    def _ranges_for_gaps(
        self, conn: sqlite3.Connection, scope: str, gap_rows: list[sqlite3.Row]
    ) -> list[dict[str, Any]]:
        # 连续 part 且状态/原因相同的合并；边界取两侧最近的已知 part。
        sorted_rows = sorted(gap_rows, key=lambda r: int(r["part"]))
        ranges: list[dict[str, Any]] = []
        current: list[sqlite3.Row] = []

        def flush() -> None:
            if not current:
                return
            first = int(current[0]["part"])
            last = int(current[-1]["part"])
            status = current[0]["status"]
            reason = current[0]["reason"]
            before = conn.execute(
                "SELECT MAX(part) AS p FROM records WHERE scope=? AND part < ?",
                (scope, first),
            ).fetchone()["p"]
            after = conn.execute(
                "SELECT MIN(part) AS p FROM records WHERE scope=? AND part > ?",
                (scope, last),
            ).fetchone()["p"]
            ranges.append({
                "scope": scope,
                "from_part": first,
                "to_part": last,
                "status": status,
                "reason": reason,
                "before_part": before,
                "after_part": after,
                "record_ids": [r["record_id"] for r in current if r["record_id"]],
            })

        for row in sorted_rows:
            if current and (
                int(row["part"]) != int(current[-1]["part"]) + 1
                or row["status"] != current[-1]["status"]
                or row["reason"] != current[-1]["reason"]
            ):
                flush()
                current = []
            current.append(row)
        flush()
        return ranges

    def scope_status(self, scope: str, *, now_ms: int | None = None) -> dict[str, Any]:
        now = self._time(now_ms)
        with self._lock, self.conn:
            self._sync_scope_gaps(self.conn, scope, now)
            self._expire_gaps(self.conn, now)
            rows = self.conn.execute(
                "SELECT * FROM records WHERE scope=? ORDER BY part, record_id", (scope,)
            ).fetchall()
            if not rows:
                raise AgentError(f"未知诊断范围：{scope}")
            source = rows[0]["source"]
            trace = rows[0]["trace"]
            end_row = self.conn.execute(
                "SELECT part FROM records WHERE scope=? AND kind='end' ORDER BY part DESC LIMIT 1",
                (scope,),
            ).fetchone()
            end_part = int(end_row["part"]) if end_row else None
            by_part: dict[int, sqlite3.Row] = {}

            def evidence_rank(row: sqlite3.Row) -> int:
                if row["collector_acked"]:
                    return 4
                if row["local_state"] == STATE_STORED and not row["reclaimed"]:
                    return 3
                if row["local_state"] == STATE_SAMPLED:
                    return 1
                return 0

            for row in rows:
                part = int(row["part"])
                old = by_part.get(part)
                if old is None or evidence_rank(row) > evidence_rank(old):
                    by_part[part] = row
            delivered, local, unavailable, sampled = [], [], [], []
            for part, row in sorted(by_part.items()):
                if row["collector_acked"]:
                    delivered.append(part)
                elif row["local_state"] == STATE_STORED and not row["reclaimed"]:
                    local.append(part)
                elif row["local_state"] == STATE_SAMPLED:
                    sampled.append(part)
                else:
                    unavailable.append(part)
            gap_rows = self.conn.execute(
                "SELECT * FROM gaps WHERE scope=? AND status != ? ORDER BY part",
                (scope, GAP_HEALED),
            ).fetchall()
            missing = [int(r["part"]) for r in gap_rows if r["status"] == GAP_MISSING]
            waiting = [int(r["part"]) for r in gap_rows if r["status"] == GAP_WAITING]
            gap_sampled = [int(r["part"]) for r in gap_rows if r["status"] == GAP_SAMPLED]
            if end_part is not None:
                known_complete_range = all(
                    p in delivered or p in local for p in range(1, end_part + 1)
                )
            else:
                known_complete_range = False
            delivered_complete = end_part is not None and all(p in delivered for p in range(1, end_part + 1))
            evidence_complete = end_part is not None and known_complete_range and not missing and not sampled and not waiting
            if missing or sampled or unavailable:
                phase = "incomplete_with_gaps"
            elif end_part is None:
                phase = "awaiting_end"
            elif waiting:
                phase = "waiting_late_parts"
            elif not evidence_complete:
                phase = "incomplete"
            elif delivered_complete:
                phase = "delivered_complete"
            else:
                phase = "complete_pending_delivery"
            retransmittable = [row["record_id"] for row in rows
                               if row["local_state"] == STATE_STORED and not row["reclaimed"]
                               and not row["collector_acked"]]
            return {
                "scope": scope,
                "source": source,
                "trace": trace,
                "end_part": end_part,
                "known_parts": sorted(by_part),
                "delivered_parts": delivered,
                "local_parts": local,
                "sampled_parts": sorted(set(sampled + gap_sampled)),
                "unavailable_parts": sorted(set(unavailable + missing)),
                "missing_parts": missing,
                "waiting_parts": waiting,
                "gap_ranges": self._ranges_for_gaps(self.conn, scope, gap_rows),
                "retransmittable_record_ids": retransmittable,
                "evidence_complete": evidence_complete,
                "delivered_complete": delivered_complete,
                # 对外的 complete 采用最严格口径：证据齐且收集端确认齐。
                "complete": delivered_complete and evidence_complete,
                "phase": phase,
            }

    def scopes_status(self, *, now_ms: int | None = None) -> list[dict[str, Any]]:
        with self._lock:
            scopes = [r["scope"] for r in self.conn.execute(
                "SELECT DISTINCT scope FROM records ORDER BY scope"
            ).fetchall()]
        return [self.scope_status(scope, now_ms=now_ms) for scope in scopes]

    # ---------- delivery / reconciliation ----------

    def pending_records(
        self, *, source: str | None = None, limit: int | None = None
    ) -> list[dict[str, Any]]:
        sql = """SELECT record_id FROM records
                 WHERE collector_acked=0 AND reclaimed=0 AND local_state=?"""
        params: list[Any] = [STATE_STORED]
        if source is not None:
            sql += " AND source=?"
            params.append(source)
        sql += " ORDER BY first_seen_ms, scope, part"
        if limit is not None:
            sql += " LIMIT ?"
            params.append(limit)
        with self._lock:
            return [{"record_id": r["record_id"]} for r in self.conn.execute(sql, params).fetchall()]

    def retransmittable_records(self, *, source: str | None = None) -> list[dict[str, Any]]:
        return self.pending_records(source=source)

    def pending_record_rows(
        self, *, source: str | None = None, limit: int | None = None
    ) -> list[dict[str, Any]]:
        """返回可重传记录的完整字段和原始 payload，便于自带 sender。"""

        sql = """SELECT * FROM records
                 WHERE collector_acked=0 AND reclaimed=0 AND local_state=?"""
        params: list[Any] = [STATE_STORED]
        if source is not None:
            sql += " AND source=?"
            params.append(source)
        sql += " ORDER BY first_seen_ms, scope, part"
        if limit is not None:
            sql += " LIMIT ?"
            params.append(limit)
        with self._lock:
            rows = []
            for row in self.conn.execute(sql, params).fetchall():
                item = dict(row)
                item["payload"] = json.loads(item.pop("payload_json"))
                rows.append(item)
            return rows

    def get_record(self, record_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM records WHERE record_id=?", (record_id,)
            ).fetchone()
            if row is None:
                return None
            item = dict(row)
            item["payload"] = json.loads(item.pop("payload_json"))
            return item

    def record_status(self, record_id: str) -> dict[str, Any] | None:
        return self.get_record(record_id)

    def mark_accepted(
        self, record_ids: Sequence[str], *, now_ms: int | None = None
    ) -> dict[str, Any]:
        """手工填入收集端已确认 ID；等价于非权威 reconcile_remote。"""

        return self.reconcile_remote(list(record_ids), now_ms=now_ms, authoritative=False)

    def flush(
        self,
        *,
        at_ms: int | None = None,
        now_ms: int | None = None,
        record_ids: Sequence[str] | None = None,
        source: str | None = None,
        limit: int | None = None,
        transport: Any | None = None,
    ) -> dict[str, Any]:
        transport = transport or self.transport
        if transport is None:
            raise AgentError("flush 需要 transport，或构造 CollectionAgent(transport=...)")
        now = self._time(now_ms)
        at_ms = now if at_ms is None else at_ms
        requested_limit = limit if limit is not None else self.policy.max_batch_records
        batch_limit = max(1, min(int(requested_limit), self.policy.max_batch_records))

        with self._lock:
            deferred_requested_ids: list[str] = []
            if record_ids is not None:
                requested = list(record_ids)
                if not requested:
                    return {
                        "at_ms": at_ms, "sent": 0, "status": "empty",
                        "record_ids": [], "accepted": [], "not_accepted": [],
                        "unknown": False, "missing_requested_ids": [],
                        "skipped_requested_ids": [], "deferred_requested_ids": [],
                    }
                placeholders = ",".join("?" for _ in requested)
                rows = self.conn.execute(
                    f"""SELECT * FROM records WHERE record_id IN ({placeholders})
                        ORDER BY first_seen_ms, scope, part""",
                    requested,
                ).fetchall()
                found = {r["record_id"]: r for r in rows}
                missing_ids = [rid for rid in requested if rid not in found]
                eligible_all = [
                    found[rid] for rid in requested
                    if rid in found and found[rid]["local_state"] == STATE_STORED
                    and not found[rid]["reclaimed"] and not found[rid]["collector_acked"]
                ]
                eligible = eligible_all[:batch_limit]
                eligible_ids = {r["record_id"] for r in eligible}
                deferred_requested_ids = [r["record_id"] for r in eligible_all[batch_limit:]]
                skipped = [
                    rid for rid in requested
                    if rid not in eligible_ids and rid not in deferred_requested_ids
                ]
            else:
                rows = self.conn.execute(
                    """SELECT * FROM records
                       WHERE collector_acked=0 AND reclaimed=0 AND local_state=?
                       ORDER BY first_seen_ms, scope, part LIMIT ?""",
                    (STATE_STORED, batch_limit),
                ).fetchall()
                eligible = list(rows)
                missing_ids, skipped = [], []

            if not eligible:
                return {
                    "at_ms": at_ms, "sent": 0, "status": "empty",
                    "record_ids": [], "accepted": [], "not_accepted": [],
                    "unknown": False, "missing_requested_ids": missing_ids,
                    "skipped_requested_ids": skipped,
                    "deferred_requested_ids": deferred_requested_ids,
                }

            ids = [r["record_id"] for r in eligible]
            batch_id = f"batch-{self.conn.execute('SELECT COUNT(*) c FROM batches').fetchone()['c'] + 1:06d}-{uuid.uuid4().hex[:8]}"
            self.conn.execute(
                """INSERT INTO batches(batch_id, requested_at_ms, status, created_at_ms, updated_at_ms)
                   VALUES (?, ?, 'sending', ?, ?)""",
                (batch_id, at_ms, now, now),
            )
            for index, record_id in enumerate(ids):
                self.conn.execute(
                    "INSERT INTO batch_items(batch_id, ordinal, record_id) VALUES (?, ?, ?)",
                    (batch_id, index, record_id),
                )
            payload_records = [json.loads(r["payload_json"]) for r in eligible]
            self.conn.commit()

        try:
            ack = transport.post_batch(at_ms, payload_records)
        except CollectorUnavailable as exc:
            with self._lock, self.conn:
                self._note_collector_failure(self.conn, now)
                self.conn.execute(
                    "UPDATE batches SET status='indeterminate', reply_received=0, error=?, updated_at_ms=? "
                    "WHERE batch_id=?",
                    (str(exc), now, batch_id),
                )
                self._evaluate_backpressure(self.conn, now)
                self.conn.commit()
            return {
                "at_ms": at_ms, "batch_id": batch_id, "sent": len(ids),
                "status": "indeterminate", "unknown": True,
                "maybe_received": exc.maybe_received,
                "record_ids": ids, "accepted": [], "not_accepted": ids,
                "retransmittable_record_ids": ids, "error": str(exc),
                "missing_requested_ids": missing_ids,
                "skipped_requested_ids": skipped,
                "deferred_requested_ids": deferred_requested_ids,
            }
        except Exception as exc:  # transport 编程/协议异常也不能把批次永久留在 sending
            with self._lock, self.conn:
                self._note_collector_failure(self.conn, now)
                self.conn.execute(
                    "UPDATE batches SET status='indeterminate', reply_received=0, error=?, updated_at_ms=? "
                    "WHERE batch_id=?",
                    (f"{type(exc).__name__}: {exc}", now, batch_id),
                )
                self._evaluate_backpressure(self.conn, now)
                self.conn.commit()
            return {
                "at_ms": at_ms, "batch_id": batch_id, "sent": len(ids),
                "status": "indeterminate", "unknown": True,
                "maybe_received": True,
                "record_ids": ids, "accepted": [], "not_accepted": ids,
                "retransmittable_record_ids": ids,
                "error": f"{type(exc).__name__}: {exc}",
                "missing_requested_ids": missing_ids,
                "skipped_requested_ids": skipped,
                "deferred_requested_ids": deferred_requested_ids,
            }

        accepted_set = set(ack.accepted)
        with self._lock, self.conn:
            self._note_collector_success(self.conn, now)
            newly_acked = self._apply_remote_accepted(self.conn, ack.accepted, now)
            for record_id in ids:
                self.conn.execute(
                    "UPDATE batch_items SET accepted=? WHERE batch_id=? AND record_id=?",
                    (1 if record_id in accepted_set else 0, batch_id, record_id),
                )
            accepted_in_batch = [rid for rid in ids if rid in accepted_set]
            status = "acknowledged" if len(accepted_in_batch) == len(ids) else "partial"
            self.conn.execute(
                """UPDATE batches SET status=?, reply_received=1, http_status=?,
                   updated_at_ms=? WHERE batch_id=?""",
                (status, ack.http_status, now, batch_id),
            )
            if self.policy.auto_release_acked:
                self._release_acked(self.conn)
            self._touch_ack_sources(self.conn, newly_acked, now)
            self.conn.commit()
            not_accepted = [rid for rid in ids if rid not in accepted_set]
            return {
                "at_ms": at_ms, "batch_id": batch_id, "sent": len(ids),
                "status": status, "unknown": False,
                "http_status": ack.http_status,
                "record_ids": ids, "accepted": accepted_in_batch,
                "not_accepted": not_accepted,
                "newly_acked": newly_acked,
                "retransmittable_record_ids": not_accepted,
                "missing_requested_ids": missing_ids,
                "skipped_requested_ids": skipped,
                "deferred_requested_ids": deferred_requested_ids,
            }

    def reconcile_remote(
        self,
        accepted_ids: Sequence[str] | None = None,
        *,
        transport: Any | None = None,
        at_ms: int | None = None,
        now_ms: int | None = None,
        authoritative: bool | None = None,
    ) -> dict[str, Any]:
        now = self._time(now_ms)
        transport = transport or self.transport
        fetched_from_remote = accepted_ids is None
        if accepted_ids is None:
            if transport is None:
                raise AgentError("reconcile_remote 需要 accepted_ids 或 transport")
            try:
                accepted_ids = transport.get_received()
            except CollectorUnavailable as exc:
                with self._lock, self.conn:
                    self._note_collector_failure(self.conn, now)
                    self._evaluate_backpressure(self.conn, now)
                    self.conn.commit()
                raise
        if authoritative is None:
            # GET /received 是收集端全量实际集合；手工传入 ID 默认只作为“这些已确认”。
            authoritative = fetched_from_remote
        remote_ids = list(dict.fromkeys(accepted_ids))
        with self._lock, self.conn:
            self._note_collector_success(self.conn, now)
            before = {r["record_id"] for r in self.conn.execute(
                "SELECT record_id FROM records WHERE collector_acked=1"
            ).fetchall()}
            newly = self._apply_remote_accepted(self.conn, remote_ids, now)
            settled_batches = self._settle_batches(
                self.conn, set(remote_ids), authoritative=bool(authoritative), now=now
            )
            if self.policy.auto_release_acked:
                released = self._release_acked(self.conn)
            else:
                released = []
            self._touch_ack_sources(self.conn, newly, now)
            for scope_row in self.conn.execute("SELECT DISTINCT scope FROM records").fetchall():
                self._sync_scope_gaps(self.conn, scope_row["scope"], now)
            self._evaluate_backpressure(self.conn, now)
            self.conn.commit()
            unknown_remote = [
                rid for rid in remote_ids
                if self.conn.execute("SELECT 1 FROM records WHERE record_id=?", (rid,)).fetchone() is None
            ]
            return {
                "at_ms": at_ms if at_ms is not None else now,
                "reconciled_at_ms": now,
                "remote_total": len(remote_ids),
                "already_acked": [rid for rid in remote_ids if rid in before],
                "newly_acked": newly,
                "settled_batches": settled_batches,
                "authoritative": authoritative,
                "released": released,
                "unknown_remote_ids": unknown_remote,
            }

    def _apply_remote_accepted(
        self, conn: sqlite3.Connection, accepted_ids: Sequence[str], now: int
    ) -> list[str]:
        newly: list[str] = []
        for record_id in dict.fromkeys(accepted_ids):
            conn.execute(
                "INSERT OR IGNORE INTO remote_accepted(record_id, first_seen_remote_ms) VALUES (?, ?)",
                (record_id, now),
            )
            row = conn.execute(
                "SELECT collector_acked FROM records WHERE record_id=?", (record_id,)
            ).fetchone()
            if row is not None and not row["collector_acked"]:
                conn.execute(
                    "UPDATE records SET collector_acked=1, acked_at_ms=?, updated_at_ms=? "
                    "WHERE record_id=?",
                    (now, now, record_id),
                )
                newly.append(record_id)
        return newly

    def _settle_batches(
        self, conn: sqlite3.Connection, remote_ids: set[str], *, authoritative: bool, now: int
    ) -> list[dict[str, Any]]:
        """用事后 /received 结果结清断连时状态未知的批次。"""

        batch_ids = [r["batch_id"] for r in conn.execute(
            "SELECT DISTINCT batch_id FROM batch_items"
        ).fetchall()]
        settled: list[dict[str, Any]] = []
        for batch_id in batch_ids:
            items = conn.execute(
                "SELECT record_id, accepted FROM batch_items WHERE batch_id=? ORDER BY ordinal",
                (batch_id,),
            ).fetchall()
            changed = False
            for item in items:
                if item["accepted"] is not None:
                    continue
                if item["record_id"] in remote_ids:
                    conn.execute(
                        "UPDATE batch_items SET accepted=1 WHERE batch_id=? AND record_id=?",
                        (batch_id, item["record_id"]),
                    )
                    changed = True
                elif authoritative:
                    conn.execute(
                        "UPDATE batch_items SET accepted=0 WHERE batch_id=? AND record_id=?",
                        (batch_id, item["record_id"]),
                    )
                    changed = True
            if not changed:
                continue
            final_items = conn.execute(
                "SELECT accepted FROM batch_items WHERE batch_id=?", (batch_id,)
            ).fetchall()
            values = [row["accepted"] for row in final_items]
            if all(value == 1 for value in values):
                status = "acknowledged"
            elif any(value == 1 for value in values):
                status = "partial" if all(value is not None for value in values) else "indeterminate"
            elif authoritative and all(value == 0 for value in values):
                status = "not_accepted"
            else:
                status = "indeterminate"
            conn.execute(
                "UPDATE batches SET status=?, updated_at_ms=? WHERE batch_id=?",
                (status, now, batch_id),
            )
            settled.append({"batch_id": batch_id, "status": status})
        return settled

    def _touch_ack_sources(self, conn: sqlite3.Connection, record_ids: Sequence[str], now: int) -> None:
        sources = {
            r["source"] for rid in record_ids
            for r in conn.execute("SELECT source FROM records WHERE record_id=?", (rid,)).fetchall()
        }
        for source in sources:
            conn.execute("UPDATE sources SET last_ack_ms=? WHERE source=?", (now, source))

    def release_acked(self, *, source: str | None = None) -> list[dict[str, Any]]:
        with self._lock, self.conn:
            released = self._release_acked(self.conn, source)
            self.conn.commit()
            return released

    # ---------- operator status / persistence ----------

    def status(self, *, now_ms: int | None = None) -> dict[str, Any]:
        now = self._time(now_ms)
        with self._lock:
            self._expire_gaps(self.conn, now)
            self._evaluate_backpressure(self.conn, now)
            self.conn.commit()
            sources = [
                self.source_status(row["source"], now_ms=now)
                for row in self.conn.execute("SELECT source FROM sources ORDER BY source").fetchall()
            ]
            scopes = self.scopes_status(now_ms=now)
            gaps = self.gap_report(now_ms=now)
            batches = [dict(r) for r in self.conn.execute(
                "SELECT batch_id, requested_at_ms, status, reply_received, http_status, error "
                "FROM batches ORDER BY created_at_ms DESC LIMIT 20"
            ).fetchall()]
            admissions = [dict(r) for r in self.conn.execute(
                "SELECT at_ms, record_id, scope, part, kind, decision, reason "
                "FROM admissions ORDER BY id DESC LIMIT 30"
            ).fetchall()]
            retransmittable = [r["record_id"] for r in self.pending_records()]
            return {
                "at_ms": now,
                "database": self.db_path,
                "disk": self.disk_status(now_ms=now),
                "collector": self.collector_health(now_ms=now),
                "sources": {item["source"]: item for item in sources},
                "scopes": scopes,
                "actual_missing": gaps["actual_missing"],
                "gap_scopes": gaps["scopes"],
                "retransmittable_record_ids": retransmittable,
                "batches": batches,
                "recent_admissions": admissions,
            }

    def persist_snapshot_json(self, path: str | Path, *, now_ms: int | None = None) -> None:
        Path(path).write_text(
            json.dumps(self.status(now_ms=now_ms), ensure_ascii=False, indent=2, sort_keys=True),
            encoding="utf-8",
        )


# Common descriptive aliases for downstream scripts/tests.
CollectorAgent = CollectionAgent
SpoolAgent = CollectionAgent
DiagnosticAgent = CollectionAgent
ReplayAgent = CollectionAgent
Collector = CollectionAgent
Agent = CollectionAgent

# Verb aliases for varying embedding vocabularies.
for _alias in (
    "handle_record", "process_record", "submit_record", "add_record",
    "handle", "process", "submit",
):
    if not hasattr(CollectionAgent, _alias):
        setattr(CollectionAgent, _alias, CollectionAgent.ingest)
for _alias in ("send_pending", "send_batch", "forward", "post_batch"):
    if not hasattr(CollectionAgent, _alias):
        setattr(CollectionAgent, _alias, CollectionAgent.flush)
for _alias in ("reconcile", "sync_collector"):
    if not hasattr(CollectionAgent, _alias):
        setattr(CollectionAgent, _alias, CollectionAgent.reconcile_remote)
for _alias in ("ack", "acknowledge", "mark_acknowledged"):
    if not hasattr(CollectionAgent, _alias):
        setattr(CollectionAgent, _alias, CollectionAgent.mark_accepted)
for _alias in ("snapshot", "get_status", "report"):
    if not hasattr(CollectionAgent, _alias):
        setattr(CollectionAgent, _alias, CollectionAgent.status)
if not hasattr(CollectionAgent, "gaps"):
    CollectionAgent.gaps = CollectionAgent.gap_report
if not hasattr(CollectionAgent, "disk_snapshot"):
    CollectionAgent.disk_snapshot = CollectionAgent.apply_disk_snapshot
if not hasattr(CollectionAgent, "add_source"):
    CollectionAgent.add_source = CollectionAgent.register_source

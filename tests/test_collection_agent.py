import json
from pathlib import Path
import tempfile
import threading
import unittest

from collector_agent import CollectionAgent, Policy, ScriptedTransport
from collector_agent.rehearsal import run_material_rehearsal
from spool_peer import make_server
from collector_agent import HttpTransport


def rec(record_id, *, source="svc", trace="t1", part=1, kind="detail",
        size=1, at_ms=1):
    return {
        "at_ms": at_ms,
        "source": source,
        "trace": trace,
        "part": part,
        "record_id": record_id,
        "kind": kind,
        "size_units": size,
    }


class AdmissionAndGapTest(unittest.TestCase):
    def test_rejected_protected_record_and_gap_are_atomically_consistent(self):
        agent = CollectionAgent(":memory:", policy=Policy())
        agent.register_source("svc", reserve_units=5)
        agent.apply_disk_snapshot(at_ms=0, spool_limit=100, total_free=100, online_floor=0)

        result = agent.ingest(rec("err-1", kind="error", size=9, part=2), now_ms=1)
        self.assertEqual(result["decision"], "rejected")
        self.assertFalse(result["admitted"])

        gaps = agent.gap_report(now_ms=2)
        self.assertEqual([g["record_ids"] for g in gaps["actual_missing"]], [["err-1"]])
        scope = agent.scope_status("svc/t1", now_ms=2)
        self.assertIn(2, scope["missing_parts"])
        self.assertFalse(scope["complete"])

        admissions = agent.conn.execute(
            "SELECT decision FROM admissions WHERE record_id='err-1'"
        ).fetchall()
        self.assertEqual([row["decision"] for row in admissions], ["rejected"])
        agent.close()

    def test_rate_limited_routine_admission_matches_queryable_gap(self):
        agent = CollectionAgent(":memory:")
        agent.apply_disk_snapshot(at_ms=0, spool_limit=100, total_free=100, online_floor=0)
        agent.limit_source("health", reason="backlog", now_ms=10)

        result = agent.ingest(rec("h-9", source="health", kind="routine"), now_ms=11)
        self.assertEqual(result["decision"], "rate_limited")
        status = agent.status(now_ms=12)
        self.assertEqual(status["sources"]["health"]["routine_admission"], "rate_limited")
        self.assertTrue(any(
            gap["scope"] == "health/t1" and gap["record_ids"] == ["h-9"]
            for gap in status["actual_missing"]
        ))
        agent.close()

    def test_sampled_routine_is_known_omission_not_silent_unknown(self):
        agent = CollectionAgent(":memory:", policy=Policy(routine_sample_rate=0.0))
        agent.register_source("svc")
        agent.apply_disk_snapshot(at_ms=0, spool_limit=100, total_free=100, online_floor=0)
        result = agent.ingest(rec("r-1", kind="routine"), now_ms=1)
        self.assertEqual(result["decision"], "sampled")
        report = agent.gap_report(now_ms=2)
        self.assertEqual(report["scopes"][0]["sampled_parts"], [1])
        self.assertEqual(report["actual_missing"], [])
        scope = agent.scope_status("svc/t1")
        self.assertEqual(scope["sampled_parts"], [1])
        self.assertFalse(scope["evidence_complete"])
        agent.close()

    def test_disk_pressure_evicts_routine_and_commits_boundary_gap(self):
        agent = CollectionAgent(":memory:")
        agent.register_source("svc", reserve_units=100)
        agent.apply_disk_snapshot(at_ms=0, spool_limit=100, total_free=30, online_floor=18)
        routine = agent.ingest(rec("routine-1", trace="other", kind="routine", size=5, part=1))
        self.assertEqual(routine["decision"], "accepted")

        # 新快照把实际空闲报告为 23；随后 10 单位 error 只有 5 单位安全余量。
        agent.apply_disk_snapshot(at_ms=5, spool_limit=100, total_free=23, online_floor=18)
        error = agent.ingest(rec("err-1", kind="error", size=10, part=2), now_ms=6)
        self.assertEqual(error["decision"], "accepted")
        self.assertIn("routine-1", [x["record_id"] for x in error["evicted"]])

        scope = agent.scope_status("svc/other", now_ms=7)
        self.assertIn(1, scope["missing_parts"])
        self.assertEqual(scope["gap_ranges"][0]["reason"], "disk_or_online_floor")
        disk = agent.disk_status()
        self.assertGreaterEqual(disk["free_units"], disk["online_floor"])
        agent.close()


class ScopeReconciliationTest(unittest.TestCase):
    def test_missing_part_waits_then_becomes_actual_gap_and_can_heal(self):
        agent = CollectionAgent(":memory:", policy=Policy(gap_wait_ms=10))
        agent.register_source("svc", reserve_units=100)
        agent.apply_disk_snapshot(at_ms=0, spool_limit=100, total_free=100, online_floor=0)
        agent.ingest(rec("p1", part=1, size=1, at_ms=1), now_ms=1)
        agent.ingest(rec("p3-end", part=3, kind="end", size=1, at_ms=2), now_ms=2)

        waiting = agent.scope_status("svc/t1", now_ms=5)
        self.assertEqual(waiting["waiting_parts"], [2])
        self.assertEqual(waiting["phase"], "waiting_late_parts")
        self.assertFalse(waiting["complete"])

        expired = agent.scope_status("svc/t1", now_ms=20)
        self.assertEqual(expired["missing_parts"], [2])
        self.assertEqual(expired["phase"], "incomplete_with_gaps")

        agent.ingest(rec("p2", part=2, kind="detail", at_ms=21), now_ms=21)
        healed = agent.scope_status("svc/t1", now_ms=22)
        self.assertEqual(healed["waiting_parts"], [])
        self.assertEqual(healed["missing_parts"], [])
        self.assertTrue(healed["evidence_complete"])
        self.assertFalse(healed["delivered_complete"])
        agent.close()

    def test_partial_ack_disconnect_reconcile_and_idempotent_resend(self):
        transport = ScriptedTransport([
            {"at_ms": 0, "max_accept": 1, "reply": True},
            {"at_ms": 10, "max_accept": 2, "reply": True},
        ])
        with tempfile.TemporaryDirectory() as directory:
            db = str(Path(directory) / "agent.sqlite3")
            agent = CollectionAgent(db, transport=transport)
            agent.apply_disk_snapshot(at_ms=0, spool_limit=100, total_free=100, online_floor=0)
            agent.ingest(rec("a", part=1), now_ms=1)
            agent.ingest(rec("b", part=2), now_ms=2)

            first = agent.flush(at_ms=3, now_ms=3)
            self.assertEqual(first["status"], "partial")
            self.assertEqual(first["accepted"], ["a"])
            self.assertEqual(first["retransmittable_record_ids"], ["b"])

            again = agent.flush(at_ms=4, now_ms=4, record_ids=["a", "b"])
            self.assertEqual(again["sent"], 1)  # a is already acked and skipped.
            self.assertEqual(again["accepted"], ["b"])
            self.assertEqual(sorted(transport.received), ["a", "b"])

            # 重开代理后，SQLite 中的已确认/可重传口径不变。
            agent.close()
            agent = CollectionAgent(db, transport=transport)
            status = agent.status(now_ms=5)
            self.assertEqual(status["retransmittable_record_ids"], [])
            reconciled = agent.reconcile_remote(at_ms=5, now_ms=5)
            self.assertEqual(reconciled["newly_acked"], [])
            agent.close()

    def test_disconnected_post_is_unknown_until_remote_reconciliation(self):
        transport = ScriptedTransport([
            {"at_ms": 20, "max_accept": 1, "reply": False},
            {"at_ms": 22, "max_accept": 2, "reply": True},
        ])
        agent = CollectionAgent(":memory:", transport=transport)
        agent.apply_disk_snapshot(at_ms=0, spool_limit=100, total_free=100, online_floor=0)
        agent.ingest(rec("a"), now_ms=1)
        agent.ingest(rec("b", part=2), now_ms=2)

        result = agent.flush(at_ms=20, now_ms=20)
        self.assertEqual(result["status"], "indeterminate")
        self.assertTrue(result["unknown"])
        self.assertEqual(result["accepted"], [])
        self.assertEqual(set(result["retransmittable_record_ids"]), {"a", "b"})

        remote = agent.reconcile_remote(at_ms=21, now_ms=21)
        self.assertEqual(remote["newly_acked"], ["a"])
        self.assertEqual(agent.status()["retransmittable_record_ids"], ["b"])

        retry = agent.flush(at_ms=22, now_ms=22)
        self.assertEqual(retry["accepted"], ["b"])
        # 收集端稳定 ID 去重，a 重发也不会变成两条。
        self.assertEqual(sorted(transport.get_received()), ["a", "b"])
        agent.close()

    def test_duplicate_stable_id_with_redelivery_time_is_idempotent(self):
        agent = CollectionAgent(":memory:")
        agent.apply_disk_snapshot(at_ms=0, spool_limit=100, total_free=100, online_floor=0)
        first = agent.ingest(rec("dup", at_ms=23), now_ms=23)
        second_row = rec("dup", at_ms=54)
        second = agent.ingest(second_row, now_ms=54)
        self.assertEqual(first["decision"], "accepted")
        self.assertEqual(second["decision"], "duplicate")
        count = agent.conn.execute("SELECT COUNT(*) c FROM records WHERE record_id='dup'").fetchone()["c"]
        self.assertEqual(count, 1)
        self.assertEqual(agent.conn.execute("SELECT receive_count FROM records").fetchone()["receive_count"], 2)
        agent.close()


    def test_auto_resumed_source_does_not_reject_first_routine(self):
        agent = CollectionAgent(":memory:", policy=Policy(source_resume_watermark=0.5))
        agent.register_source("svc", reserve_units=10)
        agent.apply_disk_snapshot(at_ms=0, spool_limit=100, total_free=100, online_floor=0)
        agent.limit_source("svc", reason="backlog", now_ms=1)
        agent.ingest(rec("bp1", size=9), now_ms=2)
        agent.reconcile_remote(["bp1"], now_ms=3, authoritative=True)

        result = agent.ingest(rec("new-r", kind="routine", size=1, part=2), now_ms=4)
        self.assertEqual(result["decision"], "accepted")
        self.assertEqual(result["source_state"]["routine_admission"], "open")
        self.assertEqual(agent.gap_report(now_ms=5)["actual_missing"], [])
        agent.close()

    def test_remote_id_known_before_ingest_is_not_pending(self):
        agent = CollectionAgent(":memory:")
        agent.reconcile_remote(["pre-known"], now_ms=1)
        result = agent.ingest(rec("pre-known"), now_ms=2)
        self.assertIn(result["decision"], ("duplicate", "accepted"))
        record = agent.get_record("pre-known")
        self.assertTrue(record["collector_acked"])
        self.assertEqual(agent.pending_records(), [])
        agent.close()

    def test_gap_deadline_uses_observation_time_not_old_event_time(self):
        agent = CollectionAgent(":memory:", policy=Policy(gap_wait_ms=10))
        agent.apply_disk_snapshot(at_ms=0, spool_limit=100, total_free=100, online_floor=0)
        agent.ingest(rec("end", part=2, kind="end", at_ms=2), now_ms=500)
        waiting = agent.scope_status("svc/t1", now_ms=501)
        self.assertEqual(waiting["waiting_parts"], [1])
        expired = agent.scope_status("svc/t1", now_ms=512)
        self.assertEqual(expired["missing_parts"], [1])
        agent.close()

    def test_reconcile_settles_historical_indeterminate_batch(self):
        transport = ScriptedTransport([{"at_ms": 20, "max_accept": 1, "reply": False}])
        agent = CollectionAgent(":memory:", transport=transport)
        agent.apply_disk_snapshot(at_ms=0, spool_limit=100, total_free=100, online_floor=0)
        agent.ingest(rec("a"), now_ms=1)
        agent.ingest(rec("b", part=2), now_ms=2)
        flush = agent.flush(at_ms=20, now_ms=20)
        self.assertEqual(flush["status"], "indeterminate")

        result = agent.reconcile_remote(at_ms=21, now_ms=21)
        self.assertEqual(result["settled_batches"], [
            {"batch_id": flush["batch_id"], "status": "partial"}
        ])
        batch = agent.status(now_ms=21)["batches"][0]
        self.assertEqual(batch["status"], "partial")
        agent.close()


    def test_disk_pressure_queries_late_ack_before_evicting_evidence(self):
        transport = ScriptedTransport([{"at_ms": 0, "max_accept": 1, "reply": False}])
        agent = CollectionAgent(":memory:", transport=transport)
        agent.register_source("svc", reserve_units=100)
        agent.apply_disk_snapshot(at_ms=0, spool_limit=100, total_free=28, online_floor=18)
        agent.ingest(rec("late-ack", size=5, part=1), now_ms=1)
        flush = agent.flush(at_ms=1, now_ms=1)
        self.assertEqual(flush["status"], "indeterminate")

        result = agent.ingest(rec("new-error", kind="error", size=10, part=2), now_ms=2)
        self.assertEqual(result["decision"], "accepted")
        old = agent.get_record("late-ack")
        self.assertTrue(old["collector_acked"])
        self.assertEqual(old["local_state"], "reclaimed")
        self.assertEqual(agent.gap_report(now_ms=3)["actual_missing"], [])
        agent.close()

    def test_rejected_end_still_declares_waiting_range(self):
        agent = CollectionAgent(":memory:", policy=Policy(gap_wait_ms=100))
        agent.register_source("svc", reserve_units=100)
        agent.apply_disk_snapshot(at_ms=0, spool_limit=100, total_free=10, online_floor=9)
        # 安全余量只有 1，end 大小 5，且没有可回收记录。
        result = agent.ingest(rec("end-bad", part=3, kind="end", size=5), now_ms=1)
        self.assertEqual(result["decision"], "rejected")
        scope = agent.scope_status("svc/t1", now_ms=2)
        self.assertEqual(scope["end_part"], 3)
        self.assertEqual(scope["waiting_parts"], [1, 2])
        self.assertIn(3, scope["missing_parts"])
        self.assertFalse(scope["complete"])
        agent.close()


class MaterialRehearsalTest(unittest.TestCase):
    def test_key_operator_view_and_final_reconciliation(self):
        result = run_material_rehearsal()
        try:
            view = result["operator_view"]
            self.assertEqual(view["health_new_routine"], "rate_limited")
            self.assertEqual(view["new_worker_part_decision"], "accepted")
            self.assertIn(3, view["worker_t40"]["local_parts"])
            self.assertIn("w-403", view["worker_t40"]["retransmittable_record_ids"])
            self.assertEqual(view["worker_t40"]["waiting_parts"], [])
            self.assertTrue(any(g["scope"] == "health/h2" for g in view["actual_missing"]))
            phases = {s["scope"]: s for s in result["final_status"]["scopes"]}
            self.assertTrue(phases["worker/t40"]["complete"])
            self.assertTrue(phases["front/t20"]["complete"])
            self.assertTrue(phases["front/t21"]["complete"])
            self.assertEqual(result["final_status"]["retransmittable_record_ids"], [])
        finally:
            result["agent"].close()


class LiveSpoolPeerRestartTest(unittest.TestCase):
    def test_agent_restart_uses_same_received_boundary(self):
        with tempfile.TemporaryDirectory() as directory:
            collector_db = Path(directory) / "collector.sqlite3"
            agent_db = Path(directory) / "agent.sqlite3"
            with make_server(collector_db, 0) as server:
                thread = threading.Thread(target=server.serve_forever)
                thread.start()
                try:
                    url = f"http://127.0.0.1:{server.server_address[1]}"
                    transport = HttpTransport(url, timeout=2)
                    agent = CollectionAgent(agent_db, transport=transport)
                    agent.apply_disk_snapshot(at_ms=0, spool_limit=100, total_free=100, online_floor=0)
                    agent.ingest(rec("live-a"), now_ms=1)
                    agent.ingest(rec("live-b", part=2), now_ms=2)
                    disconnected = agent.flush(at_ms=24, now_ms=24)
                    self.assertEqual(disconnected["status"], "indeterminate")

                    reconciled = agent.reconcile_remote()
                    self.assertEqual(reconciled["newly_acked"], ["live-a"])
                    before = agent.status(now_ms=30)
                    agent.close()

                    agent = CollectionAgent(agent_db, transport=transport)
                    reopened = agent.status(now_ms=30)
                    self.assertEqual(
                        reopened["retransmittable_record_ids"],
                        before["retransmittable_record_ids"],
                    )
                    retried = agent.flush(at_ms=48, now_ms=48)
                    self.assertEqual(retried["accepted"], ["live-b"])
                    final = agent.reconcile_remote()
                    self.assertEqual(final["newly_acked"], [])
                    self.assertEqual(agent.status()["retransmittable_record_ids"], [])
                    agent.close()
                finally:
                    server.shutdown()
                    thread.join()


if __name__ == "__main__":
    unittest.main()

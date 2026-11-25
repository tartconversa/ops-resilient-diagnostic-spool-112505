from __future__ import annotations

import argparse
import json
import threading
from pathlib import Path

from .agent import CollectionAgent, HttpTransport, Policy
from .rehearsal import replay_directory, run_material_rehearsal
from .server import make_ingest_server


def build_agent(args: argparse.Namespace) -> CollectionAgent:
    transport = HttpTransport(args.collector, timeout=args.timeout) if args.collector else None
    policy = Policy(gap_wait_ms=args.gap_wait_ms)
    return CollectionAgent(args.db, transport=transport, policy=policy)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="python -m collector_agent", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    rehearse = sub.add_parser("rehearse", help="用 materials 资料跑确定性演练")
    rehearse.add_argument("--db", default=":memory:", help="SQLite 路径；默认内存库")
    rehearse.add_argument("--report", type=Path, help="把完整演练报告写入 JSON")
    rehearse.add_argument("--materials", type=Path, help="自定义资料目录")

    replay = sub.add_parser("replay", help="通用回放一个新的 JSONL 资料目录")
    replay.add_argument("materials", type=Path)
    replay.add_argument("--db", default=":memory:")
    replay.add_argument("--report", type=Path)
    replay.add_argument("--no-flush", action="store_true")
    replay.add_argument("--no-drain", action="store_true")

    status = sub.add_parser("status", help="输出持久化状态和对账视图")
    status.add_argument("--db", default="agent_state.sqlite3")
    status.add_argument("--collector", help="收集端 base URL；提供时先查询 /received")
    status.add_argument("--timeout", type=float, default=2.0)
    status.add_argument("--gap-wait-ms", type=int, default=10_000)
    status.add_argument("--now-ms", type=int)

    reconcile = sub.add_parser("reconcile", help="查询收集端 /received 并对账")
    reconcile.add_argument("--db", default="agent_state.sqlite3")
    reconcile.add_argument("--collector", default="http://127.0.0.1:8099")
    reconcile.add_argument("--timeout", type=float, default=2.0)
    reconcile.add_argument("--gap-wait-ms", type=int, default=10_000)

    serve = sub.add_parser("serve", help="启动本地记录入口")
    serve.add_argument("--db", default="agent_state.sqlite3")
    serve.add_argument("--collector", default="http://127.0.0.1:8099")
    serve.add_argument("--ingest-host", default="127.0.0.1")
    serve.add_argument("--ingest-port", type=int, default=8100)
    serve.add_argument("--timeout", type=float, default=2.0)
    serve.add_argument("--gap-wait-ms", type=int, default=10_000)
    serve.add_argument("--flush-interval-seconds", type=float, default=1.0)
    serve.add_argument("--no-background-flush", action="store_true")

    args = parser.parse_args(argv)

    if args.command == "rehearse":
        result = run_material_rehearsal(args.db, materials_root=args.materials)
        try:
            if args.report:
                args.report.write_text(
                    json.dumps(
                        {k: v for k, v in result.items() if k not in {"agent", "transport"}},
                        ensure_ascii=False, indent=2, default=str,
                    ),
                    encoding="utf-8",
                )
            print(json.dumps(result["operator_view"], ensure_ascii=False, indent=2, default=str))
            print("\n最终范围状态：")
            for scope in result["final_status"]["scopes"]:
                print(f"- {scope['scope']}: {scope['phase']} complete={scope['complete']}")
        finally:
            result["agent"].close()
        return

    if args.command == "replay":
        result = replay_directory(
            args.materials,
            args.db,
            flush_after_record=not args.no_flush,
            drain_batches=not args.no_drain,
        )
        try:
            if args.report:
                args.report.write_text(
                    json.dumps(
                        {k: v for k, v in result.items() if k not in {"agent", "transport"}},
                        ensure_ascii=False, indent=2, default=str,
                    ),
                    encoding="utf-8",
                )
            status = result["status"]
            print(json.dumps({
                "at_ms": status["at_ms"],
                "disk": status["disk"],
                "sources": status["sources"],
                "actual_missing": status["actual_missing"],
                "retransmittable_record_ids": status["retransmittable_record_ids"],
                "scopes": [
                    {k: scope[k] for k in (
                        "scope", "phase", "complete", "evidence_complete",
                        "delivered_complete", "waiting_parts", "missing_parts",
                        "sampled_parts", "retransmittable_record_ids",
                    )}
                    for scope in status["scopes"]
                ],
            }, ensure_ascii=False, indent=2, default=str))
        finally:
            result["agent"].close()
        return

    agent = build_agent(args)
    try:
        if args.command == "status":
            if args.collector:
                agent.reconcile_remote()
            print(json.dumps(agent.status(now_ms=args.now_ms), ensure_ascii=False, indent=2, default=str))
        elif args.command == "reconcile":
            result = agent.reconcile_remote()
            print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
        elif args.command == "serve":
            server = make_ingest_server(agent, args.ingest_host, args.ingest_port)
            stop = threading.Event()

            def forward_loop() -> None:
                # 断连结果在 flush 内已落为 indeterminate；这里再周期性 GET /received，
                # 以接住收集端持久接收后响应晚到/连接断开的确认。
                while not stop.wait(args.flush_interval_seconds):
                    try:
                        agent.flush()
                        agent.reconcile_remote()
                    except Exception:
                        # 状态已由 agent 记录为 collector 不可用；循环继续。
                        pass

            worker = None
            if not args.no_background_flush and args.collector:
                worker = threading.Thread(target=forward_loop, name="collector-forwarder", daemon=True)
                worker.start()
            print(f"采集代理入口监听 http://{args.ingest_host}:{args.ingest_port}", flush=True)
            try:
                server.serve_forever()
            except KeyboardInterrupt:
                pass
            finally:
                stop.set()
                server.server_close()
                if worker is not None:
                    worker.join(timeout=2)
    finally:
        agent.close()


if __name__ == "__main__":
    main()

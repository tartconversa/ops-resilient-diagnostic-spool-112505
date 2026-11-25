"""代理的本地 HTTP 入口，便于和 spool_peer.py 做真实进程联调。"""

from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from .agent import CollectionAgent


def make_ingest_server(agent: CollectionAgent, host: str = "127.0.0.1", port: int = 8100):
    class Handler(BaseHTTPRequestHandler):
        def _send_json(self, status: int, value: Any) -> None:
            data = json.dumps(value, ensure_ascii=False, default=str).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def _body(self) -> Any:
            length = int(self.headers.get("Content-Length", "0"))
            if length <= 0 or length > 8 * 1024 * 1024:
                raise ValueError("invalid content length")
            return json.loads(self.rfile.read(length).decode("utf-8"))

        def do_GET(self) -> None:  # noqa: N802
            if self.path in ("/status", "/v1/status"):
                self._send_json(200, agent.status())
            elif self.path in ("/gaps", "/v1/gaps"):
                self._send_json(200, agent.gap_report())
            elif self.path in ("/retransmittable", "/v1/retransmittable"):
                self._send_json(200, {"record_ids": [r["record_id"] for r in agent.retransmittable_records()]})
            elif self.path == "/healthz":
                self._send_json(200, {"ok": True})
            else:
                self._send_json(404, {"error": "unknown path"})

        def do_POST(self) -> None:  # noqa: N802
            try:
                payload = self._body()
            except Exception as exc:
                self._send_json(400, {"error": f"invalid JSON body: {exc}"})
                return
            try:
                if self.path in ("/ingest", "/v1/records"):
                    if isinstance(payload, list):
                        results = [agent.ingest(row) for row in payload]
                        self._send_json(200, {"results": results})
                    else:
                        self._send_json(200, agent.ingest(payload))
                elif self.path in ("/disk", "/v1/disk-snapshot"):
                    self._send_json(200, agent.apply_disk_snapshot(payload))
                elif self.path in ("/sources", "/v1/sources"):
                    if isinstance(payload, list):
                        agent.load_sources(payload)
                        self._send_json(200, {"ok": True})
                    else:
                        self._send_json(200, agent.register_source(**payload))
                elif self.path in ("/flush", "/v1/flush"):
                    self._send_json(200, agent.flush(**payload))
                elif self.path in ("/reconcile", "/v1/reconcile"):
                    self._send_json(200, agent.reconcile_remote(**payload))
                elif self.path in ("/limit-source", "/v1/source-limit"):
                    self._send_json(200, agent.limit_source(**payload))
                elif self.path in ("/resume-source", "/v1/source-resume"):
                    self._send_json(200, agent.resume_source(**payload))
                else:
                    self._send_json(404, {"error": "unknown path"})
            except TypeError as exc:
                self._send_json(400, {"error": f"invalid request arguments: {exc}"})
            except Exception as exc:  # Keep local entrypoint usable during drills.
                self._send_json(409, {"error": str(exc), "type": type(exc).__name__})

    return ThreadingHTTPServer((host, port), Handler)

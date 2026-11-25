"""本地合成收集端，保存自己已接收的记录标识。"""
import argparse
from contextlib import closing
import json
import socket
import sqlite3
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from materials import read_feed


def make_server(state_path, port=0):
    with closing(sqlite3.connect(state_path)) as connection, connection:
        connection.execute("CREATE TABLE IF NOT EXISTS accepted (id TEXT PRIMARY KEY)")
    states = list(read_feed("endpoint"))

    class Handler(BaseHTTPRequestHandler):
        def send_json(self, status, value):
            data = json.dumps(value).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self):
            if self.path != "/received":
                self.send_json(404, {"error": "未知的路径"})
                return
            with closing(sqlite3.connect(state_path)) as connection, connection:
                rows = connection.execute("SELECT id FROM accepted ORDER BY id").fetchall()
            self.send_json(200, {"accepted": [row[0] for row in rows]})

        def do_POST(self):
            if self.path != "/batch":
                self.send_json(404, {"error": "未知的路径"})
                return
            try:
                length = int(self.headers["Content-Length"])
                if not 0 < length <= 1024 * 1024:
                    raise ValueError()
                payload = json.loads(self.rfile.read(length))
                now = payload["at_ms"]
                items = payload["records"]
                if type(now) is not int or now < 0 or not isinstance(items, list):
                    raise ValueError()
                identifiers = [item["record_id"] for item in items]
                if any(not isinstance(identifier, str) or not identifier for identifier in identifiers):
                    raise ValueError()
            except (KeyError, TypeError, ValueError, UnicodeDecodeError):
                self.send_json(400, {"error": "记录集合或时钟无效"})
                return
            state = max((s for s in states if s["at_ms"] <= now), key=lambda s: s["at_ms"])
            accepted = []
            remaining = state["max_accept"]
            with closing(sqlite3.connect(state_path)) as connection, connection:
                connection.execute("BEGIN IMMEDIATE")
                for identifier in identifiers:
                    exists = connection.execute("SELECT 1 FROM accepted WHERE id=?", (identifier,)).fetchone()
                    if exists:
                        accepted.append(identifier)
                    elif remaining > 0:
                        connection.execute("INSERT INTO accepted VALUES (?)", (identifier,))
                        accepted.append(identifier)
                        remaining -= 1
            if state["reply"]:
                self.send_json(200, {"accepted": accepted})
            else:
                self.close_connection = True
                self.connection.shutdown(socket.SHUT_WR)

    return ThreadingHTTPServer(("127.0.0.1", port), Handler)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=8099)
    parser.add_argument("--state", type=Path, default=Path("collector.sqlite3"))
    args = parser.parse_args()
    with make_server(args.state, args.port) as server:
        print(f"本地合成收集端监听 {server.server_address[1]}", flush=True)
        server.serve_forever()


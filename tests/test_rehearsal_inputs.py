import http.client
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import threading
import unittest
from urllib.request import Request, urlopen

from materials import read_feed
from spool_peer import make_server


class InputsTest(unittest.TestCase):
    def test_part_arrival_order(self):
        rows = [r for r in read_feed("records") if r["trace"] == "t40"]
        self.assertEqual(sorted(r["part"] for r in rows), [1, 2, 3, 4, 5])
        self.assertGreater(next(r["at_ms"] for r in rows if r["part"] == 3),
                           next(r["at_ms"] for r in rows if r["kind"] == "end"))

    def test_collector_acknowledges_real_partial_batch(self):
        with TemporaryDirectory() as dirname:
            state = Path(dirname) / "store.db"
            with make_server(state) as server:
                thread = threading.Thread(target=server.serve_forever)
                thread.start()
                host = f"http://127.0.0.1:{server.server_address[1]}"
                rows = [{"record_id": "demo-a"}, {"record_id": "demo-b"}]
                batch = Request(host + "/batch", json.dumps({"at_ms": 24, "records": rows}).encode())
                try:
                    with self.assertRaises(http.client.RemoteDisconnected):
                        urlopen(batch, timeout=2)
                    with urlopen(host + "/received", timeout=2) as result:
                        self.assertEqual(json.load(result)["accepted"], ["demo-a"])
                finally:
                    server.shutdown()
                    thread.join()
            with make_server(state) as restarted:
                thread = threading.Thread(target=restarted.serve_forever)
                thread.start()
                try:
                    url = f"http://127.0.0.1:{restarted.server_address[1]}/batch"
                    batch = Request(url, json.dumps({"at_ms": 48, "records": rows}).encode())
                    with urlopen(batch, timeout=2) as result:
                        self.assertEqual(json.load(result)["accepted"], ["demo-a", "demo-b"])
                finally:
                    restarted.shutdown()
                    thread.join()


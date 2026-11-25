"""兼容直接 `python agent.py ...` 和 `import agent` 的入口。

主要实现位于 :mod:`collector_agent.agent`。
"""

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))

from collector_agent.agent import *  # noqa: E402,F401,F403
from collector_agent.agent import (  # noqa: E402,F401
    AdmissionDecision,
    Agent,
    CollectionAgent,
    CollectorUnavailable,
    HttpTransport,
    Policy,
    ScriptedTransport,
    SpoolAgent,
    stable_batch_id,
)
from collector_agent.cli import main  # noqa: E402,F401
from collector_agent.rehearsal import (  # noqa: E402,F401
    replay_directory,
    run_material_rehearsal,
    run_rehearsal,
)
from collector_agent.server import make_ingest_server  # noqa: E402,F401

make_server = make_ingest_server

if __name__ == "__main__":
    main()

"""Compatibility module name; implementation is in :mod:`collector_agent.agent`."""

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))

from collector_agent.agent import *  # noqa: E402,F401,F403
from collector_agent.agent import (  # noqa: E402,F401
    AdmissionDecision,
    Agent,
    AgentError,
    CollectionAgent,
    Collector,
    CollectorUnavailable,
    DiagnosticAgent,
    Policy,
    ScriptedTransport,
)
from collector_agent.cli import main

if __name__ == "__main__":
    main()

"""直接运行资料演练：``python rehearse_agent.py [report.json]``。"""

from __future__ import annotations

import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))

from collector_agent.rehearsal import run_material_rehearsal  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    report_path = argv[0] if argv else None
    result = run_material_rehearsal()
    try:
        view = result["operator_view"]
        print(json.dumps(view, ensure_ascii=False, indent=2, default=str))
        if report_path:
            with open(report_path, "w", encoding="utf-8") as stream:
                json.dump(
                    {k: v for k, v in result.items() if k not in {"agent", "transport"}},
                    stream,
                    ensure_ascii=False,
                    indent=2,
                    default=str,
                )
        print("\n最终范围状态：")
        for scope in result["final_status"]["scopes"]:
            print(f"- {scope['scope']}: {scope['phase']} complete={scope['complete']}")
    finally:
        result["agent"].close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    repository = Path(__file__).resolve().parent.parent
    if str(repository) not in sys.path:
        sys.path.insert(0, str(repository))

    from trendradar.ai.client import AIClient

    payload = json.loads(Path(args.input).read_text(encoding="utf-8"))
    response = AIClient(payload["config"]).chat(payload["messages"])
    Path(args.output).write_text(response, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

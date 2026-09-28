"""从命令行校验领域事件（支持单个事件对象或事件数组）。"""

import json
import sys
from pathlib import Path
from typing import Any

from .contracts import validate_event


def _load_documents(path: Path) -> list[Any]:
    text = path.read_text(encoding="utf-8").strip()
    if not text:
        return []
    if text[0] in "[{":
        doc = json.loads(text)
        return doc if isinstance(doc, list) else [doc]
    # JSONL
    return [json.loads(line) for line in text.splitlines() if line.strip()]


def main() -> int:
    if len(sys.argv) != 3:
        print("用法: python -m bakery_exit.cli <schema.json> <event.json|events.jsonl>", file=sys.stderr)
        return 2
    schema = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    documents = _load_documents(Path(sys.argv[2]))
    if not documents:
        print("empty", file=sys.stderr)
        return 2

    found_issue = False
    for index, event in enumerate(documents):
        issues = validate_event(event, schema)
        for issue in issues:
            found_issue = True
            prefix = f"[{index}] " if len(documents) > 1 else ""
            print(f"{prefix}{issue.field}	{issue.code}	{issue.message}")
    if found_issue:
        return 1
    print("valid")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""事件存储：JSONL 仅追加日志，进程重启后完整回放。"""

from __future__ import annotations

import json
import os
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable


class EventStore:
    """按写入顺序持久化事件；版本号由调用方（服务层）保证按聚合递增。"""

    def __init__(self, path: str | os.PathLike[str]) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._events: list[dict[str, Any]] = []
        self._versions: dict[str, int] = defaultdict(int)
        self._replay()

    @staticmethod
    def _key(aggregate_type: str, aggregate_id: str) -> str:
        return f"{aggregate_type} {aggregate_id}"

    def _replay(self) -> None:
        if not self.path.exists():
            return
        with self.path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                event = json.loads(line)
                self._index(event)

    def _index(self, event: dict[str, Any]) -> None:
        self._events.append(event)
        key = self._key(event["aggregate_type"], event["aggregate_id"])
        version = int(event["version"])
        if version != self._versions[key] + 1:
            raise ValueError(
                f"事件流版本断裂：{key} 期望 {self._versions[key] + 1}，实际 {version}"
            )
        self._versions[key] = version

    def append(self, event: dict[str, Any]) -> None:
        key = self._key(event["aggregate_type"], event["aggregate_id"])
        expected = self._versions[key] + 1
        if int(event["version"]) != expected:
            raise ValueError(f"事件版本必须连续：{key} 下一版本应为 {expected}")
        self._write(event)
        self._index(event)

    def _write(self, event: dict[str, Any]) -> None:
        line = json.dumps(event, ensure_ascii=False, separators=(",", ":"))
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(line + "\n")
            handle.flush()
            os.fsync(handle.fileno())

    def read_all(self) -> list[dict[str, Any]]:
        return list(self._events)

    def for_aggregate(self, aggregate_type: str, aggregate_id: str) -> list[dict[str, Any]]:
        key = self._key(aggregate_type, aggregate_id)
        return [
            event
            for event in self._events
            if self._key(event["aggregate_type"], event["aggregate_id"]) == key
        ]

    def next_version(self, aggregate_type: str, aggregate_id: str) -> int:
        return self._versions[self._key(aggregate_type, aggregate_id)] + 1

    def __len__(self) -> int:
        return len(self._events)

    def extend(self, events: Iterable[dict[str, Any]]) -> None:
        for event in events:
            self.append(event)

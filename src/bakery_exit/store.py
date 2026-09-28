"""仅追加事件存储：聚合版本并发控制、命令幂等指纹、JSONL 持久化与重放恢复。"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Optional, Sequence

from .errors import ConcurrencyError, IdempotencyConflictError


@dataclass(frozen=True)
class StoredEvent:
    seq: int
    event_id: str
    event_type: str
    aggregate_type: str
    aggregate_id: str
    occurred_at: str
    version: int
    payload: dict[str, Any]
    command_id: Optional[str] = None

    def as_envelope(self) -> dict[str, Any]:
        envelope = {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "aggregate_type": self.aggregate_type,
            "aggregate_id": self.aggregate_id,
            "occurred_at": self.occurred_at,
            "version": self.version,
            "payload": self.payload,
        }
        if self.command_id is not None:
            envelope["command_id"] = self.command_id
        return envelope


@dataclass(frozen=True)
class EventSpec:
    """一次命令内待写入的单个事件（版本由存储统一分配）。"""

    aggregate_type: str
    aggregate_id: str
    event_type: str
    payload: dict[str, Any]
    expected_version: int


def canonical_fingerprint(specs: Sequence[EventSpec]) -> str:
    body = [
        [s.aggregate_type, s.aggregate_id, s.event_type, s.payload]
        for s in specs
    ]
    return json.dumps(body, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


class EventStore:
    """按 (aggregate_type, aggregate_id) 维护版本的仅追加日志。

    同一 ``command_id`` 的重复提交：
    - 指纹一致 -> 不重复写入，返回首次产生的事件（关闭回执重放语义）；
    - 指纹不一致 -> 抛 IdempotencyConflictError，由服务层决定暂停案件。
    """

    def __init__(self, path: Optional[str | Path] = None) -> None:
        self._events: list[StoredEvent] = []
        self._versions: dict[tuple[str, str], int] = {}
        self._commands: dict[str, dict[str, Any]] = {}
        self._path = Path(path) if path else None
        if self._path and self._path.exists():
            self._load()

    # ----- 查询 -----

    def events_for(self, aggregate_type: str, aggregate_id: str) -> list[StoredEvent]:
        key = (aggregate_type, aggregate_id)
        return [e for e in self._events if (e.aggregate_type, e.aggregate_id) == key]

    def all_events(self) -> list[StoredEvent]:
        return list(self._events)

    def version_of(self, aggregate_type: str, aggregate_id: str) -> int:
        return self._versions.get((aggregate_type, aggregate_id), 0)

    def command_result(self, command_id: str) -> Optional[list[StoredEvent]]:
        record = self._commands.get(command_id)
        if record is None:
            return None
        return [self._events[i] for i in record["event_indexes"]]

    # ----- 写入 -----

    def commit(
        self,
        specs: Sequence[EventSpec],
        occurred_at: str,
        command_id: Optional[str] = None,
    ) -> list[StoredEvent]:
        if command_id is not None and command_id in self._commands:
            recorded = self._commands[command_id]
            if recorded["fingerprint"] != canonical_fingerprint(list(specs)):
                raise IdempotencyConflictError(
                    "相同编号的回执内容不一致（日期、范围或承接方不同），已暂停",
                    field="command_id",
                )
            return [self._events[i] for i in recorded["event_indexes"]]

        # 同一批次内允许对同一聚合写入多个事件（版本顺序递增）。
        # expected_version 表示该聚合在本批次开始时的基版本：
        # 首次出现时必须等于当前版本，同批次后续事件必须等于同一基版本。
        base_versions: dict[tuple[str, str], int] = {}
        for spec in specs:
            key = (spec.aggregate_type, spec.aggregate_id)
            if key not in base_versions:
                base_versions[key] = self._versions.get(key, 0)
            if spec.expected_version != base_versions[key]:
                raise ConcurrencyError(
                    f"聚合 {spec.aggregate_type}/{spec.aggregate_id} 版本已推进 "
                    f"（期望基版本 {base_versions[key]}，提交 {spec.expected_version}）",
                    field="version",
                )

        produced: list[StoredEvent] = []
        indexes: list[int] = []
        for order, spec in enumerate(specs):
            key = (spec.aggregate_type, spec.aggregate_id)
            new_version = self._versions.get(key, 0) + 1
            self._versions[key] = new_version
            event = StoredEvent(
                seq=len(self._events),
                event_id=(
                    f"{command_id}-{order}"
                    if command_id is not None
                    else f"evt-{uuid.uuid4().hex}"
                ),
                event_type=spec.event_type,
                aggregate_type=spec.aggregate_type,
                aggregate_id=spec.aggregate_id,
                occurred_at=occurred_at,
                version=new_version,
                payload=dict(spec.payload),
                command_id=command_id,
            )
            self._events.append(event)
            indexes.append(len(self._events) - 1)
            produced.append(event)

        if command_id is not None:
            self._commands[command_id] = {
                "fingerprint": canonical_fingerprint(list(specs)),
                "event_indexes": indexes,
            }
        if self._path is not None:
            self._persist(produced)
        return produced

    # ----- 持久化 -----

    def _persist(self, events: Iterable[StoredEvent]) -> None:
        assert self._path is not None
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with self._path.open("a", encoding="utf-8") as handle:
            for event in events:
                handle.write(json.dumps(event.as_envelope(), ensure_ascii=False) + "\n")
            handle.flush()

    def _load(self) -> None:
        assert self._path is not None
        for line in self._path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            raw = json.loads(line)
            event = StoredEvent(
                seq=len(self._events),
                event_id=raw["event_id"],
                event_type=raw["event_type"],
                aggregate_type=raw["aggregate_type"],
                aggregate_id=raw["aggregate_id"],
                occurred_at=raw["occurred_at"],
                version=raw["version"],
                payload=raw.get("payload", {}),
                command_id=raw.get("command_id"),
            )
            self._events.append(event)
            key = (event.aggregate_type, event.aggregate_id)
            self._versions[key] = max(self._versions.get(key, 0), event.version)
            if event.command_id is not None:
                record = self._commands.setdefault(
                    event.command_id, {"fingerprint": None, "event_indexes": []}
                )
                record["event_indexes"].append(len(self._events) - 1)
        # 恢复后按日志内同命令事件重算指纹；同一聚合的期望版本以首事件前一版为准。
        for record in self._commands.values():
            if record["fingerprint"] is not None:
                continue
            first_seen: dict[tuple[str, str], int] = {}
            specs: list[EventSpec] = []
            for i in record["event_indexes"]:
                event = self._events[i]
                key = (event.aggregate_type, event.aggregate_id)
                if key not in first_seen:
                    first_seen[key] = event.version - 1
                specs.append(
                    EventSpec(
                        event.aggregate_type,
                        event.aggregate_id,
                        event.event_type,
                        event.payload,
                        expected_version=first_seen[key],
                    )
                )
            record["fingerprint"] = canonical_fingerprint(specs)

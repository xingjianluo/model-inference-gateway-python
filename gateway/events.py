"""结构化事件记录：供单测断言命中/合并/降级等判定。

事件种类（kind）保持稳定字符串：
  quota_admit / quota_reject / cache_lookup / cache_put /
  downstream_result / coalesce / fallback_decision
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

DEFAULT_RECORDER: "EventRecorder | None" = None


@dataclass
class Event:
    request_id: str
    kind: str
    detail: dict[str, Any] = field(default_factory=dict)


class EventRecorder:
    """进程内事件记录器。

    网关运行在 asyncio 单线程事件循环中，所有调用点都在事件循环线程；
    列表 append/读取本身具备原子性，这里仍用简单锁保护快照拷贝，
    使从工作线程（若有）调用时也安全。
    """

    def __init__(self, max_events: int = 10_000) -> None:
        self._events: list[Event] = []
        self._max = max_events

    def record(self, request_id: str, kind: str, **detail: Any) -> None:
        self._events.append(Event(request_id=request_id, kind=kind, detail=dict(detail)))
        if len(self._events) > self._max:
            # 简单环形截断，避免无界增长
            del self._events[: len(self._events) - self._max]

    def snapshot(self) -> list[Event]:
        return list(self._events)

    def by_request(self, request_id: str) -> list[Event]:
        return [e for e in self._events if e.request_id == request_id]

    def kinds(self, request_id: str) -> list[str]:
        return [e.kind for e in self.by_request(request_id)]

    def clear(self) -> None:
        self._events.clear()


def get_default_recorder() -> EventRecorder:
    global DEFAULT_RECORDER
    if DEFAULT_RECORDER is None:
        DEFAULT_RECORDER = EventRecorder()
    return DEFAULT_RECORDER

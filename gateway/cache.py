"""相同输入的短窗口合并（single-flight）与结果缓存。

缓存规则
--------
- key = sha256(model + 规范化 JSON(inputs))，相同模型与输入视为同一请求。
- 仅 ``put_success`` 写入；下游超时/失败/异常结果绝不写缓存。
- TTL 惰性过期；命中/未命中均产生可观测事件。

合并规则
--------
- 合并窗口（coalesce_window）内到达的相同 key 请求，只有首个（leader）
  真正执行下游调用，其余（follower）共享同一份结果或同一个异常。
- 合并请求与单独请求返回值完全一致（同一 dict 内容）。
- follower 被客户端取消时只退出自己，不影响 leader；leader 被取消时
  取消共享的下游任务，follower 收到取消传播。
"""

from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Protocol

from gateway.config import CacheConfig
from gateway.events import EventRecorder
from gateway.logging_setup import get_logger

_log = get_logger("cache")

WorkFactory = Callable[[], Awaitable[dict[str, Any]]]


@dataclass(frozen=True)
class CacheSnapshot:
    hit: bool
    coalesced: bool
    key: str

    @property
    def source(self) -> str:
        if self.hit:
            return "cache"
        if self.coalesced:
            return "coalesced"
        return "downstream"


@dataclass
class CacheEntry:
    value: dict[str, Any]
    expires_at: float


class Clock(Protocol):
    def monotonic(self) -> float:
        ...


def make_key(model: str, inputs: Any) -> str:
    canonical = json.dumps(inputs, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    digest = hashlib.sha256(f"{model}\n{canonical}".encode("utf-8")).hexdigest()
    return f"{model}:{digest[:16]}"


class ResultCache:
    def __init__(
        self,
        ttl_seconds: float,
        events: EventRecorder,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._ttl = ttl_seconds
        self._events = events
        self._clock = clock
        self._entries: dict[str, CacheEntry] = {}

    def get(self, key: str, request_id: str) -> dict[str, Any] | None:
        now = self._clock()
        entry = self._entries.get(key)
        if entry is None:
            self._events.record(request_id, "cache_lookup", key=key, hit=False)
            _log.info("request_id=%s cache_lookup key=%s hit=False", request_id, key)
            return None
        if entry.expires_at <= now:
            del self._entries[key]
            self._events.record(request_id, "cache_lookup", key=key, hit=False, expired=True)
            _log.info("request_id=%s cache_lookup key=%s hit=False expired=True", request_id, key)
            return None
        self._events.record(request_id, "cache_lookup", key=key, hit=True)
        _log.info("request_id=%s cache_lookup key=%s hit=True", request_id, key)
        # 返回拷贝，避免调用方修改污染缓存
        return copy.deepcopy(entry.value)

    def put_success(self, key: str, value: dict[str, Any], request_id: str) -> None:
        if self._ttl <= 0:
            return
        self._entries[key] = CacheEntry(
            value=copy.deepcopy(value), expires_at=self._clock() + self._ttl
        )
        self._events.record(request_id, "cache_put", key=key, ttl=self._ttl)
        _log.info("request_id=%s cache_put key=%s ttl=%s", request_id, key, self._ttl)

    def clear(self) -> None:
        self._entries.clear()

    def size(self) -> int:
        return len(self._entries)


@dataclass
class _Flight:
    followers: list[asyncio.Future]
    join_closed: asyncio.Event
    done: asyncio.Event
    result: dict[str, Any] | None = None
    error: BaseException | None = None


class Coalescer:
    def __init__(self, window: float, events: EventRecorder) -> None:
        self._window = window
        self._events = events
        self._lock = asyncio.Lock()
        self._flights: dict[str, _Flight] = {}

    async def execute(
        self,
        key: str,
        work_factory: WorkFactory,
        request_id: str,
    ) -> tuple[dict[str, Any], bool]:
        async with self._lock:
            flight = self._flights.get(key)
            if flight is not None and not flight.join_closed.is_set():
                future: asyncio.Future = asyncio.get_running_loop().create_future()
                flight.followers.append(future)
                coalesced = True
                self._events.record(
                    request_id, "coalesce", key=key, role="follower", followers=len(flight.followers)
                )
                _log.info("request_id=%s coalesce key=%s role=follower", request_id, key)
            else:
                flight = _Flight(
                    followers=[],
                    join_closed=asyncio.Event(),
                    done=asyncio.Event(),
                )
                self._flights[key] = flight
                future = None
                coalesced = False
                self._events.record(request_id, "coalesce", key=key, role="leader")
                _log.info("request_id=%s coalesce key=%s role=leader", request_id, key)

        if coalesced:
            assert future is not None
            return await future, True  # type: ignore[return-value]

        return await self._run_leader(key, flight, work_factory, request_id), False

    async def _run_leader(
        self,
        key: str,
        flight: _Flight,
        work_factory: WorkFactory,
        request_id: str,
    ) -> dict[str, Any]:
        task = asyncio.ensure_future(work_factory())
        try:
            # 打开合并窗口：窗口期内允许 follower 挂入
            await asyncio.sleep(self._window)
            flight.join_closed.set()

            done, _ = await asyncio.wait({task})
            if task in done:
                exc = task.exception()
                if exc is not None:
                    raise exc
                flight.result = task.result()
            else:  # pragma: no cover - wait 已含任务，理论不会到这
                raise RuntimeError("unexpected leader state")
        except BaseException as exc:
            flight.error = exc
            if not task.done():
                task.cancel()
            self._finish(key, flight)
            raise
        self._finish(key, flight)
        return flight.result  # type: ignore[return-value]

    def _finish(self, key: str, flight: _Flight) -> None:
        flight.done.set()
        if self._flights.get(key) is flight:
            del self._flights[key]
        for fut in flight.followers:
            if fut.done():
                continue
            if flight.error is not None:
                fut.set_exception(flight.error)
            else:
                # 每个 follower 拿到独立深拷贝，互不影响
                fut.set_result(copy.deepcopy(flight.result))

    def active_keys(self) -> list[str]:
        return list(self._flights.keys())

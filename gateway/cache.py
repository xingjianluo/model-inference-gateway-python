"""短窗口请求合并（单飞）与结果缓存。

三级判定：
- HIT:       命中未过期缓存，直接返回缓存值（与新请求语义一致）。
- COALESCED: 相同 key 已有在途请求，等待同一个结果 future（成功/失败一致）。
- MISS:      成为 leader，负责调用下游，并以 set_result/set_failure 收尾。

保证：
- 成功才写缓存；超时或下游失败只传播异常，绝不写入结果缓存。
- 合并等待者用 asyncio.shield 等待，等待者被取消不会带走 leader。
- coalesce_window_seconds > 0 时，失败条目会在窗口内继续合并瞬时重试，
  避免失败风暴；这仍不是结果缓存（窗口过后自动清除），默认 0 表示关闭。
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from enum import Enum


class CacheStatus(str, Enum):
    HIT = "hit"
    COALESCED = "coalesced"
    MISS = "miss"


@dataclass
class _Cached:
    value: object
    expires_at: float


@dataclass
class _InFlight:
    future: asyncio.Future
    started_at: float
    failed_at: float | None = None


@dataclass
class CacheOutcome:
    key: str
    status: CacheStatus
    value: object | None = None
    future: asyncio.Future | None = None

    def as_log_fields(self) -> dict:
        return {"cache_key": self.key, "cache_status": self.status.value}


class CoalescingCache:
    def __init__(
        self,
        ttl_seconds: float = 30.0,
        coalesce_window_seconds: float = 0.0,
        *,
        clock=time.monotonic,
        loop: asyncio.AbstractEventLoop | None = None,
    ) -> None:
        self._ttl = max(0.0, float(ttl_seconds))
        self._window = max(0.0, float(coalesce_window_seconds))
        self._clock = clock
        self._loop = loop
        self._cached: dict[str, _Cached] = {}
        self._inflight: dict[str, _InFlight] = {}
        self._hits = 0
        self._misses = 0
        self._coalesced = 0

    def _get_loop(self) -> asyncio.AbstractEventLoop:
        if self._loop is None:
            self._loop = asyncio.get_running_loop()
        return self._loop

    def reconfigure(self, ttl_seconds: float, coalesce_window_seconds: float) -> None:
        self._ttl = max(0.0, float(ttl_seconds))
        self._window = max(0.0, float(coalesce_window_seconds))

    def lookup(self, key: str) -> CacheOutcome:
        """命中返回 HIT+value；在途返回 COALESCED+future；否则登记 leader 返回 MISS。"""
        now = self._clock()

        cached = self._cached.get(key)
        if cached is not None:
            if cached.expires_at > now:
                self._hits += 1
                return CacheOutcome(key, CacheStatus.HIT, value=cached.value)
            del self._cached[key]  # 惰性过期

        inflight = self._inflight.get(key)
        if inflight is not None:
            if (
                inflight.failed_at is not None
                and now - inflight.failed_at > self._window
            ):
                # 失败合并窗口已过：清旧条目，允许新的 leader
                self._inflight.pop(key, None)
            else:
                self._coalesced += 1
                return CacheOutcome(
                    key, CacheStatus.COALESCED, future=inflight.future
                )

        future = self._get_loop().create_future()
        self._inflight[key] = _InFlight(future=future, started_at=now)
        self._misses += 1
        return CacheOutcome(key, CacheStatus.MISS, future=future)

    def set_result(self, key: str, future: asyncio.Future, value: object) -> None:
        """leader 成功：写缓存并兑现所有合并者。"""
        now = self._clock()
        entry = self._inflight.get(key)
        if entry is None or entry.future is not future:
            # 已被 newer leader 取代或条目已清除：本次结果不再代表该 key
            return
        del self._inflight[key]
        if self._ttl > 0:
            self._cached[key] = _Cached(value=value, expires_at=now + self._ttl)
        if not future.done():
            future.set_result(value)

    def set_failure(self, key: str, future: asyncio.Future, exc: BaseException) -> None:
        """leader 失败：不写结果缓存；窗口开启则短暂保留失败用于合并，否则移除。

        leader 自身的取消（CancelledError）照常抛给 leader，但传播给合并
        等待者的异常归一化为 DownstreamInvokeError——内部取消语义不外泄。
        """
        now = self._clock()
        entry = self._inflight.get(key)
        if entry is None or entry.future is not future:
            return
        if isinstance(exc, asyncio.CancelledError):
            from .errors import DownstreamInvokeError

            propagated: BaseException = DownstreamInvokeError(
                "downstream request was cancelled", detail={"key": key}
            )
        else:
            propagated = exc
        if not future.done():
            future.set_exception(propagated)
        # 标记异常已被代表 leader 取回（等待者仍可 await 到同一异常），
        # 防止 leader 自身提前退出时出现 "exception never retrieved" 噪音。
        future.exception()
        if self._window <= 0:
            self._inflight.pop(key, None)
        else:
            entry.failed_at = now

    def stats(self) -> dict:
        return {
            "hits": self._hits,
            "misses": self._misses,
            "coalesced": self._coalesced,
            "cached_entries": len(self._cached),
            "inflight_keys": len(self._inflight),
            "ttl_seconds": self._ttl,
            "coalesce_window_seconds": self._window,
        }

    def clear(self) -> None:
        for entry in self._inflight.values():
            if not entry.future.done():
                entry.future.cancel()
        self._inflight.clear()
        self._cached.clear()

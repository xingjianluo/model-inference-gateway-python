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
from collections import OrderedDict
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
    # leader 收尾（set_result/set_failure）时使用的在途条目键；与 key 相同除非指定了 lane
    inflight_key: str | None = None

    def as_log_fields(self) -> dict:
        return {"cache_key": self.key, "cache_status": self.status.value}


class CoalescingCache:
    def __init__(
        self,
        ttl_seconds: float = 30.0,
        coalesce_window_seconds: float = 0.0,
        *,
        max_entries: int | None = None,
        clock=time.monotonic,
        loop: asyncio.AbstractEventLoop | None = None,
    ) -> None:
        self._ttl = max(0.0, float(ttl_seconds))
        self._window = max(0.0, float(coalesce_window_seconds))
        self._max_entries = max_entries if max_entries and max_entries > 0 else None
        self._clock = clock
        self._loop = loop
        self._cached: OrderedDict[str, _Cached] = OrderedDict()  # LRU：末尾最近使用
        self._inflight: dict[str, _InFlight] = {}
        self._hits = 0
        self._misses = 0
        self._coalesced = 0
        self._evictions = 0

    def _get_loop(self) -> asyncio.AbstractEventLoop:
        if self._loop is None:
            self._loop = asyncio.get_running_loop()
        return self._loop

    def reconfigure(
        self,
        ttl_seconds: float,
        coalesce_window_seconds: float,
        max_entries: int | None = None,
    ) -> None:
        self._ttl = max(0.0, float(ttl_seconds))
        self._window = max(0.0, float(coalesce_window_seconds))
        self._max_entries = max_entries if max_entries and max_entries > 0 else None
        self._enforce_capacity()

    def _enforce_capacity(self) -> None:
        """按最久未用顺序回收结果缓存条目；只影响后续是否命中，不触碰在途合并。"""
        if self._max_entries is None:
            return
        while len(self._cached) > self._max_entries:
            self._cached.popitem(last=False)
            self._evictions += 1

    def lookup(
        self,
        key: str,
        *,
        use_cache: bool = True,
        lane: str = "",
    ) -> CacheOutcome:
        """命中返回 HIT+value；在途返回 COALESCED+future；否则登记 leader 返回 MISS。

        use_cache=False：跳过结果缓存读（强一致请求），但仍参与在途合并登记。
        lane：在途合并的隔离通道；不同 lane 的相同 key 互不合并（结果缓存仍共享 key）。
        """
        now = self._clock()

        if use_cache:
            cached = self._cached.get(key)
            if cached is not None:
                if cached.expires_at > now:
                    self._cached.move_to_end(key)  # 命中即刷新 LRU 热度
                    self._hits += 1
                    return CacheOutcome(key, CacheStatus.HIT, value=cached.value)
                del self._cached[key]  # 惰性过期

        inflight_key = f"{lane}{key}" if lane else key
        inflight = self._inflight.get(inflight_key)
        if inflight is not None:
            if (
                inflight.failed_at is not None
                and now - inflight.failed_at > self._window
            ):
                # 失败合并窗口已过：清旧条目，允许新的 leader
                self._inflight.pop(inflight_key, None)
            else:
                self._coalesced += 1
                return CacheOutcome(
                    key, CacheStatus.COALESCED, future=inflight.future,
                    inflight_key=inflight_key,
                )

        future = self._get_loop().create_future()
        self._inflight[inflight_key] = _InFlight(future=future, started_at=now)
        self._misses += 1
        return CacheOutcome(key, CacheStatus.MISS, future=future,
                            inflight_key=inflight_key)

    def set_result(
        self,
        key: str,
        future: asyncio.Future,
        value: object,
        *,
        inflight_key: str | None = None,
        cache_result: bool = True,
    ) -> None:
        """leader 成功：兑现所有合并者；cache_result=True 时写入结果缓存。

        cache_result=False 用于降级结果：本次合并者仍拿到一致结果，但不留下
        缓存条目，下游恢复后同一输入会重新走主路径而不是复用降级结果。
        """
        now = self._clock()
        ikey = inflight_key or key
        entry = self._inflight.get(ikey)
        if entry is None or entry.future is not future:
            # 已被 newer leader 取代或条目已清除：本次结果不再代表该 key
            return
        del self._inflight[ikey]
        if self._ttl > 0 and cache_result:
            self._cached[key] = _Cached(value=value, expires_at=now + self._ttl)
            self._cached.move_to_end(key)
            self._enforce_capacity()
        if not future.done():
            future.set_result(value)

    def set_failure(
        self,
        key: str,
        future: asyncio.Future,
        exc: BaseException,
        *,
        inflight_key: str | None = None,
    ) -> None:
        """leader 失败：不写结果缓存；窗口开启则短暂保留失败用于合并，否则移除。

        leader 自身的取消（CancelledError）照常抛给 leader，但传播给合并
        等待者的异常归一化为 DownstreamInvokeError——内部取消语义不外泄。
        """
        now = self._clock()
        ikey = inflight_key or key
        entry = self._inflight.get(ikey)
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
            self._inflight.pop(ikey, None)
        else:
            entry.failed_at = now

    def stats(self) -> dict:
        return {
            "hits": self._hits,
            "misses": self._misses,
            "coalesced": self._coalesced,
            "evictions": self._evictions,
            "cached_entries": len(self._cached),
            "max_entries": self._max_entries,
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

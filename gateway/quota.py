"""按调用方维度的并发与速率配额管理。

并发语义
--------
- 每个 caller 维护一个精确的 ``in_flight`` 计数；acquire 在同一把异步锁内
  完成"读快照 -> 校验 -> 自增"，因此并发调整与请求同时发生时不会超卖。
- release 同样在锁内递减并做下限钳制，取消/超时路径由上层 finally 保证调用，
  计数不会漂移。
- 配额配置热更新只替换限额数值，不重置计数；缩小并发时，新请求按新限额拒绝，
  已在执行的请求自然排空（不抢占）。

速率语义
--------
每 caller 一个令牌桶，容量/补充速率均为 ``rate_per_second``；
不足 1 个令牌即拒绝，原因 rate_limited。
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass

from gateway.config import ConfigStore
from gateway.events import EventRecorder
from gateway.logging_setup import get_logger

_log = get_logger("quota")


@dataclass
class _RateState:
    tokens: float
    last_refill: float


@dataclass(frozen=True)
class QuotaDecision:
    allowed: bool
    caller: str
    reason: str  # "ok" | "concurrency_limited" | "rate_limited"
    in_flight: int
    max_concurrency: int
    rate_per_second: float
    retry_after: float | None = None


class QuotaManager:
    def __init__(self, config_store: ConfigStore, events: EventRecorder) -> None:
        self._store = config_store
        self._events = events
        self._lock = asyncio.Lock()
        self._in_flight: dict[str, int] = {}
        self._rate: dict[str, _RateState] = {}

    async def acquire(self, caller: str, request_id: str) -> QuotaDecision:
        async with self._lock:
            cfg = self._store.get().quota_for(caller)
            in_flight = self._in_flight.get(caller, 0)

            # 1) 并发优先判定（更能反映瞬时压力）
            if in_flight >= cfg.max_concurrency:
                decision = QuotaDecision(
                    False,
                    caller,
                    "concurrency_limited",
                    in_flight,
                    cfg.max_concurrency,
                    cfg.rate_per_second,
                )
                self._events.record(
                    request_id,
                    "quota_reject",
                    caller=caller,
                    reason=decision.reason,
                    in_flight=in_flight,
                    max_concurrency=cfg.max_concurrency,
                    rate_per_second=cfg.rate_per_second,
                )
                _log.info(
                    "request_id=%s quota_reject caller=%s reason=concurrency_limited "
                    "in_flight=%s max_concurrency=%s",
                    request_id,
                    caller,
                    in_flight,
                    cfg.max_concurrency,
                )
                return decision

            # 2) 速率判定（令牌桶）
            now = time.monotonic()
            state = self._rate.get(caller)
            if state is None:
                state = _RateState(tokens=cfg.rate_per_second, last_refill=now)
                self._rate[caller] = state
            self._refill(state, cfg.rate_per_second, now)

            if state.tokens < 1.0:
                retry_after = (1.0 - state.tokens) / cfg.rate_per_second if cfg.rate_per_second > 0 else None
                decision = QuotaDecision(
                    False,
                    caller,
                    "rate_limited",
                    in_flight,
                    cfg.max_concurrency,
                    cfg.rate_per_second,
                    retry_after=retry_after,
                )
                self._events.record(
                    request_id,
                    "quota_reject",
                    caller=caller,
                    reason=decision.reason,
                    in_flight=in_flight,
                    max_concurrency=cfg.max_concurrency,
                    rate_per_second=cfg.rate_per_second,
                    retry_after=retry_after,
                )
                _log.info(
                    "request_id=%s quota_reject caller=%s reason=rate_limited "
                    "tokens=%.3f rate_per_second=%s retry_after=%s",
                    request_id,
                    caller,
                    state.tokens,
                    cfg.rate_per_second,
                    retry_after,
                )
                return decision

            # 3) 扣令牌并占用槽位（同锁内完成，杜绝超卖）
            state.tokens -= 1.0
            new_in_flight = in_flight + 1
            self._in_flight[caller] = new_in_flight
            decision = QuotaDecision(
                True,
                caller,
                "ok",
                new_in_flight,
                cfg.max_concurrency,
                cfg.rate_per_second,
            )
            self._events.record(
                request_id,
                "quota_admit",
                caller=caller,
                in_flight=new_in_flight,
                max_concurrency=cfg.max_concurrency,
                rate_per_second=cfg.rate_per_second,
            )
            _log.info(
                "request_id=%s quota_admit caller=%s in_flight=%s/%s tokens_left=%.3f",
                request_id,
                caller,
                new_in_flight,
                cfg.max_concurrency,
                state.tokens,
            )
            return decision

    @staticmethod
    def _refill(state: _RateState, rate: float, now: float) -> None:
        elapsed = max(0.0, now - state.last_refill)
        if elapsed <= 0:
            return
        state.tokens = min(rate, state.tokens + elapsed * rate)
        state.last_refill = now

    async def release(self, caller: str) -> None:
        async with self._lock:
            current = self._in_flight.get(caller, 0)
            # 钳制到 0：任何异常路径重复释放也不会产生负计数（计数不漂移）
            self._in_flight[caller] = max(0, current - 1)

    async def in_flight_of(self, caller: str) -> int:
        async with self._lock:
            return self._in_flight.get(caller, 0)

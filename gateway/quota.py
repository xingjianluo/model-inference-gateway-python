"""调用方维度的并发槽 + 令牌桶速率配额。

关键约束：
- try_acquire 是非阻塞的：拿不到立即拒绝，绝不排队（排队会放大下游压力）。
- 所有计数在同一把锁下完成，临界区内没有 await，因此并发降配与请求准入
  交错时不会超卖或漂移。
- 降配只影响新准入；已受理请求的槽位在 release 时回收，计数单调一致。
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass


@dataclass
class _Counter:
    capacity: int | None
    rate: float | None
    active: int = 0
    tokens: float = 0.0
    last_refill: float = 0.0


@dataclass(frozen=True)
class QuotaDecision:
    """单次配额判定结果（也是日志里的“判定依据”）。"""

    allowed: bool
    reason: str  # "ok" | "concurrency_limited" | "rate_limited"
    caller: str
    active_concurrency: int
    max_concurrency: int | None
    rate_per_second: float | None
    available_tokens: float
    rule_version: int

    def as_log_fields(self) -> dict:
        return {
            "allowed": self.allowed,
            "reason": self.reason,
            "caller": self.caller,
            "active_concurrency": self.active_concurrency,
            "max_concurrency": self.max_concurrency,
            "rate_per_second": self.rate_per_second,
            "available_tokens": round(self.available_tokens, 3),
            "rule_version": self.rule_version,
        }


class QuotaManager:
    """按调用方维护并发计数与令牌桶，并响应配置热更新。

    全部状态仅在一把 threading.Lock 下读写且临界区内无 await，
    事件循环线程上的“准入 / 释放 / 热更新”因此天然串行。
    """

    def __init__(self, config=None, *, clock=time.monotonic) -> None:
        self._lock = threading.Lock()
        self._clock = clock
        self._config = config
        self._counters: dict[str, _Counter] = {}
        self._version = 0
        if config is not None:
            now = self._clock()
            for caller, rule in config.quotas.items():
                self._counters[caller] = self._new_counter(
                    rule.max_concurrency, rule.rate_per_second, now
                )

    @staticmethod
    def _new_counter(capacity: int | None, rate: float | None, now: float) -> _Counter:
        return _Counter(
            capacity=capacity,
            rate=rate,
            tokens=float(rate) if rate is not None else float("inf"),
            last_refill=now,
        )

    def apply_config(self, config) -> None:
        """根据新配置快照调整限额（与准入共用锁）。

        降并发立即生效（在途请求跑完前槽位只减不增，绝不超卖）；
        速率变更以当前时刻为起点重新填充，避免用旧速率补出大量令牌；
        升并发立即放行。
        """
        now = self._clock()
        with self._lock:
            self._config = config
            self._version += 1
            for caller, rule in config.quotas.items():
                c = self._counters.get(caller)
                if c is None:
                    self._counters[caller] = self._new_counter(
                        rule.max_concurrency, rule.rate_per_second, now
                    )
                else:
                    c.capacity = rule.max_concurrency
                    if c.rate != rule.rate_per_second:
                        c.rate = rule.rate_per_second
                        c.tokens = (
                            float(rule.rate_per_second)
                            if rule.rate_per_second is not None
                            else float("inf")
                        )
                        c.last_refill = now

    def _rule_for(self, caller: str):
        if self._config is None:
            from .config import QuotaRule

            return QuotaRule(caller=caller)
        return self._config.rule_for(caller)

    def _refill(self, c: _Counter, now: float) -> None:
        if c.rate is None:
            c.tokens = float("inf")
            return
        elapsed = now - c.last_refill
        if elapsed > 0:
            c.tokens = min(float(c.rate), c.tokens + elapsed * c.rate)
            c.last_refill = now

    def _sync_rule(self, c: _Counter, rule, now: float) -> None:
        c.capacity = rule.max_concurrency
        if c.rate != rule.rate_per_second:
            c.rate = rule.rate_per_second
            c.tokens = (
                float(rule.rate_per_second)
                if rule.rate_per_second is not None
                else float("inf")
            )
            c.last_refill = now

    def try_acquire(self, caller: str) -> QuotaDecision:
        """非阻塞申请一个并发槽与一个速率令牌；成功则计数已增加。"""
        now = self._clock()
        with self._lock:
            rule = self._rule_for(caller)
            c = self._counters.get(caller)
            if c is None:
                c = self._new_counter(rule.max_concurrency, rule.rate_per_second, now)
                self._counters[caller] = c
            else:
                # 兜底同步 apply_config 未覆盖到的、惰性创建的调用方
                self._sync_rule(c, rule, now)

            self._refill(c, now)

            if c.capacity is not None and c.active >= c.capacity:
                return QuotaDecision(
                    False, "concurrency_limited", caller, c.active,
                    c.capacity, c.rate, c.tokens, self._version,
                )
            if c.rate is not None and c.tokens < 1.0:
                return QuotaDecision(
                    False, "rate_limited", caller, c.active,
                    c.capacity, c.rate, c.tokens, self._version,
                )

            c.active += 1
            c.tokens -= 1.0
            return QuotaDecision(
                True, "ok", caller, c.active, c.capacity, c.rate, c.tokens, self._version,
            )

    def release(self, caller: str) -> None:
        """归还并发槽；重复/多余归还被忽略，计数不会漂移为负。"""
        with self._lock:
            c = self._counters.get(caller)
            if c is not None and c.active > 0:
                c.active -= 1

    def stats(self, caller: str | None = None) -> dict:
        now = self._clock()
        with self._lock:
            def view(name: str, c: _Counter) -> dict:
                self._refill(c, now)
                return {
                    "active": c.active,
                    "max_concurrency": c.capacity,
                    "rate_per_second": c.rate,
                    "available_tokens": None if c.rate is None else round(c.tokens, 3),
                }

            if caller is not None:
                c = self._counters.get(caller)
                return view(caller, c) if c is not None else {}
            return {name: view(name, c) for name, c in self._counters.items()}

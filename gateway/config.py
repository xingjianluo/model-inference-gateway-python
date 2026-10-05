"""配置模型与热更新存储。

配置在单个 asyncio 循环内通过一把锁串行化写入；读取拿到的是不可变快照，
因此“降配额”和“准入判定”并发发生时不会出现超卖或计数漂移。
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field, replace
from typing import Mapping


@dataclass(frozen=True)
class QuotaRule:
    """单个调用方的配额；None 表示不限制。"""

    caller: str
    max_concurrency: int | None = 8
    rate_per_second: float | None = None


@dataclass(frozen=True)
class FallbackPolicy:
    """降级策略。

    mode:
      - "fail":       主路径失败即归一化失败
      - "fallback":   主路径出现可降级错误时转入备用后端
    降级目标：fallback_backend 指定的命名后端。
    """

    mode: str = "fail"
    fallback_backend: str | None = None
    # 哪些错误码可以触发降级
    retry_on: frozenset[str] = frozenset(
        {"downstream_timeout", "downstream_error", "downstream_bad_result"}
    )


@dataclass(frozen=True)
class GatewayConfig:
    """一份不可变配置快照。"""

    quotas: Mapping[str, QuotaRule] = field(default_factory=dict)
    default_rule: QuotaRule = QuotaRule(caller="__default__")
    cache_ttl_seconds: float = 30.0
    cache_max_entries: int = 1024
    coalesce_window_seconds: float = 0.0
    downstream_timeout_seconds: float = 10.0
    primary_backend: str = "echo"
    backends: Mapping[str, dict] = field(default_factory=dict)
    fallback: FallbackPolicy = FallbackPolicy()

    def rule_for(self, caller: str) -> QuotaRule:
        return self.quotas.get(caller, replace(self.default_rule, caller=caller))


class ConfigStore:
    """进程内配置存储，支持不重启进程的原子热更新。

    所有写入经由同一把 asyncio 锁串行化；监听器同步执行（仅做限额调整等
    常量时间操作），因此“配置生效”与“准入判定”之间有确定的先后顺序，
    不会出现配额超卖或计数漂移。
    """

    def __init__(self, initial: GatewayConfig | None = None) -> None:
        self._snapshot = initial if initial is not None else GatewayConfig()
        self._lock = asyncio.Lock()
        self._listeners: list = []

    def snapshot(self) -> GatewayConfig:
        return self._snapshot

    def subscribe(self, listener) -> None:
        """注册配置变更监听器：listener(old, new)（同步、不得 await）。"""
        self._listeners.append(listener)

    async def update(self, config: GatewayConfig) -> GatewayConfig:
        """原子替换整份配置；返回新快照。"""
        async with self._lock:
            return self._commit(config)

    async def update_quotas(self, rules: Mapping[str, QuotaRule]) -> GatewayConfig:
        async with self._lock:
            return self._commit(replace(self._snapshot, quotas=dict(rules)))

    async def update_fallback(self, policy: FallbackPolicy) -> GatewayConfig:
        async with self._lock:
            return self._commit(replace(self._snapshot, fallback=policy))

    def _commit(self, new: GatewayConfig) -> GatewayConfig:
        old, self._snapshot = self._snapshot, new
        # 监听器同步应用变更；任何监听器异常都不应破坏已提交的配置。
        for listener in list(self._listeners):
            try:
                listener(old, new)
            except Exception:  # pragma: no cover - 防御性
                pass
        return new


def config_from_dict(data: Mapping[str, object]) -> GatewayConfig:
    """从普通 dict（如解析后的 JSON）构造一份配置；缺省项走默认值。"""

    quotas: dict[str, QuotaRule] = {}
    for item in data.get("quotas", []) or []:
        rule = QuotaRule(
            caller=item["caller"],
            max_concurrency=item.get("max_concurrency", 8),
            rate_per_second=item.get("rate_per_second"),
        )
        quotas[rule.caller] = rule

    default_data = data.get("default_quotas") or {}
    default_rule = QuotaRule(
        caller="__default__",
        max_concurrency=default_data.get("max_concurrency", 8),
        rate_per_second=default_data.get("rate_per_second"),
    )

    fb_data = data.get("fallback") or {}
    fallback = FallbackPolicy(
        mode=fb_data.get("mode", "fail"),
        fallback_backend=fb_data.get("fallback_backend"),
        retry_on=frozenset(fb_data.get("retry_on", FallbackPolicy().retry_on)),
    )

    return GatewayConfig(
        quotas=quotas,
        default_rule=default_rule,
        cache_ttl_seconds=float(data.get("cache_ttl_seconds", 30.0)),
        cache_max_entries=int(data.get("cache_max_entries", 1024)),
        coalesce_window_seconds=float(data.get("coalesce_window_seconds", 0.0)),
        downstream_timeout_seconds=float(data.get("downstream_timeout_seconds", 10.0)),
        primary_backend=str(data.get("primary_backend", "echo")),
        backends=dict(data.get("backends") or {}),
        fallback=fallback,
    )

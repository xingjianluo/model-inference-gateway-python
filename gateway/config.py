"""配置模型与可热更新的配置存储。

配置对象是 *不可变* 快照：每次请求受理时持有一份快照引用，
切换降级开关等变更只影响新受理的请求，已受理请求行为保持一致。
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field, replace
from enum import Enum
from pathlib import Path
from typing import Any, Protocol

from gateway.errors import InvalidRequestError


class FallbackMode(str, Enum):
    """降级策略：明确失败 / 转备用路径。"""

    FAIL = "fail"
    FALLBACK = "fallback"


@dataclass(frozen=True)
class QuotaConfig:
    caller: str
    max_concurrency: int
    rate_per_second: float

    @staticmethod
    def from_dict(caller: str, raw: dict[str, Any]) -> "QuotaConfig":
        return QuotaConfig(
            caller=caller,
            max_concurrency=_nonneg_int(raw["max_concurrency"], "max_concurrency"),
            rate_per_second=_nonneg_float(raw["rate_per_second"], "rate_per_second"),
        )


@dataclass(frozen=True)
class CacheConfig:
    ttl_seconds: float = 30.0
    coalesce_window: float = 0.02


@dataclass(frozen=True)
class TimeoutConfig:
    downstream_timeout: float = 2.0
    fallback_timeout: float = 2.0


@dataclass(frozen=True)
class ModelRoute:
    model: str
    primary: str
    fallback: str | None = None

    @staticmethod
    def from_dict(model: str, raw: dict[str, Any]) -> "ModelRoute":
        primary = raw.get("primary", model)
        fb = raw.get("fallback")
        return ModelRoute(model=model, primary=str(primary), fallback=None if fb is None else str(fb))


@dataclass(frozen=True)
class GatewayConfig:
    quotas: dict[str, QuotaConfig] = field(default_factory=dict)
    cache: CacheConfig = field(default_factory=CacheConfig)
    timeout: TimeoutConfig = field(default_factory=TimeoutConfig)
    routes: dict[str, ModelRoute] = field(default_factory=dict)
    fallback_mode: FallbackMode = FallbackMode.FAIL
    default_max_concurrency: int = 8
    default_rate_per_second: float = 20.0

    def quota_for(self, caller: str) -> QuotaConfig:
        q = self.quotas.get(caller)
        if q is not None:
            return q
        return QuotaConfig(
            caller=caller,
            max_concurrency=self.default_max_concurrency,
            rate_per_second=self.default_rate_per_second,
        )

    def route_for(self, model: str) -> ModelRoute:
        return self.routes.get(model, ModelRoute(model=model, primary=model))

    def with_(self, **changes: Any) -> "GatewayConfig":
        """返回带局部修改的新快照（dataclasses.replace 的语义化封装）。"""
        return replace(self, **changes)


class ConfigStore(Protocol):
    """配置存储协议：读取不可变快照、热更新。"""

    def get(self) -> GatewayConfig:
        ...

    def replace(self, config: GatewayConfig) -> None:
        ...


def _nonneg_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise InvalidRequestError(f"{name} 必须是非负整数")
    return value


def _nonneg_float(value: Any, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value < 0:
        raise InvalidRequestError(f"{name} 必须是非负数字")
    return float(value)


def parse_config(raw: dict[str, Any]) -> GatewayConfig:
    """从普通 dict（如 JSON 请求体）解析配置，非法输入抛 InvalidRequestError。"""
    cache_raw = raw.get("cache", {}) or {}
    timeout_raw = raw.get("timeout", {}) or {}
    cache = CacheConfig(
        ttl_seconds=_nonneg_float(cache_raw.get("ttl_seconds", 30.0), "cache.ttl_seconds"),
        coalesce_window=_nonneg_float(
            cache_raw.get("coalesce_window", 0.02), "cache.coalesce_window"
        ),
    )
    timeout = TimeoutConfig(
        downstream_timeout=_nonneg_float(
            timeout_raw.get("downstream_timeout", 2.0), "timeout.downstream_timeout"
        ),
        fallback_timeout=_nonneg_float(
            timeout_raw.get("fallback_timeout", 2.0), "timeout.fallback_timeout"
        ),
    )
    quotas = {
        str(caller): QuotaConfig.from_dict(str(caller), dict(q))
        for caller, q in (raw.get("quotas", {}) or {}).items()
    }
    routes = {
        str(model): ModelRoute.from_dict(str(model), dict(r))
        for model, r in (raw.get("routes", {}) or {}).items()
    }
    mode_raw = str(raw.get("fallback_mode", FallbackMode.FAIL.value))
    try:
        mode = FallbackMode(mode_raw)
    except ValueError:
        raise InvalidRequestError(
            f"fallback_mode 必须是 {[m.value for m in FallbackMode]}"
        ) from None
    return GatewayConfig(
        quotas=quotas,
        cache=cache,
        timeout=timeout,
        routes=routes,
        fallback_mode=mode,
        default_max_concurrency=_nonneg_int(
            raw.get("default_max_concurrency", 8), "default_max_concurrency"
        ),
        default_rate_per_second=_nonneg_float(
            raw.get("default_rate_per_second", 20.0), "default_rate_per_second"
        ),
    )


def load_config(path: str | Path) -> GatewayConfig:
    with open(path, "r", encoding="utf-8") as fh:
        return parse_config(json.load(fh))

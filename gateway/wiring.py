"""默认装配：内存配置存储 + 可脚本化后端，零外部依赖即可本地运行/测试。"""

from __future__ import annotations

import asyncio

from gateway.cache import Coalescer, ResultCache
from gateway.config import GatewayConfig
from gateway.downstream import DownstreamClient, ScriptedBackend, ScriptedBehavior
from gateway.events import EventRecorder
from gateway.fallback import FallbackExecutor
from gateway.logging_setup import configure_logging
from gateway.quota import QuotaManager
from gateway.service import InferenceGateway


class InMemoryConfigStore:
    """持有不可变配置快照，replace 原子替换，热更新无需重启进程。"""

    def __init__(self, config: GatewayConfig) -> None:
        self._config = config
        self._lock = asyncio.Lock()

    def get(self) -> GatewayConfig:
        return self._config

    def replace(self, config: GatewayConfig) -> None:
        # 单次属性赋值在 asyncio 单线程下是原子的：
        # 进行中的请求仍持有旧快照引用，新请求读到新快照。
        self._config = config

    async def replace_async(self, config: GatewayConfig) -> None:
        async with self._lock:
            self._config = config


def build_gateway(
    config: GatewayConfig,
    *,
    behaviors: dict[str, ScriptedBehavior] | None = None,
    backend: ScriptedBackend | None = None,
    events: EventRecorder | None = None,
    store: InMemoryConfigStore | None = None,
    configure_log: bool = True,
) -> tuple[InferenceGateway, InMemoryConfigStore, EventRecorder, ScriptedBackend]:
    """装配网关，返回 (网关, 配置存储, 事件记录器, 脚本后端) 便于测试。"""
    if configure_log:
        configure_logging()
    store = store or InMemoryConfigStore(config)
    recorder = events or EventRecorder()
    if backend is None:
        backend = ScriptedBackend(behaviors=behaviors or {})
    client = DownstreamClient(backend, config.timeout, recorder)
    quota = QuotaManager(store, recorder)
    cache = ResultCache(config.cache.ttl_seconds, recorder)
    coalescer = Coalescer(config.cache.coalesce_window, recorder)
    fallback = FallbackExecutor(client, recorder)
    gateway = InferenceGateway(
        config_store=store,
        quota=quota,
        cache=cache,
        coalescer=coalescer,
        client=client,
        fallback=fallback,
        events=recorder,
    )
    return gateway, store, recorder, backend


def demo_config() -> GatewayConfig:
    """开箱即用的演示配置（含两个调用方、一个模型的主/备路径）。"""
    from gateway.config import (
        CacheConfig,
        FallbackMode,
        ModelRoute,
        QuotaConfig,
        TimeoutConfig,
    )

    return GatewayConfig(
        quotas={
            "tenant-a": QuotaConfig("tenant-a", max_concurrency=2, rate_per_second=5.0),
            "tenant-b": QuotaConfig("tenant-b", max_concurrency=1, rate_per_second=2.0),
        },
        cache=CacheConfig(ttl_seconds=30.0, coalesce_window=0.05),
        timeout=TimeoutConfig(downstream_timeout=0.5, fallback_timeout=0.5),
        routes={
            "gpt-demo": ModelRoute(
                model="gpt-demo", primary="primary-demo", fallback="backup-demo"
            )
        },
        fallback_mode=FallbackMode.FALLBACK,
        default_max_concurrency=4,
        default_rate_per_second=10.0,
    )

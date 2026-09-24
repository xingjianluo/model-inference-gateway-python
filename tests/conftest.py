"""共享测试夹具：全部基于内存脚本后端，无外部服务/真实凭据。"""

from __future__ import annotations

import pytest

from gateway.config import (
    CacheConfig,
    FallbackMode,
    GatewayConfig,
    ModelRoute,
    QuotaConfig,
    TimeoutConfig,
)
from gateway.downstream import ScriptedBehavior, ScriptedBackend
from gateway.events import EventRecorder
from gateway.wiring import InMemoryConfigStore, build_gateway


def make_config(**overrides) -> GatewayConfig:
    cfg = GatewayConfig(
        quotas={
            "a": QuotaConfig("a", max_concurrency=2, rate_per_second=100.0),
            "b": QuotaConfig("b", max_concurrency=1, rate_per_second=100.0),
        },
        cache=CacheConfig(ttl_seconds=30.0, coalesce_window=0.05),
        timeout=TimeoutConfig(downstream_timeout=0.3, fallback_timeout=0.3),
        routes={"m": ModelRoute(model="m", primary="p", fallback="fb")},
        fallback_mode=FallbackMode.FAIL,
        default_max_concurrency=3,
        default_rate_per_second=100.0,
    )
    for key, value in overrides.items():
        cfg = cfg.with_(**{key: value})
    return cfg


@pytest.fixture
def events():
    return EventRecorder()


@pytest.fixture
def config():
    return make_config()


@pytest.fixture
def store(config):
    return InMemoryConfigStore(config)


@pytest.fixture
def fast_backend():
    return ScriptedBackend(
        behaviors={
            "p": ScriptedBehavior(delay=0.0, value={"output": {"answer": 42}}),
            "fb": ScriptedBehavior(delay=0.0, value={"output": {"answer": "backup"}}),
        }
    )


@pytest.fixture
def harness(store, fast_backend, events):
    gw, _, _, backend = build_gateway(
        store.get(),
        backend=fast_backend,
        events=events,
        store=store,
        configure_log=False,
    )

    class _Harness:
        pass

    h = _Harness()
    h.gw = gw
    h.store = store
    h.events = events
    h.backend = backend
    return h

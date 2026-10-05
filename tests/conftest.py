"""pytest 公共夹具：可注入时钟、内置桩后端、零外部依赖。"""

from __future__ import annotations

import logging

import pytest

from gateway.config import ConfigStore, FallbackPolicy, GatewayConfig, QuotaRule
from gateway.logging_setup import configure_logging
from gateway.service import GatewayService


class FakeClock:
    """手动推进的单调时钟，供令牌桶/TTL 做确定性测试。"""

    def __init__(self, start: float = 1000.0) -> None:
        self.value = start

    def __call__(self) -> float:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += seconds


@pytest.fixture(autouse=True)
def _gateway_logger():
    """让单测日志打印请求标识、配额判定依据与下游结果（pytest log_cli 输出）。"""
    logger = configure_logging(logging.DEBUG)
    return logger


class _JsonIntoMessage(logging.Filter):
    """把结构化字段拼进 record.msg，使 pytest live log 的 %(message)s 行包含它们。"""

    _FIELDS = (
        "event", "request_id", "caller", "allowed", "reason",
        "active_concurrency", "max_concurrency", "rate_per_second",
        "available_tokens", "rule_version", "cache_key", "cache_status",
        "backend", "primary", "error_code", "degraded", "latency_ms",
        "consistency", "source", "freshness", "produced_at",
    )

    def filter(self, record: logging.LogRecord) -> bool:
        if getattr(record, "_gateway_fields_appended", False):
            return True
        parts = []
        for name in self._FIELDS:
            if hasattr(record, name):
                parts.append(f"{name}={getattr(record, name)}")
        if parts:
            record.msg = f"{record.getMessage()} | " + " ".join(parts)
            record.args = ()
            record._gateway_fields_appended = True
        return True


def _attach_pytest_log_filter() -> None:
    # 直接挂在 gateway logger 上：无论 pytest 的 LogCaptureHandler 何时创建，
    # 记录都会先经过该 filter，把结构化字段并入 live log 的消息行。
    gateway_logger = logging.getLogger("gateway")
    if not any(isinstance(f, _JsonIntoMessage) for f in gateway_logger.filters):
        gateway_logger.addFilter(_JsonIntoMessage())


_attach_pytest_log_filter()


@pytest.fixture
def fake_clock():
    return FakeClock()


def make_config(**overrides) -> GatewayConfig:
    defaults = dict(
        quotas={"c1": QuotaRule("c1", max_concurrency=2, rate_per_second=2.0),
                "c2": QuotaRule("c2", max_concurrency=1, rate_per_second=None)},
        default_rule=QuotaRule("__default__", max_concurrency=3, rate_per_second=None),
        cache_ttl_seconds=30.0,
        coalesce_window_seconds=0.0,
        downstream_timeout_seconds=1.0,
        primary_backend="primary",
        backends={},
        fallback=FallbackPolicy(mode="fail"),
    )
    defaults.update(overrides)
    return GatewayConfig(**defaults)


@pytest.fixture
def make_service(fake_clock):
    """工厂：(config, backends) -> (service, store)。"""

    def _make(config: GatewayConfig | None = None, backends=None):
        store = ConfigStore(config or make_config())
        service = GatewayService(store, backends=backends or {}, clock=fake_clock)
        return service, store

    return _make

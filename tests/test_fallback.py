"""降级与错误归一化测试。"""

from __future__ import annotations

import asyncio

import pytest

from gateway.config import FallbackMode, QuotaConfig
from gateway.downstream import DownstreamFailureKind, ScriptedBehavior
from gateway.errors import (
    DownstreamBadResponseError,
    DownstreamTimeoutError,
    DownstreamUnavailableError,
    FallbackFailedError,
)

pytestmark = pytest.mark.anyio


def _bump(harness, n=10):
    cfg = harness.store.get()
    harness.store.replace(
        cfg.with_(
            quotas={
                "a": QuotaConfig("a", max_concurrency=n, rate_per_second=1000.0),
                "b": cfg.quotas["b"],
            }
        )
    )


async def test_fail_mode_raises_normalized_error(harness):
    _bump(harness)
    assert harness.store.get().fallback_mode is FallbackMode.FAIL
    harness.backend.behaviors["p"] = ScriptedBehavior(
        fail=DownstreamFailureKind.UNAVAILABLE
    )
    with pytest.raises(DownstreamUnavailableError) as ei:
        await harness.gw.infer(request_id="fb-0", caller="a", model="m", inputs={"i": 1})
    # 错误消息不含内部异常文本（脚本里抛的是 RuntimeError）
    assert "simulated connection failure" not in str(ei.value)

    dec = next(e for e in harness.events.by_request("fb-0") if e.kind == "fallback_decision")
    assert dec.detail["used"] is False
    assert dec.detail["reason"] == "mode_fail"


async def test_fallback_mode_routes_to_backup(harness):
    _bump(harness)
    cfg = harness.store.get().with_(fallback_mode=FallbackMode.FALLBACK)
    harness.store.replace(cfg)
    harness.backend.behaviors["p"] = ScriptedBehavior(
        fail=DownstreamFailureKind.TIMEOUT
    )
    r = await harness.gw.infer(request_id="fb-1", caller="a", model="m", inputs={"i": 1})
    assert r.source == "fallback"
    assert r.degraded is True
    assert r.fallback_target == "fb"
    assert r.output["output"] == {"answer": "backup"}

    dec = next(e for e in harness.events.by_request("fb-1") if e.kind == "fallback_decision")
    assert dec.detail["used"] is True
    assert harness.backend.call_counts["p"] == 1
    assert harness.backend.call_counts["fb"] == 1


async def test_backup_also_fails_maps_to_fallback_failed(harness):
    _bump(harness)
    cfg = harness.store.get().with_(fallback_mode=FallbackMode.FALLBACK)
    harness.store.replace(cfg)
    harness.backend.behaviors["p"] = ScriptedBehavior(
        fail=DownstreamFailureKind.TIMEOUT
    )
    harness.backend.behaviors["fb"] = ScriptedBehavior(
        fail=DownstreamFailureKind.UNAVAILABLE
    )
    with pytest.raises(FallbackFailedError):
        await harness.gw.infer(request_id="fb-2", caller="a", model="m", inputs={"i": 1})


async def test_bad_response_is_distinct_error(harness):
    _bump(harness)
    harness.backend.behaviors["p"] = ScriptedBehavior(
        fail=DownstreamFailureKind.BAD_RESPONSE
    )
    with pytest.raises(DownstreamBadResponseError):
        await harness.gw.infer(request_id="fb-3", caller="a", model="m", inputs={"i": 1})


async def test_switch_during_inflight_keeps_admitted_semantics(harness):
    """切换期一致性：受理后开关切换，已受理请求仍按受理时快照（FALLBACK）走备用。"""
    _bump(harness)
    harness.store.replace(harness.store.get().with_(fallback_mode=FallbackMode.FALLBACK))

    async def backup_work(target, payload):
        return {"output": {"answer": "late-backup"}}

    class _TwoSided:
        def __init__(self):
            self.calls = []

        async def infer(self, target, payload):
            self.calls.append(target)
            if target == "p":
                harness.store.replace(harness.store.get().with_(fallback_mode=FallbackMode.FAIL))
                await asyncio.sleep(0.05)
                raise RuntimeError("boom")
            return await backup_work(target, payload)

    from gateway.downstream import DownstreamClient
    from gateway.events import EventRecorder
    from gateway.fallback import FallbackExecutor

    two = _TwoSided()
    events = EventRecorder()
    cfg = harness.store.get()
    client = DownstreamClient(two, cfg.timeout, events)
    from gateway.cache import Coalescer, ResultCache
    from gateway.quota import QuotaManager
    from gateway.service import InferenceGateway

    gw = InferenceGateway(
        config_store=harness.store,
        quota=QuotaManager(harness.store, events),
        cache=ResultCache(cfg.cache.ttl_seconds, events),
        coalescer=Coalescer(cfg.cache.coalesce_window, events),
        client=client,
        fallback=FallbackExecutor(client, events),
        events=events,
    )
    r = await gw.infer(request_id="fb-4", caller="a", model="m", inputs={"i": 1})
    assert r.source == "fallback"
    assert r.degraded is True
    # 新受理的请求则按新开关 FAIL 处理
    with pytest.raises(DownstreamUnavailableError):
        await gw.infer(request_id="fb-5", caller="a", model="m", inputs={"i": 1})

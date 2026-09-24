"""并发与速率配额测试。

日志要求：单测运行时可看到 request_id、quota_admit/reject 的判定依据
（in_flight/max_concurrency/tokens），见 gateway.quota 的 info 日志。
"""

from __future__ import annotations

import asyncio

import pytest

from gateway.config import FallbackMode, QuotaConfig
from gateway.downstream import DownstreamFailureKind, ScriptedBehavior
from gateway.errors import (
    ConcurrencyLimitedError,
    DownstreamTimeoutError,
    RateLimitedError,
)

pytestmark = pytest.mark.anyio


async def test_concurrency_limit_rejects_with_distinct_reason(harness):
    # caller "a" 并发上限 2；后端阻塞，制造在飞请求
    harness.backend.behaviors["p"] = ScriptedBehavior(delay=0.2)
    results = []

    async def call(rid: str):
        try:
            r = await harness.gw.infer(request_id=rid, caller="a", model="m", inputs={"i": 1})
            results.append(("ok", r.source))
        except ConcurrencyLimitedError as exc:
            results.append(("concurrency_limited", exc.code.value))

    await asyncio.gather(*(call(f"c-{i}") for i in range(4)))
    ok = [r for r in results if r[0] == "ok"]
    rejected = [r for r in results if r[0] == "concurrency_limited"]
    assert len(ok) == 2
    assert len(rejected) == 2

    # 拒绝原因事件可观测
    for rid in ("c-2", "c-3"):
        kinds = harness.events.kinds(rid)
        assert "quota_reject" in kinds
    detail = next(
        e.detail
        for e in harness.events.snapshot()
        if e.kind == "quota_reject" and e.detail["reason"] == "concurrency_limited"
    )
    assert detail["in_flight"] == 2
    assert detail["max_concurrency"] == 2

    # 全部结束后槽位释放干净（计数不漂移）
    await asyncio.sleep(0)
    from gateway.quota import QuotaManager  # noqa: F401
    assert await harness.gw._quota.in_flight_of("a") == 0


async def test_rate_limit_distinct_from_concurrency(harness):
    # 速率 2/s、容量 2；前 2 个通过（含初始桶），第 3 个立即因速率被拒
    cfg = harness.store.get()
    harness.store.replace(
        cfg.with_(
            quotas={"a": QuotaConfig("a", max_concurrency=10, rate_per_second=2.0)}
        )
    )
    harness.backend.behaviors["p"] = ScriptedBehavior(delay=0.0)

    async def call(rid: str):
        try:
            await harness.gw.infer(request_id=rid, caller="a", model="m", inputs={"i": rid})
            return "ok"
        except RateLimitedError:
            return "rate_limited"
        except ConcurrencyLimitedError:
            return "concurrency_limited"

    outcomes = await asyncio.gather(*(call(f"r-{i}") for i in range(3)))
    assert outcomes.count("ok") == 2
    assert outcomes.count("rate_limited") == 1
    assert "concurrency_limited" not in outcomes

    # 等待补充后可以再次通过
    await asyncio.sleep(1.05)
    assert await call("r-later") == "ok"


async def test_hot_quota_shrink_takes_effect_without_restart(harness):
    harness.backend.behaviors["p"] = ScriptedBehavior(delay=0.2)

    async def one(rid: str):
        return await harness.gw.infer(request_id=rid, caller="a", model="m", inputs={"i": 1})

    # 先占 2 个槽（旧上限）
    t1 = asyncio.create_task(one("h-1"))
    t2 = asyncio.create_task(one("h-2"))
    await asyncio.sleep(0.02)

    # 热更新把上限缩到 1；计数不重置，仍有 2 在飞
    cfg = harness.store.get()
    harness.store.replace(
        cfg.with_(quotas={"a": QuotaConfig("a", max_concurrency=1, rate_per_second=100.0)})
    )
    with pytest.raises(ConcurrencyLimitedError):
        await harness.gw.infer(request_id="h-3", caller="a", model="m", inputs={"i": 1})

    await asyncio.gather(t1, t2)
    # 扩容到 5 后立即生效
    cfg = harness.store.get()
    harness.store.replace(
        cfg.with_(quotas={"a": QuotaConfig("a", max_concurrency=5, rate_per_second=100.0)})
    )
    harness.backend.behaviors["p"] = ScriptedBehavior(delay=0.0)
    r = await harness.gw.infer(request_id="h-4", caller="a", model="m", inputs={"i": 9})
    assert r.source in {"downstream", "cache"}


async def test_no_oversell_under_contention(harness):
    # 高并发抢占：max_concurrency=3，任何时刻在飞不得超过 3
    harness.backend.behaviors["p"] = ScriptedBehavior(delay=0.05)
    cfg = harness.store.get()
    harness.store.replace(
        cfg.with_(quotas={"a": QuotaConfig("a", max_concurrency=3, rate_per_second=10000.0)})
    )
    observed_max = 0
    lock = asyncio.Lock()

    async def call(rid: str):
        nonlocal observed_max
        try:
            await harness.gw.infer(request_id=rid, caller="a", model="m", inputs={"i": rid})
        except ConcurrencyLimitedError:
            pass
        inflight = await harness.gw._quota.in_flight_of("a")
        async with lock:
            observed_max = max(observed_max, inflight)

    await asyncio.gather(*(call(f"s-{i}") for i in range(30)))
    assert observed_max <= 3
    assert await harness.gw._quota.in_flight_of("a") == 0


async def test_timeout_releases_quota_slot(harness):
    harness.backend.behaviors["p"] = ScriptedBehavior(
        fail=DownstreamFailureKind.TIMEOUT
    )
    cfg = harness.store.get()
    harness.store.replace(cfg.with_(fallback_mode=FallbackMode.FAIL))
    with pytest.raises(DownstreamTimeoutError):
        await harness.gw.infer(request_id="t-1", caller="a", model="m", inputs={"i": 1})
    # 超时后槽位必须释放，后续请求不被"幽灵占用"挡住
    harness.backend.behaviors["p"] = ScriptedBehavior(delay=0.0)
    r = await harness.gw.infer(request_id="t-2", caller="a", model="m", inputs={"i": 1})
    assert r.source in {"downstream", "cache"}
    assert await harness.gw._quota.in_flight_of("a") == 0

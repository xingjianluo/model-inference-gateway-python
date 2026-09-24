"""合并与缓存测试：命中/未命中/合并判定可观测，失败不写缓存，语义一致。"""

from __future__ import annotations

import asyncio

import pytest

from gateway.downstream import DownstreamFailureKind, ScriptedBehavior

pytestmark = pytest.mark.anyio


async def test_cache_hit_miss_observable(harness):
    harness.backend.behaviors["p"] = ScriptedBehavior(delay=0.0)
    r1 = await harness.gw.infer(request_id="k-1", caller="a", model="m", inputs={"x": 1})
    r2 = await harness.gw.infer(request_id="k-2", caller="a", model="m", inputs={"x": 1})
    assert r1.source == "downstream"
    assert r2.source == "cache"
    assert r1.output["output"] == r2.output["output"]

    kinds = {e.kind for e in harness.events.by_request("k-2")}
    assert "cache_lookup" in kinds
    hit_ev = next(e for e in harness.events.by_request("k-2") if e.kind == "cache_lookup")
    assert hit_ev.detail["hit"] is True
    miss_ev = next(e for e in harness.events.by_request("k-1") if e.kind == "cache_lookup")
    assert miss_ev.detail["hit"] is False


async def test_different_inputs_different_keys(harness):
    harness.backend.behaviors["p"] = ScriptedBehavior(delay=0.0)
    r1 = await harness.gw.infer(request_id="d-1", caller="a", model="m", inputs={"x": 1})
    r2 = await harness.gw.infer(request_id="d-2", caller="a", model="m", inputs={"x": 2})
    assert r1.source == "downstream"
    assert r2.source == "downstream"
    assert harness.backend.call_counts["p"] == 2


async def test_coalesced_requests_share_one_downstream_call(harness):
    # 窗口 0.05s，后端耗时 0.15s：后两个请求落在窗口内被合并
    _bump(harness, 10)
    harness.backend.behaviors["p"] = ScriptedBehavior(
        delay=0.15, value={"output": {"answer": 1}}
    )

    async def call(rid):
        return await harness.gw.infer(request_id=rid, caller="a", model="m", inputs={"same": True})

    t1 = asyncio.create_task(call("m-1"))
    await asyncio.sleep(0.01)
    t2 = asyncio.create_task(call("m-2"))
    t3 = asyncio.create_task(call("m-3"))
    results = await asyncio.gather(t1, t2, t3)

    sources = sorted(r.source for r in results)
    assert sources == ["coalesced", "coalesced", "downstream"]
    # 下游只真正调用一次
    assert harness.backend.call_counts["p"] == 1
    # 三个请求结果语义完全一致
    assert all(r.output["output"] == {"answer": 1} for r in results)

    roles = {
        e.request_id: e.detail["role"]
        for e in harness.events.snapshot()
        if e.kind == "coalesce"
    }
    assert roles["m-1"] == "leader"
    assert roles["m-2"] == "follower"
    assert roles["m-3"] == "follower"


async def test_coalesced_and_solo_have_same_result_semantics(harness):
    _bump(harness, 10)
    harness.backend.behaviors["p"] = ScriptedBehavior(
        delay=0.12, value={"output": {"answer": "z"}}
    )

    async def call(rid):
        return await harness.gw.infer(request_id=rid, caller="a", model="m", inputs={"q": "z"})

    solo_task = asyncio.create_task(call("solo"))
    pair_a = asyncio.create_task(call("pair-a"))
    await asyncio.sleep(0.01)
    pair_b = asyncio.create_task(call("pair-b"))
    solo, a, b = await asyncio.gather(solo_task, pair_a, pair_b)
    assert solo.output["output"] == a.output["output"] == b.output["output"] == {"answer": "z"}


async def test_failed_downstream_does_not_populate_cache(harness):
    # 先失败（不可用，FAIL 模式明确失败），再恢复：第二次必须真实调用下游
    from gateway.errors import DownstreamUnavailableError

    harness.backend.behaviors["p"] = ScriptedBehavior(
        fail=DownstreamFailureKind.UNAVAILABLE
    )
    with pytest.raises(DownstreamUnavailableError):
        await harness.gw.infer(request_id="f-1", caller="a", model="m", inputs={"i": 1})
    puts = [e for e in harness.events.snapshot() if e.kind == "cache_put"]
    assert puts == []

    harness.backend.behaviors["p"] = ScriptedBehavior(delay=0.0)
    r = await harness.gw.infer(request_id="f-2", caller="a", model="m", inputs={"i": 1})
    assert r.source == "downstream"
    assert harness.backend.call_counts["p"] == 2  # 失败那次 + 恢复这次


async def test_cache_ttl_expiry(harness, events):
    from gateway.config import CacheConfig
    from gateway.wiring import InMemoryConfigStore, build_gateway

    cfg = harness.store.get().with_(cache=CacheConfig(ttl_seconds=0.05, coalesce_window=0.0))
    harness.backend.behaviors["p"] = ScriptedBehavior(delay=0.0)
    gw2, _, _, _ = build_gateway(
        cfg,
        backend=harness.backend,
        store=InMemoryConfigStore(cfg),
        events=events,
        configure_log=False,
    )
    r1 = await gw2.infer(request_id="e-1", caller="a", model="m", inputs={"i": 7})
    r2 = await gw2.infer(request_id="e-2", caller="a", model="m", inputs={"i": 7})
    assert r1.source == "downstream"
    assert r2.source == "cache"
    await asyncio.sleep(0.07)
    r3 = await gw2.infer(request_id="e-3", caller="a", model="m", inputs={"i": 7})
    assert r3.source == "downstream"


def _bump(harness, n):
    from gateway.config import QuotaConfig
    cfg = harness.store.get()
    harness.store.replace(
        cfg.with_(quotas={"a": QuotaConfig("a", max_concurrency=n, rate_per_second=1000.0),
                          "b": cfg.quotas["b"]})
    )

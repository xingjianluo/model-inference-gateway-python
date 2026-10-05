"""缓存调用方隔离、容量治理（service 层）与降级恢复语义。"""

from __future__ import annotations

import asyncio

import pytest

from gateway.config import FallbackPolicy, QuotaRule
from gateway.downstream import EchoBackend
from gateway.service import InferenceRequest
from tests.conftest import make_config


def req(request_id: str, *, caller: str = "c1", inputs=None):
    return InferenceRequest(caller, "m1", inputs or {"prompt": "same"}, request_id)


def unrated_config(**overrides):
    return make_config(
        quotas={
            "c1": QuotaRule("c1", max_concurrency=8, rate_per_second=None),
            "c2": QuotaRule("c2", max_concurrency=8, rate_per_second=None),
        },
        default_rule=QuotaRule("__default__", 8, None),
        **overrides,
    )


class CountingBackend:
    """记录调用次数的可控后端：可先失败后恢复。"""

    def __init__(self, name="primary"):
        self.name = name
        self.calls = 0
        self.healthy = True

    async def call(self, payload, *, request_id):
        self.calls += 1
        if not self.healthy:
            raise RuntimeError("simulated outage (internal detail)")
        return {"served_by": self.name, "generation": self.calls}


@pytest.mark.asyncio
async def test_cache_is_isolated_per_caller(make_service):
    backend = CountingBackend()
    service, _ = make_service(unrated_config(), backends={"primary": backend})

    r1 = await service.infer(req("a1", caller="c1"))
    assert r1.cache_status == "miss"
    r2 = await service.infer(req("a2", caller="c1"))
    assert r2.cache_status == "hit"  # c1 自己命中

    # 相同输入换个调用方：不得命中 c1 留下的结果，必须真正调用下游
    r3 = await service.infer(req("b1", caller="c2"))
    assert r3.cache_status == "miss"
    assert backend.calls == 2
    # c2 自己的第二次请求才命中
    r4 = await service.infer(req("b2", caller="c2"))
    assert r4.cache_status == "hit"
    assert backend.calls == 2


@pytest.mark.asyncio
async def test_degraded_result_is_not_cached_and_recovers_to_primary(make_service):
    """走过备用路径的输入：降级结果不被当正常结果缓存；下游恢复后必须重新反映主路径。"""
    primary = CountingBackend("primary")
    cfg = unrated_config(
        fallback=FallbackPolicy(mode="fallback", fallback_backend="backup")
    )
    service, _ = make_service(
        cfg, backends={"primary": primary, "backup": EchoBackend("backup")}
    )

    # 下游异常：走备用路径，degraded 结果返回给调用方
    primary.healthy = False
    r1 = await service.infer(req("r1"))
    assert r1.degraded is True and r1.source == "fallback"

    # 同一输入再来：不得把之前的降级结果当正常结果返回 -> 重新打下游
    r2 = await service.infer(req("r2"))
    assert r2.cache_status == "miss" and r2.degraded is True

    # 下游恢复：同一输入必须重新反映主路径结果
    primary.healthy = True
    r3 = await service.infer(req("r3"))
    assert r3.cache_status == "miss"
    assert r3.degraded is False and r3.source == "primary"
    assert r3.backend == "primary"

    # 恢复后的主路径结果正常进入缓存
    r4 = await service.infer(req("r4"))
    assert r4.cache_status == "hit" and r4.source == "primary"


@pytest.mark.asyncio
async def test_fallback_policy_flip_flop_keeps_in_flight_request_self_consistent(make_service):
    """降级策略反复切换：已受理请求按准入时快照执行到底，结果与错误自洽。"""
    primary = CountingBackend("primary")
    primary.healthy = False

    class SlowBackup(EchoBackend):
        started = None

        async def call(self, payload, *, request_id):
            type(self).started.set()
            await asyncio.sleep(0.02)
            return await super().call(payload, request_id=request_id)

    SlowBackup.started = asyncio.Event()
    cfg = unrated_config(
        fallback=FallbackPolicy(mode="fallback", fallback_backend="backup")
    )
    service, store = make_service(
        cfg, backends={"primary": primary, "backup": SlowBackup("backup")}
    )

    task = asyncio.create_task(service.infer(req("r1")))
    await SlowBackup.started.wait()
    # 请求在途期间反复切换降级策略
    await store.update_fallback(FallbackPolicy(mode="fail"))
    await store.update_fallback(FallbackPolicy(mode="fallback", fallback_backend="backup"))
    await store.update_fallback(FallbackPolicy(mode="fail"))

    r = await task  # 受理时是 fallback：仍按降级路径完成，结果自洽
    assert r.degraded is True and r.source == "fallback" and r.backend == "backup"

    # 新请求按最新策略（fail）：归一化失败，错误码可区分
    from gateway.errors import DownstreamInvokeError
    with pytest.raises(DownstreamInvokeError):
        await service.infer(req("r2", inputs={"prompt": "other"}))


@pytest.mark.asyncio
async def test_cache_capacity_limit_enforced_and_observable(make_service):
    cfg = unrated_config(cache_max_entries=2)
    service, _ = make_service(cfg, backends={"primary": EchoBackend("primary")})

    for i in range(3):
        r = await service.infer(req(f"k{i}", inputs={"prompt": f"p{i}"}))
        assert r.cache_status == "miss"

    stats = service.stats()["cache"]
    assert stats["cached_entries"] == 2
    assert stats["max_entries"] == 2
    assert stats["evictions"] == 1

    # 最久未用的 p0 已被回收：重新请求是 miss；p2 仍命中
    r = await service.infer(req("x", inputs={"prompt": "p2"}))
    assert r.cache_status == "hit"
    r0 = await service.infer(req("y", inputs={"prompt": "p0"}))
    assert r0.cache_status == "miss"


@pytest.mark.asyncio
async def test_strong_and_cached_callers_share_cache_but_not_results_across_callers(
    make_service,
):
    """strong 结果回填的是本调用方命名空间：其他调用方不可见。"""
    backend = CountingBackend()
    service, _ = make_service(unrated_config(), backends={"primary": backend})

    strong = InferenceRequest("c1", "m1", {"prompt": "same"}, "s1", consistency="strong")
    r1 = await service.infer(strong)
    assert r1.cache_status == "miss"

    # 其他调用方看不到 c1（含 strong）留下的缓存
    r2 = await service.infer(req("c2-first", caller="c2"))
    assert r2.cache_status == "miss"
    assert backend.calls == 2

"""按次一致性等级：strong 请求必须拿主路径新鲜结果，不读缓存、不走备用路径。"""

from __future__ import annotations

import asyncio

import pytest

from gateway.config import FallbackPolicy, QuotaRule
from gateway.downstream import EchoBackend, FlakyBackend
from gateway.errors import (
    ConsistencyUnavailableError,
    ErrorCode,
    InvalidRequestError,
)
from gateway.service import InferenceRequest
from tests.conftest import make_config
from tests.test_service import HoldBackend


def req(request_id: str, *, caller: str = "c1", consistency: str = "cached",
        inputs=None):
    return InferenceRequest(
        caller, "m1", inputs or {"prompt": "same"}, request_id,
        consistency=consistency,
    )


def unrated_config(**overrides):
    """不限速配置：避免假时钟下令牌桶影响断言。"""
    return make_config(
        quotas={"c1": QuotaRule("c1", max_concurrency=8, rate_per_second=None)},
        default_rule=QuotaRule("__default__", 8, None),
        **overrides,
    )


@pytest.mark.asyncio
async def test_invalid_consistency_value_is_rejected(make_service):
    service, _ = make_service(backends={"primary": EchoBackend("primary")})
    with pytest.raises(InvalidRequestError):
        await service.infer(req("r1", consistency="eventual"))


@pytest.mark.asyncio
async def test_strong_bypasses_cache_and_fetches_fresh(make_service):
    backend = EchoBackend("primary")
    service, _ = make_service(unrated_config(), backends={"primary": backend})

    r1 = await service.infer(req("r1"))  # 普通请求写入缓存
    assert r1.cache_status == "miss"

    r2 = await service.infer(req("r2"))  # 普通请求命中缓存
    assert r2.cache_status == "hit" and r2.freshness == "reused"

    # 强一致：绝不读缓存，重新打下游拿新鲜结果
    r3 = await service.infer(req("r3", consistency="strong"))
    assert r3.cache_status == "miss" and r3.freshness == "fresh"
    assert r3.source == "primary" and r3.degraded is False
    assert r3.produced_at >= r1.produced_at


@pytest.mark.asyncio
async def test_strong_refreshes_cache_for_normal_requests(make_service):
    service, _ = make_service(unrated_config(),
                              backends={"primary": EchoBackend("primary")})
    r1 = await service.infer(req("r1"))
    r2 = await service.infer(req("r2", consistency="strong"))
    assert r2.cache_status == "miss"
    # strong 的新鲜结果回填缓存：后续普通请求命中的是 strong 产出的那份
    r3 = await service.infer(req("r3"))
    assert r3.cache_status == "hit"
    assert r3.produced_at == r2.produced_at != r1.produced_at


@pytest.mark.asyncio
async def test_strong_never_uses_fallback_and_fails_distinguishably(make_service):
    service, _ = make_service(
        make_config(fallback=FallbackPolicy(mode="fallback", fallback_backend="backup")),
        backends={"primary": FlakyBackend("primary", behavior="error"),
                  "backup": EchoBackend("backup")},
    )
    # 普通请求：主失败 -> 备用路径降级成功
    r1 = await service.infer(req("n1"))
    assert r1.degraded is True and r1.source == "fallback"

    # 强一致：同一时刻主路径拿不到 -> 明确失败，绝不用备用路径顶替
    with pytest.raises(ConsistencyUnavailableError) as ei:
        await service.infer(req("s1", consistency="strong"))
    assert ei.value.code is ErrorCode.CONSISTENCY_UNAVAILABLE
    assert ei.value.status_code == 503
    assert ei.value.detail["reason"] == "downstream_error"


@pytest.mark.asyncio
async def test_strong_and_normal_in_flight_do_not_pollute_each_other(make_service):
    """普通在途（可能走降级）与强一致在途互不合并：各自真实调用下游。"""
    backend = HoldBackend()
    cfg = make_config(
        quotas={"c1": QuotaRule("c1", max_concurrency=5, rate_per_second=None)},
        default_rule=QuotaRule("__default__", 5, None),
    )
    service, _ = make_service(cfg, backends={"primary": backend})

    normal = asyncio.create_task(service.infer(req("n1")))
    await backend.started.wait()
    strong = asyncio.create_task(service.infer(req("s1", consistency="strong")))
    # 强一致没有合并到普通在途：自己成为 leader，产生第二次下游调用
    while backend.calls < 2:
        await asyncio.sleep(0.005)
    assert backend.calls == 2

    backend.release.set()
    r_normal, r_strong = await asyncio.gather(normal, strong)
    assert r_normal.cache_status == "miss"
    assert r_strong.cache_status == "miss"  # 不是 coalesced：两条通道隔离
    assert r_strong.freshness == "fresh"


@pytest.mark.asyncio
async def test_strong_requests_coalesce_among_themselves(make_service):
    """同输入的多个强一致请求之间仍可合并：只产生一次主路径调用。"""
    backend = HoldBackend()
    cfg = make_config(
        quotas={"c1": QuotaRule("c1", max_concurrency=5, rate_per_second=None)},
        default_rule=QuotaRule("__default__", 5, None),
    )
    service, _ = make_service(cfg, backends={"primary": backend})

    tasks = [
        asyncio.create_task(service.infer(req(f"s{i}", consistency="strong")))
        for i in range(3)
    ]
    await backend.started.wait()
    await asyncio.sleep(0.02)
    assert backend.calls == 1
    backend.release.set()

    responses = await asyncio.gather(*tasks)
    assert {r.cache_status for r in responses} == {"miss", "coalesced"}
    assert all(r.source == "primary" and not r.degraded for r in responses)


@pytest.mark.asyncio
async def test_strong_failure_is_not_cached_and_recovers(make_service):
    calls = {"n": 0}

    class FailOnce:
        name = "primary"

        async def call(self, payload, *, request_id):
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("boom internal")
            return {"recovered": True, "served_by": self.name}

    service, _ = make_service(backends={"primary": FailOnce()})
    with pytest.raises(ConsistencyUnavailableError):
        await service.infer(req("s1", consistency="strong"))
    # 下游恢复后，同一输入的强一致请求拿到主路径新鲜结果
    r = await service.infer(req("s2", consistency="strong"))
    assert r.output["recovered"] is True and r.source == "primary"

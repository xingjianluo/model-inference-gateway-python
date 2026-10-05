"""按次一致性声明（consistency="strong"）的行为测试。

强一致请求：绕过缓存与合并、禁用备用路径，只要主路径新鲜结果；
拿不到则以 consistency_unavailable 明确失败。两类请求互不污染。
"""

from __future__ import annotations

import asyncio

import pytest

from gateway.config import FallbackPolicy, QuotaRule
from gateway.downstream import EchoBackend, FlakyBackend
from gateway.errors import (
    ConcurrencyLimitedError,
    ConsistencyUnavailableError,
    ErrorCode,
    InvalidRequestError,
)
from gateway.service import InferenceRequest
from tests.conftest import make_config
from tests.test_service import HoldBackend


def req(request_id: str, *, caller: str = "c1", model: str = "m1",
        inputs=None, consistency: str = "default"):
    return InferenceRequest(
        caller, model, inputs or {"prompt": "same"}, request_id, consistency
    )


def _unlimited_config():
    return make_config(
        quotas={"c1": QuotaRule("c1", max_concurrency=5, rate_per_second=None)},
        default_rule=QuotaRule("__default__", 5, None),
    )


@pytest.mark.asyncio
async def test_strong_bypasses_cache_and_does_not_pollute(make_service):
    service, _ = make_service(_unlimited_config(),
                              backends={"primary": EchoBackend("primary")})
    r1 = await service.infer(req("n1"))  # 普通请求：miss 并写入缓存
    assert r1.cache_status == "miss"

    s1 = await service.infer(req("s1", consistency="strong"))
    assert s1.cache_status == "bypassed"      # 不命中已有缓存
    assert s1.consistency == "strong"
    assert s1.source == "fresh" and s1.path == "primary"
    assert s1.generated_at > 0

    # 强一致请求不写缓存：普通请求仍命中 r1 留下的那份
    r2 = await service.infer(req("n2"))
    assert r2.cache_status == "hit" and r2.output == r1.output
    assert service.stats()["cache"]["cached_entries"] == 1


@pytest.mark.asyncio
async def test_strong_does_not_coalesce_onto_in_flight(make_service):
    backend = HoldBackend()
    cfg = make_config(
        quotas={"c1": QuotaRule("c1", max_concurrency=5, rate_per_second=None)},
        default_rule=QuotaRule("__default__", 5, None),
    )
    service, _ = make_service(cfg, backends={"primary": backend})

    leader = asyncio.create_task(service.infer(req("leader")))
    await backend.started.wait()
    strong = asyncio.create_task(service.infer(req("s1", consistency="strong")))
    await asyncio.sleep(0.02)
    # 强一致请求不挂到在途合并上，自己打了一次下游
    assert backend.calls == 2
    backend.release.set()

    r_leader, r_strong = await asyncio.gather(leader, strong)
    assert r_leader.cache_status == "miss"
    assert r_strong.cache_status == "bypassed"


@pytest.mark.asyncio
async def test_normal_request_not_coalesced_onto_strong_in_flight(make_service):
    backend = HoldBackend()
    cfg = make_config(
        quotas={"c1": QuotaRule("c1", max_concurrency=5, rate_per_second=None)},
        default_rule=QuotaRule("__default__", 5, None),
    )
    service, _ = make_service(cfg, backends={"primary": backend})

    strong = asyncio.create_task(service.infer(req("s1", consistency="strong")))
    await backend.started.wait()
    normal = asyncio.create_task(service.infer(req("n1")))
    await asyncio.sleep(0.02)
    # 强一致请求不登记在途条目，普通请求成为自己的 leader
    assert backend.calls == 2
    backend.release.set()

    r_strong, r_normal = await asyncio.gather(strong, normal)
    assert r_strong.cache_status == "bypassed"
    assert r_normal.cache_status == "miss"


@pytest.mark.asyncio
async def test_strong_never_uses_fallback_and_fails_distinguishably(make_service):
    service, _ = make_service(
        make_config(fallback=FallbackPolicy(mode="fallback", fallback_backend="backup")),
        backends={"primary": FlakyBackend("primary", behavior="error"),
                  "backup": EchoBackend("backup")},
    )
    with pytest.raises(ConsistencyUnavailableError) as ei:
        await service.infer(req("s1", consistency="strong"))
    assert ei.value.code is ErrorCode.CONSISTENCY_UNAVAILABLE
    assert ei.value.status_code == 503
    assert ei.value.detail["underlying_error"] == "downstream_error"
    assert ei.value.detail["consistency"] == "strong"

    # 同一网关上普通请求不受影响：仍按降级策略成功
    r = await service.infer(req("n1"))
    assert r.degraded is True and r.backend == "backup" and r.path == "fallback"


@pytest.mark.asyncio
async def test_strong_failure_leaves_no_cache_or_inflight(make_service):
    calls = {"n": 0}

    class FailOnce:
        name = "primary"

        async def call(self, payload, *, request_id):
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("boom internal")
            return {"recovered": True, "served_by": self.name}

    service, _ = make_service(_unlimited_config(), backends={"primary": FailOnce()})
    with pytest.raises(ConsistencyUnavailableError):
        await service.infer(req("s1", consistency="strong"))

    # 强一致失败不留下任何缓存/在途痕迹：后续普通请求正常 miss -> hit
    r1 = await service.infer(req("n1"))
    assert r1.cache_status == "miss" and r1.output["recovered"] is True
    r2 = await service.infer(req("n2"))
    assert r2.cache_status == "hit"


@pytest.mark.asyncio
async def test_invalid_consistency_value_is_rejected(make_service):
    service, _ = make_service(backends={"primary": EchoBackend("primary")})
    with pytest.raises(InvalidRequestError) as ei:
        await service.infer(req("x1", consistency="eventually"))
    assert ei.value.code is ErrorCode.INVALID_REQUEST


@pytest.mark.asyncio
async def test_strong_still_consumes_quota(make_service):
    backend = HoldBackend()
    cfg = make_config(
        quotas={"c1": QuotaRule("c1", max_concurrency=1, rate_per_second=None)},
        default_rule=QuotaRule("__default__", 1, None),
    )
    service, _ = make_service(cfg, backends={"primary": backend})

    holder = asyncio.create_task(service.infer(req("hold", inputs={"prompt": "other"})))
    await backend.started.wait()
    # 唯一并发槽被占用：强一致请求同样被准入拒绝，不特殊化
    with pytest.raises(ConcurrencyLimitedError):
        await service.infer(req("s1", consistency="strong"))
    backend.release.set()
    await holder
    assert service.stats()["quota"]["c1"]["active"] == 0

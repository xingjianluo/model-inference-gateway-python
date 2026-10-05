"""网关编排端到端：准入、合并、超时取消、降级、热更新一致性。"""

from __future__ import annotations

import asyncio

import pytest

from gateway.config import FallbackPolicy, QuotaRule
from gateway.downstream import EchoBackend, FlakyBackend
from gateway.errors import (
    ConcurrencyLimitedError,
    DownstreamInvokeError,
    DownstreamTimeoutError,
    ErrorCode,
    FallbackExhaustedError,
    InvalidRequestError,
)
from gateway.service import InferenceRequest
from tests.conftest import make_config


def req(request_id: str, *, caller: str = "c1", model: str = "m1", inputs=None):
    return InferenceRequest(caller, model, inputs or {"prompt": "same"}, request_id)


class HoldBackend:
    """可阻塞到事件触发的后端，用于制造在途请求。"""

    name = "hold"

    def __init__(self, result=None, exc: Exception | None = None):
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.calls = 0
        self._result = result or {"ok": True}
        self._exc = exc

    async def call(self, payload, *, request_id):
        self.calls += 1
        self.started.set()
        await self.release.wait()
        if self._exc is not None:
            raise self._exc
        return self._result


@pytest.mark.asyncio
async def test_invalid_request_is_rejected(make_service):
    service, _ = make_service(backends={"primary": EchoBackend("primary")})
    with pytest.raises(InvalidRequestError):
        await service.infer(req("r1", caller=""))  # 缺少 caller


@pytest.mark.asyncio
async def test_success_then_cache_hit_same_semantics(make_service):
    service, _ = make_service(backends={"primary": EchoBackend("primary")})
    r1 = await service.infer(req("r1"))
    r2 = await service.infer(req("r2"))
    assert r1.cache_status == "miss" and r2.cache_status == "hit"
    assert r1.output == r2.output
    assert r1.backend == r2.backend == "primary"
    assert r1.request_id == "r1" and r2.request_id == "r2"


@pytest.mark.asyncio
async def test_concurrent_identical_requests_coalesce_to_one_downstream_call(make_service):
    backend = HoldBackend()
    cfg = make_config(quotas={"c1": QuotaRule("c1", max_concurrency=5, rate_per_second=None)},
                      default_rule=QuotaRule("__default__", 5, None))
    service, _ = make_service(cfg, backends={"primary": backend})

    tasks = [asyncio.create_task(service.infer(req(f"r{i}"))) for i in range(5)]
    await backend.started.wait()
    await asyncio.sleep(0.02)
    assert backend.calls == 1
    backend.release.set()

    responses = await asyncio.gather(*tasks)
    statuses = {r.cache_status for r in responses}
    assert statuses == {"miss", "coalesced"}
    assert sum(r.cache_status == "miss" for r in responses) == 1
    assert sum(r.cache_status == "coalesced" for r in responses) == 4
    outputs = {tuple(sorted(r.output.items())) for r in responses}
    assert len(outputs) == 1  # 合并与单独请求结果语义一致
    # 除 cache_status/request_id 外，backend/degraded 等字段完全一致
    backends = {r.backend for r in responses}
    degraded = {r.degraded for r in responses}
    assert backends == {"hold"} and degraded == {False}
    # leader 单槽：等待期间 5 个请求只占 1 个并发槽
    assert service.stats()["quota"]["c1"]["active"] == 0


@pytest.mark.asyncio
async def test_concurrency_limit_through_service_is_distinguishable(make_service):
    backend = HoldBackend()
    cfg = make_config(
        quotas={"c1": QuotaRule("c1", max_concurrency=1, rate_per_second=None)},
        default_rule=QuotaRule("__default__", 1, None),
    )
    service, _ = make_service(cfg, backends={"primary": backend})

    t1 = asyncio.create_task(service.infer(req("r1")))
    await backend.started.wait()
    t2 = asyncio.create_task(service.infer(req("r2")))
    result = await asyncio.gather(t2, return_exceptions=True)
    assert isinstance(result[0], ConcurrencyLimitedError)
    assert result[0].code is ErrorCode.CONCURRENCY_LIMITED
    assert result[0].status_code == 429
    backend.release.set()
    await t1
    assert service.stats()["quota"]["c1"]["active"] == 0


@pytest.mark.asyncio
async def test_timeout_releases_quota_and_caches_nothing(make_service):
    class TimeoutOnce:
        name = "primary"

        def __init__(self):
            self.calls = 0

        async def call(self, payload, *, request_id):
            self.calls += 1
            if self.calls == 1:
                await asyncio.sleep(1.0)  # 首次必超时
            return {"recovered": True, "served_by": self.name}

    cfg = make_config(downstream_timeout_seconds=0.02)
    backend = TimeoutOnce()
    service, _ = make_service(cfg, backends={"primary": backend})

    with pytest.raises(DownstreamTimeoutError) as ei:
        await service.infer(req("r1"))
    assert ei.value.status_code == 504
    # 超时不占后续配额
    assert service.stats()["quota"]["c1"]["active"] == 0
    # 超时不写缓存：第二次请求作为新 MISS 打到下游并成功
    r2 = await service.infer(req("r2"))
    assert r2.cache_status == "miss" and r2.output["recovered"] is True
    assert backend.calls == 2


@pytest.mark.asyncio
async def test_waiter_cancellation_releases_its_slot_and_leader_survives(make_service):
    backend = HoldBackend()
    cfg = make_config(
        quotas={"c1": QuotaRule("c1", max_concurrency=2, rate_per_second=None)},
        default_rule=QuotaRule("__default__", 2, None),
    )
    service, _ = make_service(cfg, backends={"primary": backend})

    leader = asyncio.create_task(service.infer(req("leader")))
    await backend.started.wait()
    waiter = asyncio.create_task(service.infer(req("waiter")))
    await asyncio.sleep(0.02)
    assert service.stats()["quota"]["c1"]["active"] == 1  # 等待者挂起不占槽
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter
    assert service.stats()["quota"]["c1"]["active"] == 1

    # leader 仍可正常完成，且其成功结果可被后续请求命中缓存
    backend.release.set()
    r = await leader
    assert r.cache_status == "miss"
    hit = await service.infer(req("after"))
    assert hit.cache_status == "hit" and hit.output == r.output


@pytest.mark.asyncio
async def test_leader_cancellation_propagates_and_cleans_quota(make_service):
    backend = HoldBackend()
    service, _ = make_service(backends={"primary": backend})
    leader = asyncio.create_task(service.infer(req("leader")))
    await backend.started.wait()
    waiter_task = asyncio.create_task(service.infer(req("waiter")))
    await asyncio.sleep(0.02)

    leader.cancel()
    with pytest.raises(asyncio.CancelledError):
        await leader
    # 合并者收到可解释的归一化错误，而不是悬挂
    with pytest.raises(DownstreamInvokeError):
        await waiter_task
    await asyncio.sleep(0.02)
    assert service.stats()["quota"]["c1"]["active"] == 0
    # 失败未缓存
    backend.release.set()


@pytest.mark.asyncio
async def test_fallback_engages_and_is_marked_degraded(make_service):
    service, _ = make_service(
        make_config(fallback=FallbackPolicy(mode="fallback", fallback_backend="backup")),
        backends={"primary": FlakyBackend("primary", behavior="error"),
                  "backup": EchoBackend("backup")},
    )
    r = await service.infer(req("r1"))
    assert r.degraded is True and r.backend == "backup"
    assert r.cache_status == "miss"


@pytest.mark.asyncio
async def test_fallback_exhausted_is_normalized(make_service):
    service, _ = make_service(
        make_config(fallback=FallbackPolicy(mode="fallback", fallback_backend="backup")),
        backends={"primary": FlakyBackend("primary", behavior="error"),
                  "backup": FlakyBackend("backup", behavior="bad_result")},
    )
    with pytest.raises(FallbackExhaustedError) as ei:
        await service.infer(req("r1"))
    detail = ei.value.detail
    assert detail["primary_error"] == "downstream_error"
    assert detail["fallback_error"] == "downstream_bad_result"
    assert "internal detail" not in ei.value.public_message


@pytest.mark.asyncio
async def test_fallback_switch_during_in_flight_uses_admission_snapshot(make_service):
    """已按 fail 策略受理的请求，处理途中切换到 fallback，仍按 fail 失败。"""
    backend = HoldBackend(exc=RuntimeError("boom internal"))
    service, store = make_service(
        make_config(fallback=FallbackPolicy(mode="fail")),
        backends={"primary": backend, "backup": EchoBackend("backup")},
    )

    task = asyncio.create_task(service.infer(req("r1")))
    await backend.started.wait()
    # 在请求在途期间热切换降级策略
    await store.update_fallback(FallbackPolicy(mode="fallback", fallback_backend="backup"))
    backend.release.set()

    with pytest.raises(DownstreamInvokeError) as ei:
        await task
    assert ei.value.code is ErrorCode.DOWNSTREAM_ERROR


@pytest.mark.asyncio
async def test_fallback_result_is_coalesced_consistently(make_service):
    """主路径慢失败转降级时，合并者与 leader 拿到同一份 degraded 结果，主/备各只调一次。"""

    class SlowFail(FlakyBackend):
        def __init__(self, name, gate):
            super().__init__(name, behavior="error")
            self.gate = gate
            self.calls = 0

        async def call(self, payload, *, request_id):
            self.calls += 1
            self.gate.set()
            import asyncio as _a
            await _a.sleep(0.02)
            return await super().call(payload, request_id=request_id)

    gate = asyncio.Event()
    primary = SlowFail("primary", gate)
    backup = EchoBackend("backup")
    service, _ = make_service(
        make_config(
            quotas={"c1": QuotaRule("c1", max_concurrency=5, rate_per_second=None)},
            default_rule=QuotaRule("__default__", 5, None),
            fallback=FallbackPolicy(mode="fallback", fallback_backend="backup"),
        ),
        backends={"primary": primary, "backup": backup},
    )
    tasks = [asyncio.create_task(service.infer(req(f"fb{i}"))) for i in range(3)]
    await gate.wait()
    responses = await asyncio.gather(*tasks)
    assert all(r.degraded and r.backend == "backup" for r in responses)
    assert {r.cache_status for r in responses} == {"miss", "coalesced"}
    assert primary.calls == 1  # 只有 leader 打了主路径，降级也只发生一次


@pytest.mark.asyncio
async def test_failure_does_not_poison_cache(make_service):
    calls = {"n": 0}

    class FailOnce(FlakyBackend):
        async def call(self, payload, *, request_id):
            calls["n"] += 1
            if calls["n"] == 1:
                return await super().call(payload, request_id=request_id)
            return {"recovered": True, "served_by": self.name}

    backend = FailOnce("primary", behavior="error")
    service, _ = make_service(backends={"primary": backend})

    with pytest.raises(DownstreamInvokeError):
        await service.infer(req("r1"))
    r2 = await service.infer(req("r2"))
    assert r2.cache_status == "miss" and r2.output == {"recovered": True, "served_by": "primary"}


@pytest.mark.asyncio
async def test_quota_hot_reload_lowers_limit_without_restart(make_service):
    backend = HoldBackend()
    cfg = make_config(quotas={"c1": QuotaRule("c1", max_concurrency=2, rate_per_second=None)},
                      default_rule=QuotaRule("__default__", 2, None))
    service, store = make_service(cfg, backends={"primary": backend})
    assert service.stats()["quota"]["c1"]["max_concurrency"] == 2

    t1 = asyncio.create_task(service.infer(req("r1")))
    t2 = asyncio.create_task(service.infer(req("r2", inputs={"prompt": "different"})))
    # 等待两个不同请求都在途（各自占用一个槽）
    while service.stats()["quota"]["c1"]["active"] < 2:
        await asyncio.sleep(0.005)
    assert service.stats()["quota"]["c1"]["active"] == 2

    # 不重启进程降配到 1：新请求立即被拒，在途 2 个保持不超卖
    from gateway.config import QuotaRule as QR
    await store.update_quotas({"c1": QR("c1", max_concurrency=1, rate_per_second=None)})
    t3 = asyncio.create_task(service.infer(req("r3")))
    t3_result = await asyncio.gather(t3, return_exceptions=True)
    assert isinstance(t3_result[0], ConcurrencyLimitedError)

    backend.release.set()
    await asyncio.gather(t1, t2)
    assert service.stats()["quota"]["c1"]["active"] == 0


# ---------- 缓存按调用方隔离 ----------


@pytest.mark.asyncio
async def test_cache_is_isolated_per_caller(make_service):
    cfg = make_config(
        quotas={
            "c1": QuotaRule("c1", max_concurrency=5, rate_per_second=None),
            "c2": QuotaRule("c2", max_concurrency=5, rate_per_second=None),
        },
        default_rule=QuotaRule("__default__", 5, None),
    )
    service, _ = make_service(cfg, backends={"primary": EchoBackend("primary")})

    r1 = await service.infer(req("a1", caller="c1"))
    assert r1.cache_status == "miss"
    # 相同输入、不同调用方：不能命中 c1 留下的缓存
    r2 = await service.infer(req("b1", caller="c2"))
    assert r2.cache_status == "miss"
    # 各自再请求：各自命中自己的条目
    assert (await service.infer(req("a2", caller="c1"))).cache_status == "hit"
    assert (await service.infer(req("b2", caller="c2"))).cache_status == "hit"
    assert service.stats()["cache"]["cached_entries"] == 2


@pytest.mark.asyncio
async def test_coalescing_is_isolated_per_caller(make_service):
    backend = HoldBackend()
    cfg = make_config(
        quotas={
            "c1": QuotaRule("c1", max_concurrency=5, rate_per_second=None),
            "c2": QuotaRule("c2", max_concurrency=5, rate_per_second=None),
        },
        default_rule=QuotaRule("__default__", 5, None),
    )
    service, _ = make_service(cfg, backends={"primary": backend})

    t1 = asyncio.create_task(service.infer(req("a1", caller="c1")))
    await backend.started.wait()
    t2 = asyncio.create_task(service.infer(req("b1", caller="c2")))
    await asyncio.sleep(0.02)
    # 不同调用方的相同输入不合并：下游被调用了两次
    assert backend.calls == 2
    backend.release.set()
    r1, r2 = await asyncio.gather(t1, t2)
    assert r1.cache_status == "miss" and r2.cache_status == "miss"


# ---------- 结果来源（provenance） ----------


@pytest.mark.asyncio
async def test_response_exposes_source_path_and_generated_at(make_service):
    cfg = make_config(
        quotas={"c1": QuotaRule("c1", max_concurrency=5, rate_per_second=None)},
        default_rule=QuotaRule("__default__", 5, None),
    )
    service, _ = make_service(cfg, backends={"primary": EchoBackend("primary")})

    r1 = await service.infer(req("p1"))
    assert r1.source == "fresh" and r1.path == "primary"
    assert r1.generated_at > 0

    r2 = await service.infer(req("p2"))
    assert r2.source == "cache" and r2.path == "primary"
    # 缓存结果保留原始产出时刻，而不是命中时刻
    assert r2.generated_at == r1.generated_at


@pytest.mark.asyncio
async def test_coalesced_response_marks_shared_source(make_service):
    backend = HoldBackend()
    cfg = make_config(
        quotas={"c1": QuotaRule("c1", max_concurrency=5, rate_per_second=None)},
        default_rule=QuotaRule("__default__", 5, None),
    )
    service, _ = make_service(cfg, backends={"primary": backend})

    leader = asyncio.create_task(service.infer(req("l1")))
    await backend.started.wait()
    waiter = asyncio.create_task(service.infer(req("w1")))
    await asyncio.sleep(0.02)
    backend.release.set()
    r_leader, r_waiter = await asyncio.gather(leader, waiter)
    assert r_leader.source == "fresh"
    assert r_waiter.source == "coalesced"
    assert r_waiter.generated_at == r_leader.generated_at


# ---------- 降级结果的恢复语义 ----------


@pytest.mark.asyncio
async def test_degraded_result_not_cached_and_primary_recovers(make_service):
    """走过备用路径的输入，在主路径恢复后必须重新反映主路径结果。"""
    state = {"primary_down": True}

    class Switchable:
        name = "primary"

        async def call(self, payload, *, request_id):
            if state["primary_down"]:
                raise RuntimeError("primary down (internal)")
            return {"served_by": self.name, "healthy": True}

    cfg = make_config(
        quotas={"c1": QuotaRule("c1", max_concurrency=5, rate_per_second=None)},
        default_rule=QuotaRule("__default__", 5, None),
        fallback=FallbackPolicy(mode="fallback", fallback_backend="backup"),
    )
    service, _ = make_service(
        cfg,
        backends={"primary": Switchable(), "backup": EchoBackend("backup")},
    )

    # 主路径故障期：拿到降级结果（备用路径）
    r1 = await service.infer(req("d1"))
    assert r1.degraded is True and r1.path == "fallback" and r1.backend == "backup"

    # 主路径恢复：同样的输入必须重新走主路径，而不是复用之前的降级结果
    state["primary_down"] = False
    r2 = await service.infer(req("d2"))
    assert r2.cache_status == "miss"  # 降级结果从未入缓存
    assert r2.degraded is False and r2.path == "primary"
    assert r2.output["healthy"] is True

    # 此后的正常结果才进入缓存
    r3 = await service.infer(req("d3"))
    assert r3.cache_status == "hit" and r3.degraded is False


@pytest.mark.asyncio
async def test_fallback_policy_flapping_keeps_results_self_consistent(make_service):
    """降级策略反复切换：已受理请求按受理时快照执行，错误与结果始终可解释。"""
    cfg = make_config(
        quotas={"c1": QuotaRule("c1", max_concurrency=5, rate_per_second=None)},
        default_rule=QuotaRule("__default__", 5, None),
        fallback=FallbackPolicy(mode="fallback", fallback_backend="backup"),
    )
    service, store = make_service(
        cfg,
        backends={"primary": FlakyBackend("primary", behavior="error"),
                  "backup": EchoBackend("backup")},
    )

    # fallback 模式：降级成功，且降级结果不入缓存
    r1 = await service.infer(req("f1"))
    assert r1.degraded is True and r1.path == "fallback"

    # 切到 fail：同样的输入不会吃到“缓存的降级结果”，而是按新策略明确失败
    await store.update_fallback(FallbackPolicy(mode="fail"))
    with pytest.raises(DownstreamInvokeError) as ei:
        await service.infer(req("f2"))
    assert ei.value.code is ErrorCode.DOWNSTREAM_ERROR

    # 再切回 fallback：新请求又按新快照降级成功
    await store.update_fallback(
        FallbackPolicy(mode="fallback", fallback_backend="backup")
    )
    r3 = await service.infer(req("f3"))
    assert r3.degraded is True and r3.cache_status == "miss"

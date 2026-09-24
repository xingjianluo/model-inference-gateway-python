"""并发与速率配额、热更新一致性测试。"""

from __future__ import annotations

import asyncio

import pytest

from gateway.config import GatewayConfig, QuotaRule
from gateway.quota import QuotaManager
from tests.conftest import make_config


def test_concurrency_cap_rejects_without_counting(fake_clock):
    qm = QuotaManager(make_config(), clock=fake_clock)
    d1 = qm.try_acquire("c1")
    d2 = qm.try_acquire("c1")
    d3 = qm.try_acquire("c1")

    assert d1.allowed and d2.allowed
    assert not d3.allowed
    assert d3.reason == "concurrency_limited"
    # 被拒请求不占槽：active 仍为 2，不漂移
    assert qm.stats("c1")["active"] == 2


def test_rate_limit_distinguishable_and_token_refill(fake_clock):
    cfg = GatewayConfig(
        quotas={"c1": QuotaRule("c1", max_concurrency=10, rate_per_second=2.0)},
        default_rule=QuotaRule("__default__", max_concurrency=10, rate_per_second=None),
    )
    qm = QuotaManager(cfg, clock=fake_clock)

    d1 = qm.try_acquire("c1")
    d2 = qm.try_acquire("c1")
    d3 = qm.try_acquire("c1")
    assert d1.allowed and d2.allowed
    assert not d3.allowed and d3.reason == "rate_limited"
    assert qm.stats("c1")["active"] == 2

    # 1 秒补 2 个令牌，又能放行
    fake_clock.advance(1.0)
    d4 = qm.try_acquire("c1")
    d5 = qm.try_acquire("c1")
    d6 = qm.try_acquire("c1")
    assert d4.allowed and d5.allowed
    assert not d6.allowed and d6.reason == "rate_limited"


def test_release_is_idempotent_and_never_negative(fake_clock):
    qm = QuotaManager(make_config(), clock=fake_clock)
    qm.try_acquire("c1")
    qm.release("c1")
    qm.release("c1")  # 重复归还
    qm.release("c1")
    assert qm.stats("c1")["active"] == 0


def test_lower_limit_hot_reload_takes_effect_immediately(fake_clock):
    cfg = make_config()
    qm = QuotaManager(cfg, clock=fake_clock)
    qm.try_acquire("c1")
    qm.try_acquire("c1")

    # 降配到 1：在途 2 个不被中断，但新请求立即拒绝，绝不超卖
    qm.apply_config(
        GatewayConfig(
            quotas={"c1": QuotaRule("c1", max_concurrency=1, rate_per_second=100.0)},
            default_rule=cfg.default_rule,
        )
    )
    decision = qm.try_acquire("c1")
    assert decision.reason == "concurrency_limited"
    assert decision.rule_version == 1
    assert qm.stats("c1")["active"] == 2

    # 在途请求陆续结束，active 降到限额以内，仍不超过新限额
    qm.release("c1")
    assert qm.try_acquire("c1").reason == "concurrency_limited"
    qm.release("c1")
    assert qm.try_acquire("c1").allowed


def test_raise_limit_hot_reload_admits_immediately(fake_clock):
    cfg = make_config()
    qm = QuotaManager(cfg, clock=fake_clock)
    assert qm.try_acquire("c1").allowed
    assert qm.try_acquire("c1").allowed
    assert qm.try_acquire("c1").reason == "concurrency_limited"

    qm.apply_config(
        GatewayConfig(
            quotas={"c1": QuotaRule("c1", max_concurrency=4, rate_per_second=1000.0)},
            default_rule=cfg.default_rule,
        )
    )
    assert qm.try_acquire("c1").allowed
    assert qm.try_acquire("c1").allowed
    assert qm.stats("c1")["active"] == 4


@pytest.mark.asyncio
async def test_concurrent_admit_never_oversells(fake_clock):
    """500 协程争抢 8 个槽：任何时刻持槽数绝不超过容量，被拒原因可区分。"""
    cfg = GatewayConfig(
        quotas={"c1": QuotaRule("c1", max_concurrency=8, rate_per_second=None)},
        default_rule=QuotaRule("__default__", max_concurrency=8, rate_per_second=None),
    )
    qm = QuotaManager(cfg, clock=fake_clock)
    decisions: list = []
    admitted = 0
    peak = 0
    release_all = asyncio.Event()

    async def worker():
        nonlocal admitted, peak
        while True:
            d = qm.try_acquire("c1")
            decisions.append(d)
            if d.allowed:
                admitted += 1
                peak = max(peak, qm.stats("c1")["active"])
                await release_all.wait()
                qm.release("c1")
                return
            await asyncio.sleep(0)  # 被拒后让出再重试，绝不排队占用资源

    workers = [asyncio.create_task(worker()) for _ in range(500)]
    # 等待恰好 8 个持槽
    while qm.stats("c1")["active"] < 8:
        await asyncio.sleep(0)
    await asyncio.sleep(0.01)  # 再让一批争抢/重试发生
    assert qm.stats("c1")["active"] == 8
    assert peak == 8
    rejected_reasons = {d.reason for d in decisions if not d.allowed}
    assert rejected_reasons == {"concurrency_limited"}
    release_all.set()
    await asyncio.gather(*workers)
    assert admitted == 500
    assert qm.stats("c1")["active"] == 0


@pytest.mark.asyncio
async def test_admit_and_lower_limit_interleave_no_drift(fake_clock):
    """准入与降配高频交错：计数始终满足 0 <= active <= max(1, 旧容量)。"""
    cfg = GatewayConfig(
        quotas={"c1": QuotaRule("c1", max_concurrency=4, rate_per_second=None)},
        default_rule=QuotaRule("__default__", max_concurrency=4, rate_per_second=None),
    )
    qm = QuotaManager(cfg, clock=fake_clock)

    async def churn():
        for _ in range(300):
            d = qm.try_acquire("c1")
            if d.allowed:
                assert qm.stats("c1")["active"] <= 4
                await asyncio.sleep(0)
                qm.release("c1")
            await asyncio.sleep(0)

    async def reconfigure():
        for cap in (1, 4, 2, 4):
            qm.apply_config(
                GatewayConfig(
                    quotas={"c1": QuotaRule("c1", cap, None)},
                    default_rule=cfg.default_rule,
                )
            )
            await asyncio.sleep(0)

    await asyncio.gather(churn(), reconfigure())
    assert qm.stats("c1")["active"] == 0


def test_unknown_caller_uses_default_rule(fake_clock):
    qm = QuotaManager(make_config(), clock=fake_clock)
    for _ in range(3):
        assert qm.try_acquire("stranger").allowed
    assert qm.try_acquire("stranger").reason == "concurrency_limited"

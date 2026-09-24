"""超时与取消测试：被取消/超时的请求不残留配额、不写缓存、不悬挂下游任务。"""

from __future__ import annotations

import asyncio

import pytest

from gateway.config import QuotaConfig
from gateway.downstream import DownstreamFailureKind, ScriptedBehavior
from gateway.errors import DownstreamTimeoutError

pytestmark = pytest.mark.anyio


def _bump(harness, n=10, rate=1000.0):
    cfg = harness.store.get()
    harness.store.replace(
        cfg.with_(
            quotas={
                "a": QuotaConfig("a", max_concurrency=n, rate_per_second=rate),
                "b": cfg.quotas["b"],
            }
        )
    )


async def test_follower_cancel_does_not_affect_leader(harness):
    _bump(harness)
    harness.backend.behaviors["p"] = ScriptedBehavior(
        delay=0.2, value={"output": {"answer": 1}}
    )

    leader = asyncio.create_task(
        harness.gw.infer(request_id="x-leader", caller="a", model="m", inputs={"k": 1})
    )
    follower = asyncio.create_task(
        harness.gw.infer(request_id="x-follower", caller="a", model="m", inputs={"k": 1})
    )
    await asyncio.sleep(0.02)  # follower 已挂入合并窗口
    follower.cancel()
    with pytest.raises(asyncio.CancelledError):
        await follower

    # follower 取消后立即释放一个槽，leader 仍占一个
    assert await harness.gw._quota.in_flight_of("a") == 1

    result = await leader
    assert result.source == "downstream"
    assert result.output["output"] == {"answer": 1}
    assert await harness.gw._quota.in_flight_of("a") == 0


async def test_leader_cancel_propagates_and_cleans_up(harness):
    _bump(harness)
    harness.backend.behaviors["p"] = ScriptedBehavior(
        delay=1.0, value={"output": {"answer": 1}}
    )

    leader = asyncio.create_task(
        harness.gw.infer(request_id="l-1", caller="a", model="m", inputs={"k": 2})
    )
    follower = asyncio.create_task(
        harness.gw.infer(request_id="l-2", caller="a", model="m", inputs={"k": 2})
    )
    await asyncio.sleep(0.02)
    leader.cancel()

    with pytest.raises(asyncio.CancelledError):
        await leader
    with pytest.raises(asyncio.CancelledError):
        await follower

    # 下游任务被真正取消（脚本后端的 finally 已执行），配额全部释放
    await asyncio.sleep(0.02)
    assert harness.backend.in_flight_counts["p"] == 0
    assert await harness.gw._quota.in_flight_of("a") == 0
    assert [e for e in harness.events.snapshot() if e.kind == "cache_put"] == []


async def test_downstream_timeout_aborts_call_and_frees_quota(harness):
    _bump(harness)
    harness.backend.behaviors["p"] = ScriptedBehavior(
        fail=DownstreamFailureKind.TIMEOUT
    )

    with pytest.raises(DownstreamTimeoutError):
        await harness.gw.infer(request_id="to-1", caller="a", model="m", inputs={"k": 3})

    await asyncio.sleep(0.02)
    # 下游协程被 wait_for 取消，无悬挂调用
    assert harness.backend.in_flight_counts["p"] == 0
    # 超时不写缓存、不占配额
    assert [e for e in harness.events.by_request("to-1") if e.kind == "cache_put"] == []
    assert await harness.gw._quota.in_flight_of("a") == 0

    ev = next(
        e
        for e in harness.events.by_request("to-1")
        if e.kind == "downstream_result"
    )
    assert ev.detail["ok"] is False
    assert ev.detail["failure"] == "timeout"

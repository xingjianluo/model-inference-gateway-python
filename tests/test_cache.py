"""请求合并与结果缓存测试。"""

from __future__ import annotations

import asyncio

import pytest

from gateway.cache import CacheStatus, CoalescingCache


@pytest.fixture
def cache(fake_clock):
    return CoalescingCache(
        ttl_seconds=10.0, coalesce_window_seconds=0.0, clock=fake_clock
    )


@pytest.mark.asyncio
async def test_miss_hit_ttl_expiry(fake_clock):
    cache = CoalescingCache(ttl_seconds=10.0, clock=fake_clock)
    o1 = cache.lookup("k")
    assert o1.status is CacheStatus.MISS
    cache.set_result("k", o1.future, {"v": 1})

    o2 = cache.lookup("k")
    assert o2.status is CacheStatus.HIT and o2.value == {"v": 1}

    fake_clock.advance(10.001)
    assert cache.lookup("k").status is CacheStatus.MISS


@pytest.mark.asyncio
async def test_coalesced_waiters_share_identical_result(cache):
    async def waiter(outcome):
        return await asyncio.shield(outcome.future)

    o1 = cache.lookup("k")
    o2 = cache.lookup("k")
    o3 = cache.lookup("k")
    assert o1.status is CacheStatus.MISS
    assert o2.status is CacheStatus.COALESCED and o2.future is o1.future
    assert o3.status is CacheStatus.COALESCED and o3.future is o1.future

    w = asyncio.gather(waiter(o2), waiter(o3))
    await asyncio.sleep(0)
    cache.set_result("k", o1.future, {"v": 42})
    results = await w

    assert results == [{"v": 42}, {"v": 42}]
    assert cache.stats()["misses"] == 1
    assert cache.stats()["coalesced"] == 2
    assert cache.lookup("k").status is CacheStatus.HIT


@pytest.mark.asyncio
async def test_failure_is_never_cached(cache):
    o1 = cache.lookup("k")
    o2 = cache.lookup("k")

    async def waiter():
        with pytest.raises(RuntimeError, match="boom"):
            await asyncio.shield(o2.future)

    t = asyncio.create_task(waiter())
    cache.set_failure("k", o1.future, RuntimeError("boom"))
    await t

    # 失败后：无缓存条目、无在途条目，下一个请求是全新 MISS
    assert cache.lookup("k").status is CacheStatus.MISS
    assert cache.stats()["cached_entries"] == 0


@pytest.mark.asyncio
async def test_cancelling_waiter_does_not_cancel_leader(cache):
    o1 = cache.lookup("k")
    o2 = cache.lookup("k")

    async def waiter():
        await asyncio.shield(o2.future)

    task = asyncio.create_task(waiter())
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    # leader 的 future 依然存活，可正常兑现
    assert not o1.future.done()
    cache.set_result("k", o1.future, {"v": 1})
    assert o1.future.result() == {"v": 1}


@pytest.mark.asyncio
async def test_failure_coalesce_window_then_retry(fake_clock):
    cache = CoalescingCache(
        ttl_seconds=10.0, coalesce_window_seconds=2.0, clock=fake_clock
    )
    o1 = cache.lookup("k")
    cache.set_failure("k", o1.future, RuntimeError("down"))
    await asyncio.sleep(0)

    # 窗口内：仍然合并到同一个失败
    o2 = cache.lookup("k")
    assert o2.status is CacheStatus.COALESCED
    with pytest.raises(RuntimeError):
        await o2.future

    # 窗口过后：允许新 leader
    fake_clock.advance(2.001)
    o3 = cache.lookup("k")
    assert o3.status is CacheStatus.MISS
    cache.set_result("k", o3.future, {"v": "recovered"})
    assert cache.lookup("k").status is CacheStatus.HIT


@pytest.mark.asyncio
async def test_stale_future_completion_is_ignored(fake_clock):
    cache = CoalescingCache(ttl_seconds=10.0, clock=fake_clock)
    old = cache.lookup("k")
    # 旧 leader 被清空后，新 leader 接管同一 key
    cache.clear()
    new = cache.lookup("k")
    cache.set_result("k", old.future, {"v": "stale"})  # 不应污染当前条目
    assert not new.future.done()
    cache.set_result("k", new.future, {"v": "fresh"})
    assert cache.lookup("k").value == {"v": "fresh"}

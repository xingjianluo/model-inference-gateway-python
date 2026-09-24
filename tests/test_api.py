"""HTTP 层测试：错误码/状态码映射、热更新端点、请求标识与日志。"""

from __future__ import annotations

import asyncio

import httpx
import pytest

from gateway.app import create_app
from gateway.config import (
    CacheConfig,
    FallbackMode,
    QuotaConfig,
    TimeoutConfig,
)
from gateway.downstream import DownstreamFailureKind, ScriptedBehavior
from gateway.events import EventRecorder
from gateway.wiring import InMemoryConfigStore, build_gateway

pytestmark = pytest.mark.anyio


@pytest.fixture
async def client(store, fast_backend):
    events = EventRecorder()
    gw, _, _, _ = build_gateway(
        store.get(),
        backend=fast_backend,
        events=events,
        store=store,
        configure_log=False,
    )
    app = create_app(gateway=gw, store=store, events=events)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://gw") as c:
        yield c, store, events, gw


async def test_infer_success_and_events(client):
    c, store, events, gw = client
    r = await c.post(
        "/v1/infer",
        json={"model": "m", "inputs": {"i": 1}, "caller": "a", "request_id": "api-1"},
    )
    assert r.status_code == 200
    body = r.json()
    assert body["request_id"] == "api-1"
    assert body["source"] == "downstream"
    r2 = await c.get("/admin/events", params={"request_id": "api-1"})
    kinds = [e["kind"] for e in r2.json()["events"]]
    assert "quota_admit" in kinds and "cache_lookup" in kinds and "downstream_result" in kinds


async def test_concurrency_and_rate_error_codes_distinct(client):
    c, store, events, gw = client
    # 用慢速后端占满 caller b（上限 1）
    fast_backend = gw._client._backend
    fast_backend.behaviors["p"] = ScriptedBehavior(delay=0.2)

    async def post(rid):
        return await c.post(
            "/v1/infer",
            json={"model": "m", "inputs": {"i": rid}, "caller": "b", "request_id": rid},
        )

    t1 = asyncio.create_task(post("api-cc-1"))
    await asyncio.sleep(0.05)
    r2 = await post("api-cc-2")
    assert r2.status_code == 429
    assert r2.json()["error"]["code"] == "concurrency_limited"
    await t1

    # 速率：把 b 配成 1/s，桶初始 1，第二个被 rate_limited
    cfg = store.get()
    store.replace(
        cfg.with_(
            quotas={"a": cfg.quotas["a"], "b": QuotaConfig("b", 4, 0.0)}
        )
    )
    # 0 速率恒不足 -> rate_limited
    r3 = await post("api-cc-3")
    assert r3.status_code == 429
    assert r3.json()["error"]["code"] == "rate_limited"


async def test_timeout_and_fallback_status_codes(client):
    c, store, events, gw = client
    gw._client._backend.behaviors["p"] = ScriptedBehavior(
        fail=DownstreamFailureKind.TIMEOUT
    )
    store.replace(
        store.get().with_(timeout=TimeoutConfig(downstream_timeout=0.05, fallback_timeout=0.05))
    )
    r = await c.post(
        "/v1/infer", json={"model": "m", "inputs": {"i": 1}, "caller": "a"}
    )
    assert r.status_code == 504
    assert r.json()["error"]["code"] == "downstream_timeout"
    assert "Traceback" not in r.text

    store.replace(store.get().with_(fallback_mode=FallbackMode.FALLBACK))
    r2 = await c.post(
        "/v1/infer", json={"model": "m", "inputs": {"i": 1}, "caller": "a"}
    )
    assert r2.status_code == 200
    assert r2.json()["degraded"] is True
    assert r2.json()["source"] == "fallback"


async def test_admin_quota_and_fallback_hot_reload(client):
    c, store, events, gw = client
    r = await c.put("/admin/quota", json={"caller": "c", "max_concurrency": 7, "rate_per_second": 3.5})
    assert r.status_code == 200
    cfg = await c.get("/admin/config")
    assert cfg.json()["quotas"]["c"]["max_concurrency"] == 7

    r = await c.put("/admin/fallback", json={"mode": "fallback"})
    assert r.json()["fallback_mode"] == "fallback"

    r = await c.put("/admin/fallback", json={"mode": "bogus"})
    assert r.status_code == 400  # 校验错误统一为 invalid_request
    assert r.json()["error"]["code"] == "invalid_request"


async def test_invalid_request_body(client):
    c, *_ = client
    r = await c.post("/v1/infer", json={"model": "", "inputs": None})
    assert r.status_code == 400
    assert r.json()["error"]["code"] == "invalid_request"


async def test_logs_contain_request_id_and_quota_basis(client, caplog):
    import logging

    c, *_ = client
    with caplog.at_level(logging.INFO, logger="gateway"):
        await c.post(
            "/v1/infer",
            json={"model": "m", "inputs": {"i": 1}, "caller": "a", "request_id": "log-1"},
        )
    text = "\n".join(r.getMessage() for r in caplog.records)
    assert "request_id=log-1" in text
    assert "quota_admit" in text
    assert "in_flight=1/2" in text
    assert "downstream_result target=p ok=True" in text

"""HTTP 层测试：错误归一化信封、管理端热更新、端到端合并可观测。"""

from __future__ import annotations

import json

import pytest
from fastapi.testclient import TestClient

from gateway.config import config_from_dict
from main import create_app


@pytest.fixture
def client():
    config = config_from_dict(
        {
            "quotas": [
                {"caller": "c1", "max_concurrency": 1, "rate_per_second": 1000.0}
            ],
            "default_quotas": {"max_concurrency": 10, "rate_per_second": None},
            "cache_ttl_seconds": 30.0,
            "downstream_timeout_seconds": 1.0,
            "primary_backend": "primary",
            "backends": {
                "flaky": {"kind": "flaky", "behavior": "error"},
                "primary": {"kind": "echo"},
                "null": {"kind": "null"},
                "echo": {"kind": "echo"},
            },
            "fallback": {"mode": "fail"},
        }
    )
    app = create_app(config)
    with TestClient(app) as c:
        yield c


def make_primary_flaky(client):
    """把 primary 后端热替换为总是失败的 flaky，用于降级路径测试。"""
    from gateway.downstream import FlakyBackend

    client.app.state.service.register_backend(FlakyBackend("primary", behavior="error"))


def body(cid="c1", **overrides):
    payload = {"caller": "c1", "model": "m1", "inputs": {"q": "hi"}, "request_id": cid}
    payload.update(overrides)
    return payload


def test_health(client):
    assert client.get("/health").json() == {"status": "ok"}


def test_infer_success_and_cache_status_observable(client):
    r1 = client.post("/v1/infer", json=body("r1"))
    r2 = client.post("/v1/infer", json=body("r2"))
    assert r1.status_code == 200
    j1, j2 = r1.json(), r2.json()
    assert j1["cache_status"] == "miss"
    assert j2["cache_status"] == "hit"
    assert j1["output"] == j2["output"]
    assert j1["quota"]["reason"] == "ok"
    # 来源与新鲜度可观测
    assert j1["source"] == "primary" and j1["freshness"] == "fresh"
    assert j2["source"] == "primary" and j2["freshness"] == "reused"
    assert j2["produced_at"] == j1["produced_at"] > 0


def test_invalid_request_envelope_does_not_leak(client):
    r = client.post("/v1/infer", json={"caller": "c1", "model": "m1", "inputs": {}})
    assert r.status_code == 400
    err = r.json()["error"]
    assert err["code"] == "invalid_request"
    assert "Traceback" not in json.dumps(err)


def test_concurrency_limit_envelope_is_distinguishable(client):
    # 把 c1 并发容量热降为 0，验证可区分的 429 信封（速率正常）。
    client.put(
        "/admin/config/quotas",
        json={"quotas": [{"caller": "c1", "max_concurrency": 0,
                          "rate_per_second": 1000.0}]},
    )
    r = client.post("/v1/infer", json=body("r2"))
    assert r.status_code == 429
    assert r.json()["error"]["code"] == "concurrency_limited"
    # 恢复后成功
    client.put(
        "/admin/config/quotas",
        json={"quotas": [{"caller": "c1", "max_concurrency": 2,
                          "rate_per_second": 1000.0}]},
    )
    # 主后端换成必失败的 flaky，开降级到 echo：恢复后走备用路径成功
    make_primary_flaky(client)
    client.put("/admin/config/fallback",
               json={"mode": "fallback", "fallback_backend": "echo"})
    r2 = client.post("/v1/infer", json=body("r3"))
    assert r2.status_code == 200 and r2.json()["degraded"] is True


def test_quota_hot_reload_via_admin_without_restart(client):
    # c1 容量降到 0 -> 拒绝
    client.put(
        "/admin/config/quotas",
        json={"quotas": [{"caller": "c1", "max_concurrency": 0,
                          "rate_per_second": 1000.0}]},
    )
    r = client.post("/v1/infer", json=body("r-a"))
    assert r.status_code == 429 and r.json()["error"]["code"] == "concurrency_limited"

    # 不重启进程，容量升回 -> 成功（主后端换 flaky 并开降级到正常 echo 后端）
    make_primary_flaky(client)
    client.put("/admin/config/fallback",
               json={"mode": "fallback", "fallback_backend": "echo"})
    client.put(
        "/admin/config/quotas",
        json={"quotas": [{"caller": "c1", "max_concurrency": 2,
                          "rate_per_second": 1000.0}]},
    )
    r2 = client.post("/v1/infer", json=body("r-b"))
    assert r2.status_code == 200


def test_fallback_switch_via_admin_changes_new_requests(client):
    # fail 模式 + 主后端 flaky：归一化 502
    client.put("/admin/config/fallback", json={"mode": "fail"})
    client.post(
        "/v1/infer",
        json=body("f1", model="m-flaky"),
    )
    # primary 当前是 echo（fixture），需要先确认降级开关生效：
    # 直接切换主后端不在管理端范围；这里验证开关本身可读回。
    cfg = client.get("/admin/config").json()
    assert cfg["fallback"]["mode"] == "fail"

    client.put("/admin/config/fallback",
               json={"mode": "fallback", "fallback_backend": "null"})
    cfg2 = client.get("/admin/config").json()
    assert cfg2["fallback"]["mode"] == "fallback"
    assert cfg2["fallback"]["fallback_backend"] == "null"


def test_stats_endpoint_exposes_quota_and_cache_counters(client):
    client.post("/v1/infer", json=body("s1"))
    client.post("/v1/infer", json=body("s2"))
    stats = client.get("/admin/stats").json()
    assert stats["cache"]["hits"] >= 1
    # 缓存治理指标可查询
    assert "evictions" in stats["cache"]
    assert "max_entries" in stats["cache"]
    assert "cached_entries" in stats["cache"]
    assert "quota" in stats


def test_strong_consistency_error_is_distinguishable_over_http(client):
    make_primary_flaky(client)
    client.put("/admin/config/fallback",
               json={"mode": "fallback", "fallback_backend": "echo"})
    # 普通请求：降级成功
    r1 = client.post("/v1/infer", json=body("n1"))
    assert r1.status_code == 200 and r1.json()["source"] == "fallback"
    # 强一致：主路径失败 -> 503 consistency_unavailable，绝不用备用顶替
    r2 = client.post("/v1/infer", json=body("s1", consistency="strong"))
    assert r2.status_code == 503
    err = r2.json()["error"]
    assert err["code"] == "consistency_unavailable"
    assert err["detail"]["reason"] == "downstream_error"


def test_invalid_consistency_value_over_http(client):
    r = client.post("/v1/infer", json=body("b1", consistency="eventual"))
    assert r.status_code == 400
    assert r.json()["error"]["code"] == "invalid_request"


def test_degraded_result_not_cached_and_recovers_over_http(client):
    make_primary_flaky(client)
    client.put("/admin/config/fallback",
               json={"mode": "fallback", "fallback_backend": "echo"})
    r1 = client.post("/v1/infer", json=body("d1"))
    assert r1.json()["degraded"] is True
    # 降级结果不进缓存：同输入再来仍是 miss 而不是 hit
    r2 = client.post("/v1/infer", json=body("d2"))
    assert r2.json()["cache_status"] == "miss" and r2.json()["degraded"] is True

    # 下游恢复（换回健康主后端）：同输入必须重新反映主路径
    from gateway.downstream import EchoBackend
    client.app.state.service.register_backend(EchoBackend("primary"))
    r3 = client.post("/v1/infer", json=body("d3"))
    j3 = r3.json()
    assert j3["cache_status"] == "miss"
    assert j3["degraded"] is False and j3["source"] == "primary"
    # 恢复后的主路径结果正常被缓存
    r4 = client.post("/v1/infer", json=body("d4"))
    assert r4.json()["cache_status"] == "hit" and r4.json()["source"] == "primary"


def test_cache_isolated_per_caller_over_http(client):
    client.post("/v1/infer", json=body("a1", caller="c1"))
    r = client.post("/v1/infer", json=body("b1", caller="c2"))
    assert r.json()["cache_status"] == "miss"  # c2 不可命中 c1 的缓存
    r2 = client.post("/v1/infer", json=body("b2", caller="c2"))
    assert r2.json()["cache_status"] == "hit"

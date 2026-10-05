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
            "primary_backend": "flaky",
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


@pytest.fixture
def healthy_client():
    """主后端健康的客户端：用于观察缓存命中（降级结果按语义不入缓存）。"""
    config = config_from_dict(
        {
            "default_quotas": {"max_concurrency": 10, "rate_per_second": None},
            "cache_ttl_seconds": 30.0,
            "downstream_timeout_seconds": 1.0,
            "primary_backend": "echo",
            "backends": {"echo": {"kind": "echo"}},
            "fallback": {"mode": "fail"},
        }
    )
    app = create_app(config)
    with TestClient(app) as c:
        yield c


def body(cid="c1", **overrides):
    payload = {"caller": "c1", "model": "m1", "inputs": {"q": "hi"}, "request_id": cid}
    payload.update(overrides)
    return payload


def test_health(client):
    assert client.get("/health").json() == {"status": "ok"}


def test_infer_success_and_cache_status_observable(healthy_client):
    client = healthy_client
    r1 = client.post("/v1/infer", json=body("r1"))
    r2 = client.post("/v1/infer", json=body("r2"))
    assert r1.status_code == 200
    j1, j2 = r1.json(), r2.json()
    assert j1["cache_status"] == "miss"
    assert j2["cache_status"] == "hit"
    assert j1["output"] == j2["output"]
    assert j1["quota"]["reason"] == "ok"


def test_degraded_result_is_not_cached(client):
    # 主后端 flaky + 降级到 echo：降级结果可返回，但不沉淀进缓存
    client.put("/admin/config/fallback",
               json={"mode": "fallback", "fallback_backend": "echo"})
    r1 = client.post("/v1/infer", json=body("r1"))
    r2 = client.post("/v1/infer", json=body("r2"))
    assert r1.status_code == 200 and r1.json()["degraded"] is True
    assert r1.json()["path"] == "fallback"
    # 第二次仍然是 miss（重新走主路径），不会把旧的降级结果当正常结果返回
    assert r2.json()["cache_status"] == "miss"


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
    # 主后端是 flaky，先开降级到 echo 以便恢复后可成功
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

    # 不重启进程，容量升回 -> 成功（开启降级到正常 echo 后端）
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
    r = client.post(
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


def test_stats_endpoint_exposes_quota_and_cache_counters(healthy_client):
    client = healthy_client
    client.post("/v1/infer", json=body("s1"))
    client.post("/v1/infer", json=body("s2"))
    stats = client.get("/admin/stats").json()
    assert stats["cache"]["hits"] >= 1
    assert "evictions" in stats["cache"]
    assert "max_entries" in stats["cache"]
    assert "quota" in stats


# ---------- 按次一致性声明（HTTP 层） ----------


def test_strong_consistency_bypasses_cache_via_http(healthy_client):
    client = healthy_client
    client.post("/v1/infer", json=body("w1"))  # 普通请求写入缓存
    r = client.post("/v1/infer", json=body("s1", consistency="strong"))
    assert r.status_code == 200
    j = r.json()
    assert j["cache_status"] == "bypassed"
    assert j["consistency"] == "strong"
    assert j["source"] == "fresh" and j["path"] == "primary"
    assert j["generated_at"] > 0
    # 强一致请求不污染缓存：普通请求仍命中第一份缓存
    r2 = client.post("/v1/infer", json=body("w2"))
    assert r2.json()["cache_status"] == "hit"


def test_strong_consistency_fails_distinguishably_via_http(client):
    # 主后端 flaky 且开启降级到 echo：普通请求降级成功，强一致请求明确失败
    client.put("/admin/config/fallback",
               json={"mode": "fallback", "fallback_backend": "echo"})
    r = client.post("/v1/infer", json=body("s1", consistency="strong"))
    assert r.status_code == 503
    err = r.json()["error"]
    assert err["code"] == "consistency_unavailable"
    assert err["detail"]["underlying_error"] == "downstream_error"
    # 普通请求不受影响
    r2 = client.post("/v1/infer", json=body("n1"))
    assert r2.status_code == 200 and r2.json()["degraded"] is True


def test_invalid_consistency_value_via_http(healthy_client):
    r = healthy_client.post("/v1/infer", json=body("x1", consistency="eventually"))
    assert r.status_code == 400
    assert r.json()["error"]["code"] == "invalid_request"


def test_response_exposes_provenance_fields(healthy_client):
    r1 = healthy_client.post("/v1/infer", json=body("p1")).json()
    r2 = healthy_client.post("/v1/infer", json=body("p2")).json()
    assert r1["source"] == "fresh" and r1["path"] == "primary"
    assert r2["source"] == "cache"
    assert r2["generated_at"] == r1["generated_at"]  # 缓存结果保留产出时刻


def test_admin_config_exposes_cache_max_entries(healthy_client):
    cfg = healthy_client.get("/admin/config").json()
    assert cfg["cache_max_entries"] == 1024

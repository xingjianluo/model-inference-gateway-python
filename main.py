"""推理网关 HTTP 入口（本地零依赖可运行）。

启动：
    uv run uvicorn main:app --reload
示例：
    curl -s localhost:8000/v1/infer -H 'Content-Type: application/json' \
      -d '{"caller":"demo","model":"m1","inputs":{"prompt":"hi"}}'
"""

from __future__ import annotations

import json
import logging
import os
import uuid
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from gateway.config import (
    ConfigStore,
    FallbackPolicy,
    GatewayConfig,
    QuotaRule,
    config_from_dict,
)
from gateway.errors import GatewayError
from gateway.logging_setup import configure_logging
from gateway.service import GatewayService, InferenceRequest

DEFAULT_CONFIG: dict[str, Any] = {
    "quotas": [
        {"caller": "demo", "max_concurrency": 4, "rate_per_second": 10.0},
    ],
    "default_quotas": {"max_concurrency": 8, "rate_per_second": None},
    "cache_ttl_seconds": 30.0,
    "coalesce_window_seconds": 0.0,
    "downstream_timeout_seconds": 2.0,
    "primary_backend": "primary",
    "backends": {
        "primary": {"kind": "echo", "delay": 0.0},
        "echo": {"kind": "echo"},
        "null": {"kind": "null"},
        "flaky": {"kind": "flaky", "behavior": "error"},
        "slow": {"kind": "flaky", "behavior": "slow", "delay": 5.0},
    },
    "fallback": {"mode": "fail", "fallback_backend": None},
}


class InferBody(BaseModel):
    caller: str = Field(min_length=1)
    model: str = Field(min_length=1)
    inputs: dict[str, Any]
    request_id: str | None = None
    consistency: str = "cached"  # cached | strong


def load_config() -> GatewayConfig:
    path = os.environ.get("GATEWAY_CONFIG")
    if path:
        data = json.loads(Path(path).read_text(encoding="utf-8"))
        return config_from_dict(data)
    return config_from_dict(DEFAULT_CONFIG)


def create_app(config: GatewayConfig | None = None) -> FastAPI:
    logger = configure_logging(
        logging.DEBUG if os.environ.get("GATEWAY_DEBUG") else logging.INFO
    )
    store = ConfigStore(config if config is not None else load_config())
    service = GatewayService(store, logger=logger)

    app = FastAPI(title="model-inference-gateway", version="0.1.0")
    app.state.config_store = store
    app.state.service = service

    @app.exception_handler(GatewayError)
    async def on_gateway_error(_: Request, exc: GatewayError) -> JSONResponse:
        # 统一错误信封：只有 code/message/detail，不携带内部堆栈
        return JSONResponse(status_code=exc.status_code, content=exc.to_public())

    @app.exception_handler(Exception)
    async def on_unexpected_error(_: Request, exc: Exception) -> JSONResponse:
        # 兜底：内部异常细节绝不出现在响应体中
        logger.error(
            "unhandled internal error",
            extra={"event": "internal_error"},
            exc_info=exc,
        )
        return JSONResponse(
            status_code=500,
            content={"error": {"code": "internal_error", "message": "internal error"}},
        )

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/v1/infer")
    async def infer(body: InferBody) -> dict[str, Any]:
        request = InferenceRequest(
            caller=body.caller,
            model=body.model,
            inputs=body.inputs,
            request_id=body.request_id or f"req-{uuid.uuid4().hex[:12]}",
            consistency=body.consistency,
        )
        response = await service.infer(request)
        return {
            "request_id": response.request_id,
            "output": response.output,
            "cache_status": response.cache_status,
            "backend": response.backend,
            "degraded": response.degraded,
            "source": response.source,
            "freshness": response.freshness,
            "produced_at": response.produced_at,
            "quota": response.quota,
        }

    @app.get("/admin/config")
    async def get_config() -> dict[str, Any]:
        cfg = store.snapshot()
        return {
            "cache_ttl_seconds": cfg.cache_ttl_seconds,
            "cache_max_entries": cfg.cache_max_entries,
            "coalesce_window_seconds": cfg.coalesce_window_seconds,
            "downstream_timeout_seconds": cfg.downstream_timeout_seconds,
            "primary_backend": cfg.primary_backend,
            "fallback": {
                "mode": cfg.fallback.mode,
                "fallback_backend": cfg.fallback.fallback_backend,
                "retry_on": sorted(cfg.fallback.retry_on),
            },
            "quotas": [
                {
                    "caller": r.caller,
                    "max_concurrency": r.max_concurrency,
                    "rate_per_second": r.rate_per_second,
                }
                for r in cfg.quotas.values()
            ],
            "default_quotas": {
                "max_concurrency": cfg.default_rule.max_concurrency,
                "rate_per_second": cfg.default_rule.rate_per_second,
            },
        }

    @app.put("/admin/config/quotas")
    async def put_quotas(payload: dict[str, Any]) -> dict[str, Any]:
        rules = {
            item["caller"]: QuotaRule(
                caller=item["caller"],
                max_concurrency=item.get("max_concurrency", 8),
                rate_per_second=item.get("rate_per_second"),
            )
            for item in payload.get("quotas", [])
        }
        await store.update_quotas(rules)
        return {"status": "applied"}

    @app.put("/admin/config/fallback")
    async def put_fallback(payload: dict[str, Any]) -> dict[str, Any]:
        policy = FallbackPolicy(
            mode=str(payload.get("mode", "fail")),
            fallback_backend=payload.get("fallback_backend"),
            retry_on=frozenset(
                payload.get(
                    "retry_on",
                    ("downstream_timeout", "downstream_error", "downstream_bad_result"),
                )
            ),
        )
        await store.update_fallback(policy)
        return {"status": "applied"}

    @app.post("/admin/config/reload")
    async def reload_config() -> dict[str, Any]:
        await store.update(load_config())
        return {"status": "reloaded"}

    @app.get("/admin/stats")
    async def stats() -> dict[str, Any]:
        return service.stats()

    return app


app = create_app()

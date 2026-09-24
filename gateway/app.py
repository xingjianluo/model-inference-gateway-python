"""FastAPI 装配：推理端点、配置热更新、统一错误归一化。

- POST /v1/infer            推理（准入/合并/缓存/降级全部在此路径生效）
- GET  /healthz             存活检查
- GET  /admin/config        查看当前配置快照
- PUT  /admin/config        全量热更新配置
- PUT  /admin/quota         调整单个调用方配额（即时生效，不重置计数）
- PUT  /admin/fallback      切换降级开关 fail/fallback（即时生效）
- GET  /admin/events        查看结构化事件（便于本地观察判定依据）
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import asdict
from typing import Any

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from gateway.config import (
    FallbackMode,
    GatewayConfig,
    QuotaConfig,
    parse_config,
)
from gateway.errors import (
    HTTP_STATUS,
    ErrorCode,
    GatewayError,
    InvalidRequestError,
)
from gateway.events import EventRecorder
from gateway.logging_setup import configure_logging, get_logger
from gateway.service import InferenceGateway
from gateway.wiring import InMemoryConfigStore, build_gateway, demo_config

_log = get_logger("app")


class InferRequest(BaseModel):
    model: str = Field(min_length=1)
    inputs: Any
    request_id: str | None = None
    caller: str | None = None


class QuotaPatch(BaseModel):
    caller: str = Field(min_length=1)
    max_concurrency: int = Field(ge=0)
    rate_per_second: float = Field(ge=0)


class FallbackPatch(BaseModel):
    mode: FallbackMode


def _config_to_jsonable(config: GatewayConfig) -> dict[str, Any]:
    data = asdict(config)
    data["fallback_mode"] = config.fallback_mode.value
    return data


def create_app(
    config: GatewayConfig | None = None,
    gateway: InferenceGateway | None = None,
    store: InMemoryConfigStore | None = None,
    events: EventRecorder | None = None,
) -> FastAPI:
    configure_logging()
    if gateway is None:
        gateway, store, events, _backend = build_gateway(config or demo_config())
    assert store is not None and events is not None

    app = FastAPI(title="model-inference-gateway", version="0.1.0")
    app.state.gateway = gateway
    app.state.store = store
    app.state.events = events

    @app.exception_handler(GatewayError)
    async def _on_gateway_error(_: Request, exc: GatewayError) -> JSONResponse:
        # 对外只暴露归一化错误码与安全消息，不带堆栈
        status = HTTP_STATUS[exc.code]
        _log.warning("gateway_error code=%s message=%s status=%s", exc.code.value, exc.message, status)
        headers = {}
        if exc.retry_after is not None:
            headers["Retry-After"] = f"{max(0.0, exc.retry_after):.3f}"
        return JSONResponse(exc.to_public(), status_code=status, headers=headers)

    @app.exception_handler(RequestValidationError)
    async def _on_validation_error(_: Request, exc: RequestValidationError) -> JSONResponse:
        return JSONResponse(
            {
                "error": {
                    "code": ErrorCode.INVALID_REQUEST.value,
                    "message": "请求参数不合法",
                    "details": exc.errors(),
                }
            },
            status_code=HTTP_STATUS[ErrorCode.INVALID_REQUEST],
        )

    @app.exception_handler(Exception)
    async def _on_unexpected_error(request: Request, exc: Exception) -> JSONResponse:
        # 内部异常只落日志，绝不外泄
        rid = getattr(request.state, "request_id", None)
        logging.getLogger("gateway.app").exception("request_id=%s unhandled_error", rid)
        return JSONResponse(
            {"error": {"code": ErrorCode.INTERNAL_ERROR.value, "message": "网关内部错误"}},
            status_code=HTTP_STATUS[ErrorCode.INTERNAL_ERROR],
        )

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/v1/infer")
    async def infer(body: InferRequest, request: Request) -> dict[str, Any]:
        request_id = (
            body.request_id
            or request.headers.get("x-request-id")
            or f"req-{uuid.uuid4().hex[:12]}"
        )
        request.state.request_id = request_id
        caller = body.caller or request.headers.get("x-caller") or "anonymous"
        _log.info(
            "request_id=%s infer_in caller=%s model=%s", request_id, caller, body.model
        )
        result = await app.state.gateway.infer(
            request_id=request_id,
            caller=caller,
            model=body.model,
            inputs=body.inputs,
        )
        _log.info(
            "request_id=%s infer_out source=%s degraded=%s fallback_target=%s",
            request_id,
            result.source,
            result.degraded,
            result.fallback_target,
        )
        return {
            "request_id": request_id,
            "model": body.model,
            "output": result.output.get("output"),
            "source": result.source,
            "degraded": result.degraded,
            "fallback_target": result.fallback_target,
        }

    @app.get("/admin/config")
    async def get_config() -> dict[str, Any]:
        return _config_to_jsonable(store.get())

    @app.put("/admin/config")
    async def put_config(request: Request) -> dict[str, Any]:
        raw = await request.json()
        if not isinstance(raw, dict):
            raise InvalidRequestError("配置必须是 JSON 对象")
        new_config = parse_config(raw)
        store.replace(new_config)
        _log.info("config_replaced hot=true fallback_mode=%s", new_config.fallback_mode.value)
        return {"status": "reloaded", "config": _config_to_jsonable(new_config)}

    @app.put("/admin/quota")
    async def put_quota(patch: QuotaPatch) -> dict[str, Any]:
        current = store.get()
        quotas = dict(current.quotas)
        quotas[patch.caller] = QuotaConfig(
            caller=patch.caller,
            max_concurrency=patch.max_concurrency,
            rate_per_second=patch.rate_per_second,
        )
        store.replace(current.with_(quotas=quotas))
        _log.info(
            "quota_updated caller=%s max_concurrency=%s rate_per_second=%s",
            patch.caller,
            patch.max_concurrency,
            patch.rate_per_second,
        )
        return {"status": "updated", "caller": patch.caller}

    @app.put("/admin/fallback")
    async def put_fallback(patch: FallbackPatch) -> dict[str, Any]:
        current = store.get()
        store.replace(current.with_(fallback_mode=patch.mode))
        _log.info("fallback_mode_updated mode=%s", patch.mode.value)
        return {"status": "updated", "fallback_mode": patch.mode.value}

    @app.get("/admin/events")
    async def get_events(request_id: str | None = None) -> dict[str, Any]:
        if request_id:
            items = events.by_request(request_id)
        else:
            items = events.snapshot()
        return {
            "events": [
                {"request_id": e.request_id, "kind": e.kind, "detail": e.detail}
                for e in items
            ]
        }

    return app

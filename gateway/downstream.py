"""下游后端抽象：调用、超时包装、结果校验与内置本地桩后端。

归一化规则（内部异常类型与堆栈不出现在对外信息中）：
- asyncio.TimeoutError/CancelledError(超时) -> DownstreamTimeoutError
- 后端抛出的任何其他异常                    -> DownstreamInvokeError
- 返回非 dict / None / 空 dict            -> DownstreamBadResultError
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

from .errors import (
    DownstreamBadResultError,
    DownstreamInvokeError,
    DownstreamTimeoutError,
)


@dataclass(frozen=True)
class DownstreamResult:
    backend: str
    output: dict[str, Any]
    degraded: bool = False
    latency_ms: float = 0.0


@runtime_checkable
class Backend(Protocol):
    name: str

    async def call(self, payload: dict[str, Any], *, request_id: str) -> dict[str, Any]:
        """成功返回非空 dict；失败可抛任意异常（由 invoke 归一化）。"""
        ...


def _validate(backend: str, result: Any) -> dict[str, Any]:
    if not isinstance(result, dict) or not result:
        raise DownstreamBadResultError(
            "downstream returned an invalid result",
            detail={"backend": backend},
        )
    return result


class EchoBackend:
    """本地确定性后端：回显输入，零外部依赖，供本地运行与测试。"""

    def __init__(self, name: str = "echo", delay: float = 0.0) -> None:
        self.name = name
        self.delay = max(0.0, float(delay))

    async def call(self, payload: dict[str, Any], *, request_id: str) -> dict[str, Any]:
        if self.delay:
            await asyncio.sleep(self.delay)
        return {
            "model": payload.get("model"),
            "echo": payload.get("inputs"),
            "served_by": self.name,
        }


class NullBackend:
    """本地“异常结果”后端：返回 None，用于触发 bad_result 降级。"""

    def __init__(self, name: str = "null") -> None:
        self.name = name

    async def call(self, payload: dict[str, Any], *, request_id: str) -> dict[str, Any]:
        return None  # type: ignore[return-value]


class FlakyBackend:
    """可控行为后端，供测试：

    behavior:
      - "ok":          返回正常结果
      - "error":       抛 RuntimeError（内部异常，调用方不可见细节）
      - "bad_result":  返回空 dict（触发 bad_result）
      - "slow":        睡眠 delay 秒（配合超时使用）
    """

    def __init__(
        self,
        name: str = "flaky",
        behavior: str = "error",
        delay: float = 0.0,
        fail_call_ids: set[str] | None = None,
    ) -> None:
        self.name = name
        self.behavior = behavior
        self.delay = max(0.0, float(delay))
        # 可按 request_id 精确指定哪些调用失败
        self.fail_call_ids = fail_call_ids

    async def call(self, payload: dict[str, Any], *, request_id: str) -> dict[str, Any]:
        should_fail = self.fail_call_ids is None or request_id in self.fail_call_ids
        if self.behavior == "slow":
            await asyncio.sleep(self.delay)
            return {"model": payload.get("model"), "served_by": self.name}
        if self.behavior == "error" and should_fail:
            raise RuntimeError("simulated downstream failure (internal detail)")
        if self.behavior == "bad_result" and should_fail:
            return {}
        return {
            "model": payload.get("model"),
            "inputs": payload.get("inputs"),
            "served_by": self.name,
        }


async def invoke(
    backend: Backend,
    payload: dict[str, Any],
    *,
    request_id: str,
    timeout: float,
) -> DownstreamResult:
    """带超时与归一化的单次下游调用。"""
    started = time.monotonic()
    try:
        raw = await asyncio.wait_for(
            backend.call(payload, request_id=request_id), timeout=timeout
        )
    except TimeoutError:
        raise DownstreamTimeoutError(
            "downstream request timed out", detail={"backend": backend.name}
        )
    except (DownstreamBadResultError, DownstreamTimeoutError, DownstreamInvokeError):
        raise
    except Exception:
        # 不回传原始异常文本/堆栈，统一为对外可解释的错误
        raise DownstreamInvokeError(
            "downstream invocation failed", detail={"backend": backend.name}
        )

    output = _validate(backend.name, raw)
    latency_ms = (time.monotonic() - started) * 1000.0
    return DownstreamResult(
        backend=backend.name, output=output, degraded=False, latency_ms=latency_ms
    )


def build_backend(spec: dict[str, Any], name: str) -> Backend:
    """按配置 dict 构造内置后端（echo/null/flaky），零外部依赖。"""
    kind = spec.get("kind", "echo")
    if kind == "echo":
        return EchoBackend(name=name, delay=float(spec.get("delay", 0.0)))
    if kind == "null":
        return NullBackend(name=name)
    if kind == "flaky":
        return FlakyBackend(
            name=name,
            behavior=str(spec.get("behavior", "error")),
            delay=float(spec.get("delay", 0.0)),
        )
    raise ValueError(f"unknown backend kind: {kind!r}")

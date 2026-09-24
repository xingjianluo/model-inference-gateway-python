"""下游推理客户端抽象与可脚本化的本地实现。

- ``ScriptedBackend`` 完全在进程内按脚本运行（延迟/失败/返回），不依赖外部服务。
- ``DownstreamClient`` 统一施加超时，并把各类底层异常归一化为 ``DownstreamError``，
  内部异常文本不会向上游泄漏。
"""

from __future__ import annotations

import asyncio
import time
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Protocol

from gateway.config import TimeoutConfig
from gateway.events import EventRecorder
from gateway.logging_setup import get_logger

_log = get_logger("downstream")


class DownstreamFailureKind(str, Enum):
    TIMEOUT = "timeout"
    UNAVAILABLE = "unavailable"
    BAD_RESPONSE = "bad_response"


class DownstreamError(Exception):
    """下游调用失败，仅携带归一化类别与安全消息。"""

    def __init__(self, kind: DownstreamFailureKind, message: str) -> None:
        super().__init__(message)
        self.kind = kind


class InferenceBackend(Protocol):
    async def infer(self, target: str, payload: dict[str, Any]) -> dict[str, Any]:
        ...


@dataclass
class ScriptedBehavior:
    delay: float = 0.0
    fail: DownstreamFailureKind | None = None
    value: dict[str, Any] | None = None


@dataclass
class ScriptedBackend:
    """测试/本地演示后端。

    behaviors 按 target 配置；default 为缺省行为（默认返回回显结果）。
    记录调用次数与在飞数，供测试断言"下游只被调用一次"等。
    """

    behaviors: dict[str, ScriptedBehavior] = field(default_factory=dict)
    default: ScriptedBehavior = field(default_factory=lambda: ScriptedBehavior())
    call_counts: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    in_flight_counts: dict[str, int] = field(default_factory=lambda: defaultdict(int))

    async def infer(self, target: str, payload: dict[str, Any]) -> dict[str, Any]:
        self.call_counts[target] += 1
        self.in_flight_counts[target] += 1
        try:
            behavior = self.behaviors.get(target, self.default)
            if behavior.delay:
                await asyncio.sleep(behavior.delay)
            if behavior.fail is DownstreamFailureKind.UNAVAILABLE:
                raise RuntimeError("simulated connection failure")
            if behavior.fail is DownstreamFailureKind.TIMEOUT:
                # 等待远超客户端超时，由 wait_for 触发；同时响应取消。
                await asyncio.sleep(3600)
            if behavior.fail is DownstreamFailureKind.BAD_RESPONSE:
                return {"output": None}
            if behavior.value is not None:
                return dict(behavior.value)
            return {"output": {"model": target, "echo": payload}}
        finally:
            self.in_flight_counts[target] -= 1


def _validate(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict) or "output" not in value or value["output"] is None:
        raise DownstreamError(
            DownstreamFailureKind.BAD_RESPONSE, "下游返回结构不合法"
        )
    return value


class DownstreamClient:
    def __init__(
        self,
        backend: InferenceBackend,
        timeout_config: TimeoutConfig,
        events: EventRecorder,
    ) -> None:
        self._backend = backend
        self._timeouts = timeout_config
        self._events = events

    async def call(
        self,
        target: str,
        payload: dict[str, Any],
        timeout: float,
        request_id: str,
    ) -> dict[str, Any]:
        started = time.monotonic()
        try:
            value = await asyncio.wait_for(self._backend.infer(target, payload), timeout=timeout)
            value = _validate(value)
        except asyncio.TimeoutError:
            latency = time.monotonic() - started
            self._record(request_id, target, False, latency, DownstreamFailureKind.TIMEOUT)
            raise DownstreamError(
                DownstreamFailureKind.TIMEOUT, f"下游 {target} 调用超时"
            ) from None
        except DownstreamError as exc:
            latency = time.monotonic() - started
            self._record(request_id, target, False, latency, exc.kind)
            raise
        except asyncio.CancelledError:
            # 上游取消必须向上传播，不包装、不吞掉
            self._record(request_id, target, False, time.monotonic() - started, None, cancelled=True)
            raise
        except Exception:
            # 其他底层异常一律归一化为"不可用"，内部细节不外泄
            latency = time.monotonic() - started
            self._record(
                request_id, target, False, latency, DownstreamFailureKind.UNAVAILABLE
            )
            raise DownstreamError(
                DownstreamFailureKind.UNAVAILABLE, f"下游 {target} 不可用"
            ) from None
        latency = time.monotonic() - started
        self._record(request_id, target, True, latency, None)
        return value

    def _record(
        self,
        request_id: str,
        target: str,
        ok: bool,
        latency: float,
        failure: DownstreamFailureKind | None,
        cancelled: bool = False,
    ) -> None:
        detail: dict[str, Any] = {
            "target": target,
            "ok": ok,
            "latency": round(latency, 6),
        }
        if failure is not None:
            detail["failure"] = failure.value
        if cancelled:
            detail["cancelled"] = True
        self._events.record(request_id, "downstream_result", **detail)
        _log.info(
            "request_id=%s downstream_result target=%s ok=%s latency=%.4f failure=%s",
            request_id,
            target,
            ok,
            latency,
            failure.value if failure else None,
        )

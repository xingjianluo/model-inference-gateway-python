"""降级执行器。

- 每个请求在受理时取得不可变的 ``GatewayConfig`` 快照；降级开关在此之后切换，
  不影响该请求，因此"切换期间已受理请求的结果与错误保持一致且可解释"。
- FAIL：明确失败，携带主路径的归一化失败类别。
- FALLBACK：存在备用 target 时转备用路径（独立超时）；备用也失败时
  返回 used=True 且 error_kind 为主/备的最终类别，由上层映射为对外错误。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from gateway.config import FallbackMode, GatewayConfig
from gateway.downstream import DownstreamClient, DownstreamError, DownstreamFailureKind
from gateway.events import EventRecorder
from gateway.logging_setup import get_logger

_log = get_logger("fallback")


@dataclass
class FallbackOutcome:
    used: bool
    value: dict[str, Any] | None = None
    fallback_target: str | None = None
    error_kind: DownstreamFailureKind | None = None


class FallbackExecutor:
    def __init__(self, client: DownstreamClient, events: EventRecorder) -> None:
        self._client = client
        self._events = events

    async def try_fallback(
        self,
        *,
        request_id: str,
        config: GatewayConfig,
        model: str,
        payload: dict[str, Any],
        primary_error_kind: DownstreamFailureKind,
    ) -> FallbackOutcome:
        route = config.route_for(model)

        if config.fallback_mode is not FallbackMode.FALLBACK or route.fallback is None:
            self._events.record(
                request_id,
                "fallback_decision",
                used=False,
                reason=(
                    "no_fallback_target"
                    if route.fallback is None
                    else "mode_fail"
                ),
                fallback_mode=config.fallback_mode.value,
                primary_error=primary_error_kind.value,
            )
            _log.info(
                "request_id=%s fallback_decision used=False mode=%s primary_error=%s",
                request_id,
                config.fallback_mode.value,
                primary_error_kind.value,
            )
            return FallbackOutcome(used=False, error_kind=primary_error_kind)

        target = route.fallback
        self._events.record(
            request_id,
            "fallback_decision",
            used=True,
            fallback_target=target,
            primary_error=primary_error_kind.value,
        )
        _log.info(
            "request_id=%s fallback_decision used=True target=%s primary_error=%s",
            request_id,
            target,
            primary_error_kind.value,
        )
        try:
            value = await self._client.call(
                target=target,
                payload=payload,
                timeout=config.timeout.fallback_timeout,
                request_id=request_id,
            )
        except DownstreamError as exc:
            _log.info(
                "request_id=%s fallback_failed target=%s error=%s",
                request_id,
                target,
                exc.kind.value,
            )
            # 备路径失败：以主路径错误类别对外（更能反映根因）
            return FallbackOutcome(
                used=True, fallback_target=target, error_kind=primary_error_kind
            )
        return FallbackOutcome(used=True, value=value, fallback_target=target)

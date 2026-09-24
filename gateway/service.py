"""网关编排服务。

单次推理的完整语义：
1. 读取不可变配置快照（降级开关在本请求受理后切换不影响本请求）。
2. quota.acquire 判定并发/速率；拒绝直接抛出可区分的对外错误。
3. 查结果缓存：命中直接返回（source=cache）。
4. 进入 Coalescer：合并窗口内的相同 key 共享一次下游调用
   （leader 实际调用，follower 标记 source=coalesced）。
5. 下游超时/不可用/异常结果 -> 按快照中的策略 FAIL / FALLBACK。
6. 仅成功的主路径结果写缓存；任何失败/取消都不写。
7. finally 中 release 配额槽：成功、失败、取消、超时路径均释放，不残留。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from gateway.cache import Coalescer, ResultCache, make_key
from gateway.config import ConfigStore, GatewayConfig
from gateway.downstream import DownstreamClient, DownstreamError
from gateway.errors import (
    ConcurrencyLimitedError,
    DownstreamBadResponseError,
    DownstreamTimeoutError,
    DownstreamUnavailableError,
    FallbackFailedError,
    RateLimitedError,
)
from gateway.events import EventRecorder
from gateway.fallback import FallbackExecutor
from gateway.logging_setup import get_logger
from gateway.quota import QuotaManager

_log = get_logger("service")

#: 共享工作返回的内部信封键
_DEGRADED_MARK = "_gateway_degraded"
_FALLBACK_TARGET_MARK = "_gateway_fallback_target"


@dataclass
class InferResult:
    output: dict[str, Any]
    source: str  # cache / coalesced / downstream / fallback
    request_id: str
    degraded: bool = False
    fallback_target: str | None = None


class InferenceGateway:
    def __init__(
        self,
        config_store: ConfigStore,
        quota: QuotaManager,
        cache: ResultCache,
        coalescer: Coalescer,
        client: DownstreamClient,
        fallback: FallbackExecutor,
        events: EventRecorder,
    ) -> None:
        self._store = config_store
        self._quota = quota
        self._cache = cache
        self._coalescer = coalescer
        self._client = client
        self._fallback = fallback
        self._events = events

    async def infer(
        self,
        *,
        request_id: str,
        caller: str,
        model: str,
        inputs: Any,
    ) -> InferResult:
        # 1) 受理时固定配置快照
        config = self._store.get()
        key = make_key(model, inputs)

        # 2) 配额准入
        decision = await self._quota.acquire(caller, request_id)
        if not decision.allowed:
            if decision.reason == "concurrency_limited":
                raise ConcurrencyLimitedError(
                    f"调用方 {caller} 并发已达上限 {decision.max_concurrency}"
                )
            raise RateLimitedError(
                f"调用方 {caller} 请求速率超限",
                retry_after=decision.retry_after,
            )

        try:
            return await self._dispatch(
                request_id=request_id,
                config=config,
                model=model,
                inputs=inputs,
                key=key,
            )
        finally:
            # 成功/失败/取消/超时均释放，配额绝不被已结束请求继续占用
            await self._quota.release(caller)

    async def _dispatch(
        self,
        *,
        request_id: str,
        config: GatewayConfig,
        model: str,
        inputs: Any,
        key: str,
    ) -> InferResult:
        # 3) 缓存命中
        cached = self._cache.get(key, request_id)
        if cached is not None:
            return InferResult(output=cached, source="cache", request_id=request_id)

        # 4) 合并窗口 single-flight：leader 内完成下游+降级，follower 共享结果
        async def work() -> dict[str, Any]:
            return await self._leader_work(
                request_id=request_id, config=config, model=model, inputs=inputs, key=key
            )

        envelope, coalesced = await self._coalescer.execute(key, work, request_id)
        degraded = bool(envelope.pop(_DEGRADED_MARK, False))
        fallback_target = envelope.pop(_FALLBACK_TARGET_MARK, None)

        # 5) source 归一：合并方标 coalesced；leader 降级标 fallback，否则 downstream
        if coalesced:
            source = "coalesced"
        elif degraded:
            source = "fallback"
        else:
            source = "downstream"
        return InferResult(
            output=envelope,
            source=source,
            request_id=request_id,
            degraded=degraded,
            fallback_target=fallback_target,
        )

    async def _leader_work(
        self,
        *,
        request_id: str,
        config: GatewayConfig,
        model: str,
        inputs: Any,
        key: str,
    ) -> dict[str, Any]:
        route = config.route_for(model)
        payload = {"model": model, "inputs": inputs}
        try:
            value = await self._client.call(
                target=route.primary,
                payload=payload,
                timeout=config.timeout.downstream_timeout,
                request_id=request_id,
            )
        except DownstreamError as exc:
            outcome = await self._fallback.try_fallback(
                request_id=request_id,
                config=config,
                model=model,
                payload=payload,
                primary_error_kind=exc.kind,
            )
            if outcome.used and outcome.value is not None:
                _log.info(
                    "request_id=%s degraded=True fallback_target=%s",
                    request_id,
                    outcome.fallback_target,
                )
                envelope = dict(outcome.value)
                envelope[_DEGRADED_MARK] = True
                envelope[_FALLBACK_TARGET_MARK] = outcome.fallback_target
                return envelope
            if outcome.used:
                raise FallbackFailedError(
                    f"主路径与备用路径均失败（{exc.kind.value}）"
                ) from None
            await self._raise_normalized(exc, route.primary)
            raise  # pragma: no cover

        # 6) 仅主路径成功才写缓存；超时/失败路径不会到达这里
        self._cache.put_success(key, value, request_id)
        return dict(value)

    @staticmethod
    async def _raise_normalized(exc: DownstreamError, target: str) -> None:
        kind = exc.kind.value
        if kind == "timeout":
            raise DownstreamTimeoutError(f"下游 {target} 调用超时")
        if kind == "bad_response":
            raise DownstreamBadResponseError(f"下游 {target} 返回异常结果")
        raise DownstreamUnavailableError(f"下游 {target} 不可用")

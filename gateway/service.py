"""编排：请求校验 -> 配额准入 -> 缓存/合并 -> 下游调用 -> 降级 -> 归一化响应。

语义保证：
- 准入时快照整份配置（配额规则 + 降级策略）；降级开关在请求处理途中切换
  不改变该请求的判定路径，结果与错误保持一致且可解释。
- 配额获取成功之后，无论成功、失败、客户端取消、下游超时，槽位必定归还
  且只归还一次；被拒请求从不计数。
- 合并等待者在挂起前释放自己的槽位（下游工作由 leader 的单槽承担），
  因此被取消/超时的等待者不会继续占用配额。
- 缓存按调用方隔离（caller 命名空间）；整体容量受 cache_max_entries 约束，
  按 LRU 回收且只影响结果缓存，不触碰在途合并。
- 一致性等级：consistency="strong" 的请求不读缓存、不走备用路径、与普通
  请求的在途合并隔离；主路径此刻拿不到新鲜结果即以 consistency_unavailable
  明确失败。普通请求的合并与缓存行为不变，两类请求互不污染。
- 只有主路径成功结果写入缓存；超时、下游失败、降级路径上的任何异常都不写；
  降级（备用路径）结果只兑现本次合并者、不留缓存，下游恢复后同一输入重新
  反映主路径结果。
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import time
from dataclasses import dataclass
from typing import Any

from .cache import CacheStatus, CoalescingCache
from .config import ConfigStore, GatewayConfig
from .downstream import (
    Backend,
    DownstreamResult,
    build_backend,
)
from .downstream import (
    invoke as downstream_invoke,
)
from .errors import (
    ConcurrencyLimitedError,
    ConsistencyUnavailableError,
    FallbackExhaustedError,
    GatewayError,
    InvalidRequestError,
    RateLimitedError,
)
from .quota import QuotaManager


@dataclass(frozen=True)
class InferenceRequest:
    caller: str
    model: str
    inputs: dict[str, Any]
    request_id: str
    # 一致性等级：
    #   "cached"  默认；允许命中缓存/合并/降级路径
    #   "strong"  必须实时打主路径：不读缓存、不走备用路径，拿不到即明确失败
    consistency: str = "cached"


@dataclass(frozen=True)
class InferenceResponse:
    request_id: str
    output: dict[str, Any]
    cache_status: str
    backend: str
    degraded: bool
    quota: dict[str, Any]
    # 结果来源与新鲜度（可观测、可解释）
    source: str = "primary"      # primary | fallback
    freshness: str = "fresh"     # fresh（本次新算） | reused（复用在途/缓存结果）
    produced_at: float = 0.0     # 结果产出的墙钟时间戳


def _validate_request(request: InferenceRequest) -> None:
    if not request.caller or not isinstance(request.caller, str):
        raise InvalidRequestError("missing caller")
    if not request.model or not isinstance(request.model, str):
        raise InvalidRequestError("missing model")
    if not isinstance(request.inputs, dict) or not request.inputs:
        raise InvalidRequestError("inputs must be a non-empty object")
    if not request.request_id or not isinstance(request.request_id, str):
        raise InvalidRequestError("missing request_id")
    if request.consistency not in ("cached", "strong"):
        raise InvalidRequestError(
            "consistency must be 'cached' or 'strong'",
            detail={"consistency": request.consistency},
        )


class GatewayService:
    def __init__(
        self,
        config_store: ConfigStore,
        *,
        backends: dict[str, Backend] | None = None,
        logger: logging.Logger | None = None,
        clock=time.monotonic,
    ) -> None:
        self._store = config_store
        self._clock = clock
        self._logger = logger or logging.getLogger("gateway")
        self._quota = QuotaManager(config_store.snapshot(), clock=clock)
        config_store.subscribe(lambda old, new: self._on_config(old, new))
        self._cache = CoalescingCache(
            ttl_seconds=config_store.snapshot().cache_ttl_seconds,
            coalesce_window_seconds=config_store.snapshot().coalesce_window_seconds,
            max_entries=config_store.snapshot().cache_max_entries,
            clock=clock,
        )
        self._backends: dict[str, Backend] = dict(backends or {})
        self._resolved_backends: set[str] = set()
        self._ensure_backends(config_store.snapshot())

    # ---------- 配置热更新 ----------

    def _on_config(self, old: GatewayConfig, new: GatewayConfig) -> None:
        # 同步监听器：先调限额（锁内串行，杜绝超卖），再调缓存参数。
        self._quota.apply_config(new)
        self._cache.reconfigure(
            new.cache_ttl_seconds, new.coalesce_window_seconds, new.cache_max_entries
        )
        self._ensure_backends(new)

    def _ensure_backends(self, config: GatewayConfig) -> None:
        for name, spec in config.backends.items():
            if name not in self._backends:
                self._backends[name] = build_backend(spec, name)

    def register_backend(self, backend: Backend) -> None:
        self._backends[backend.name] = backend

    # ---------- 对外主流程 ----------

    def cache_key(self, request: InferenceRequest) -> str:
        raw = json.dumps(
            {"model": request.model, "inputs": request.inputs},
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        )
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]

    async def infer(self, request: InferenceRequest) -> InferenceResponse:
        _validate_request(request)
        strong = request.consistency == "strong"

        # 1) 准入点：此后该请求看到的配置是这份不可变快照
        config = self._store.snapshot()
        decision = self._quota.try_acquire(request.caller)
        self._logger.info(
            "quota decision",
            extra={"request_id": request.request_id, "event": "quota_decision",
                   "consistency": request.consistency,
                   **decision.as_log_fields()},
        )
        if not decision.allowed:
            if decision.reason == "rate_limited":
                raise RateLimitedError(detail={"caller": request.caller})
            raise ConcurrencyLimitedError(detail={"caller": request.caller})

        acquired = True
        try:
            # 缓存按调用方隔离：不同 caller 的相同输入互不命中、互不合并
            key = f"{request.caller}:{self.cache_key(request)}"
            outcome = self._cache.lookup(
                key,
                use_cache=not strong,          # 强一致：绝不读历史缓存
                lane="strong:" if strong else "",  # 强/普通在途合并隔离，互不污染
            )
            ikey = outcome.inflight_key

            if outcome.status is CacheStatus.HIT:
                result: DownstreamResult = outcome.value
                self._logger.info(
                    "cache hit",
                    extra={"request_id": request.request_id, "event": "cache_hit",
                           "cache_key": key, "backend": result.backend},
                )
                return self._response(request, result, CacheStatus.HIT, decision)

            if outcome.status is CacheStatus.COALESCED:
                # 挂起等待期间不占用并发槽：实际下游工作由 leader 承担
                acquired = self._release(request.caller, acquired)
                self._logger.info(
                    "coalesced onto in-flight request",
                    extra={"request_id": request.request_id, "event": "coalesced",
                           "cache_key": key, "consistency": request.consistency},
                )
                result = await asyncio_shield(outcome.future)
                return self._response(request, result, CacheStatus.COALESCED, decision)

            # 2) leader：调用下游（强一致走主路径且禁止降级；使用准入时快照）
            try:
                result = await self._invoke_with_fallback(
                    config, request, allow_fallback=not strong
                )
            except BaseException as exc:
                # 任何失败（含下游超时/调用方取消）都不写结果缓存，并把异常
                # 传播给所有合并者、移除在途条目，避免等待者悬挂或占用配额。
                self._cache.set_failure(key, outcome.future, exc, inflight_key=ikey)
                raise
            # 3) 成功才兑现合并者；降级结果不写结果缓存（见 set_result cache_result）
            self._cache.set_result(
                key, outcome.future, result,
                inflight_key=ikey, cache_result=not result.degraded,
            )
            self._logger.info(
                "downstream success",
                extra={"request_id": request.request_id, "event": "downstream_success",
                       "cache_key": key, **self._result_log(result)},
            )
            return self._response(request, result, CacheStatus.MISS, decision)
        finally:
            if acquired:
                self._quota.release(request.caller)

    def _release(self, caller: str, acquired: bool) -> bool:
        if acquired:
            self._quota.release(caller)
        return False

    # ---------- 下游调用与降级 ----------

    async def _invoke_with_fallback(
        self,
        config: GatewayConfig,
        request: InferenceRequest,
        *,
        allow_fallback: bool = True,
    ) -> DownstreamResult:
        payload = {"model": request.model, "inputs": request.inputs}
        timeout = config.downstream_timeout_seconds
        primary = self._backend_for(config.primary_backend, config)

        try:
            return await downstream_invoke(
                primary, payload, request_id=request.request_id, timeout=timeout
            )
        except GatewayError as primary_err:
            self._log_downstream_failure(request, config.primary_backend, primary_err)
            if not allow_fallback:
                # 强一致请求：不允许备用路径顶替，按可区分错误明确失败
                self._logger.info(
                    "strong consistency unavailable",
                    extra={"request_id": request.request_id,
                           "event": "consistency_unavailable",
                           "backend": config.primary_backend,
                           "error_code": primary_err.code.value},
                )
                raise ConsistencyUnavailableError(
                    "strong consistency required but primary path failed",
                    detail={
                        "backend": config.primary_backend,
                        "reason": primary_err.code.value,
                    },
                ) from primary_err

            policy = config.fallback
            can_fallback = (
                policy.mode == "fallback"
                and bool(policy.fallback_backend)
                and policy.fallback_backend != config.primary_backend
                and primary_err.code.value in policy.retry_on
            )
            if not can_fallback:
                raise

            backup = self._backend_for(policy.fallback_backend, config)
            try:
                result = await downstream_invoke(
                    backup, payload, request_id=request.request_id, timeout=timeout
                )
            except GatewayError as backup_err:
                self._log_downstream_failure(request, policy.fallback_backend, backup_err)
                # 主备均失败：归一化为可解释的单一错误，不泄漏内部堆栈
                raise FallbackExhaustedError(
                    "primary and fallback backends both failed",
                    detail={
                        "primary": config.primary_backend,
                        "primary_error": primary_err.code.value,
                        "fallback": policy.fallback_backend,
                        "fallback_error": backup_err.code.value,
                    },
                )

            object.__setattr__(result, "degraded", True)
            self._logger.info(
                "fallback success",
                extra={"request_id": request.request_id, "event": "fallback_success",
                        "primary": config.primary_backend,
                        "backend": result.backend},
            )
            return result

    def _backend_for(self, name: str | None, config: GatewayConfig) -> Backend:
        if name is None:
            raise InvalidRequestError("backend not configured")
        backend = self._backends.get(name)
        if backend is None:
            spec = config.backends.get(name)
            if spec is None:
                raise InvalidRequestError(f"backend {name!r} not configured")
            backend = build_backend(spec, name)
            self._backends[name] = backend
        return backend

    # ---------- 辅助 ----------

    def _response(
        self,
        request: InferenceRequest,
        result: DownstreamResult,
        status: CacheStatus,
        decision,
    ) -> InferenceResponse:
        response = InferenceResponse(
            request_id=request.request_id,
            output=result.output,
            cache_status=status.value,
            backend=result.backend,
            degraded=result.degraded,
            quota={
                "caller": decision.caller,
                "reason": decision.reason,
                "rule_version": decision.rule_version,
                "active_concurrency_at_admission": decision.active_concurrency,
            },
            source="fallback" if result.degraded else "primary",
            freshness="fresh" if status is CacheStatus.MISS else "reused",
            produced_at=result.produced_at,
        )
        # 每个成功响应一条来源日志：请求标识 + 判定依据 + 结果从哪儿来
        self._logger.info(
            "response",
            extra={
                "request_id": request.request_id,
                "event": "response",
                "consistency": request.consistency,
                "cache_status": response.cache_status,
                "source": response.source,
                "freshness": response.freshness,
                "backend": response.backend,
                "degraded": response.degraded,
                "produced_at": response.produced_at,
            },
        )
        return response

    @staticmethod
    def _result_log(result: DownstreamResult) -> dict:
        return {
            "backend": result.backend,
            "degraded": result.degraded,
            "latency_ms": round(result.latency_ms, 2),
        }

    def _log_downstream_failure(self, request: InferenceRequest, backend: str | None,
                                err: GatewayError) -> None:
        self._logger.warning(
            "downstream failure",
            extra={
                "request_id": request.request_id,
                "event": "downstream_failure",
                "backend": backend,
                "error_code": err.code.value,
            },
        )

    def stats(self) -> dict:
        return {"cache": self._cache.stats(), "quota": self._quota.stats()}


async def asyncio_shield(future):
    """等待合并 future；等待者取消不影响 leader。"""
    return await asyncio.shield(future)

"""对外错误语义：所有失败都归一化为 GatewayError，不泄漏内部堆栈。"""

from __future__ import annotations

from enum import Enum
from typing import Any


class ErrorCode(str, Enum):
    """可区分的对外错误码（字符串值会原样返回给调用方）。"""

    RATE_LIMITED = "rate_limited"
    CONCURRENCY_LIMITED = "concurrency_limited"
    INVALID_REQUEST = "invalid_request"
    DOWNSTREAM_TIMEOUT = "downstream_timeout"
    DOWNSTREAM_ERROR = "downstream_error"
    DOWNSTREAM_BAD_RESULT = "downstream_bad_result"
    FALLBACK_EXHAUSTED = "fallback_exhausted"
    CONSISTENCY_UNAVAILABLE = "consistency_unavailable"
    INTERNAL_ERROR = "internal_error"


# 错误码 -> HTTP 状态码
HTTP_STATUS: dict[ErrorCode, int] = {
    ErrorCode.RATE_LIMITED: 429,
    ErrorCode.CONCURRENCY_LIMITED: 429,
    ErrorCode.INVALID_REQUEST: 400,
    ErrorCode.DOWNSTREAM_TIMEOUT: 504,
    ErrorCode.DOWNSTREAM_ERROR: 502,
    ErrorCode.DOWNSTREAM_BAD_RESULT: 502,
    ErrorCode.FALLBACK_EXHAUSTED: 502,
    ErrorCode.CONSISTENCY_UNAVAILABLE: 503,
    ErrorCode.INTERNAL_ERROR: 500,
}


class GatewayError(Exception):
    """所有网关错误的基类；只携带可对外暴露的信息。"""

    def __init__(
        self,
        code: ErrorCode,
        message: str,
        *,
        detail: Any = None,
        status_code: int | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.public_message = message
        self.detail = detail
        self.status_code = (
            status_code if status_code is not None else HTTP_STATUS[code]
        )

    def to_public(self) -> dict[str, Any]:
        body: dict[str, Any] = {"error": {"code": self.code.value, "message": self.public_message}}
        if self.detail is not None:
            body["error"]["detail"] = self.detail
        return body


class RateLimitedError(GatewayError):
    def __init__(self, message: str = "rate limit exceeded", *, detail: Any = None) -> None:
        super().__init__(ErrorCode.RATE_LIMITED, message, detail=detail)


class ConcurrencyLimitedError(GatewayError):
    def __init__(self, message: str = "concurrency limit exceeded", *, detail: Any = None) -> None:
        super().__init__(ErrorCode.CONCURRENCY_LIMITED, message, detail=detail)


class InvalidRequestError(GatewayError):
    def __init__(self, message: str = "invalid request", *, detail: Any = None) -> None:
        super().__init__(ErrorCode.INVALID_REQUEST, message, detail=detail)


class DownstreamTimeoutError(GatewayError):
    def __init__(self, message: str = "downstream timeout", *, detail: Any = None) -> None:
        super().__init__(ErrorCode.DOWNSTREAM_TIMEOUT, message, detail=detail)


class DownstreamInvokeError(GatewayError):
    def __init__(self, message: str = "downstream invocation failed", *, detail: Any = None) -> None:
        super().__init__(ErrorCode.DOWNSTREAM_ERROR, message, detail=detail)


class DownstreamBadResultError(GatewayError):
    def __init__(self, message: str = "downstream returned an invalid result", *, detail: Any = None) -> None:
        super().__init__(ErrorCode.DOWNSTREAM_BAD_RESULT, message, detail=detail)


class FallbackExhaustedError(GatewayError):
    def __init__(self, message: str = "all downstream paths failed", *, detail: Any = None) -> None:
        super().__init__(ErrorCode.FALLBACK_EXHAUSTED, message, detail=detail)


class ConsistencyUnavailableError(GatewayError):
    """强一致请求在此刻拿不到主路径新鲜结果：明确失败，绝不以旧值/降级顶替。"""

    def __init__(self, message: str = "strong consistency unavailable", *, detail: Any = None) -> None:
        super().__init__(ErrorCode.CONSISTENCY_UNAVAILABLE, message, detail=detail)

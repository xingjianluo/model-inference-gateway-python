"""统一的对外错误类型定义（骨架：仅类型）。"""

from __future__ import annotations

from enum import Enum


class ErrorCode(str, Enum):
    """对外可区分的错误码。"""

    INVALID_REQUEST = "invalid_request"
    CONCURRENCY_LIMITED = "concurrency_limited"
    RATE_LIMITED = "rate_limited"
    DOWNSTREAM_TIMEOUT = "downstream_timeout"
    DOWNSTREAM_UNAVAILABLE = "downstream_unavailable"
    DOWNSTREAM_BAD_RESPONSE = "downstream_bad_response"
    FALLBACK_FAILED = "fallback_failed"
    INTERNAL_ERROR = "internal_error"


#: 错误码到 HTTP 状态码的稳定映射
HTTP_STATUS: dict[ErrorCode, int] = {
    ErrorCode.INVALID_REQUEST: 400,
    ErrorCode.CONCURRENCY_LIMITED: 429,
    ErrorCode.RATE_LIMITED: 429,
    ErrorCode.DOWNSTREAM_TIMEOUT: 504,
    ErrorCode.DOWNSTREAM_UNAVAILABLE: 502,
    ErrorCode.DOWNSTREAM_BAD_RESPONSE: 502,
    ErrorCode.FALLBACK_FAILED: 502,
    ErrorCode.INTERNAL_ERROR: 500,
}


class GatewayError(Exception):
    """网关对外错误基类：只携带可暴露的信息，绝不携带内部堆栈。"""

    code: ErrorCode = ErrorCode.INTERNAL_ERROR

    def __init__(self, message: str, *, retry_after: float | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.retry_after = retry_after

    def to_public(self) -> dict:
        payload = {"error": {"code": self.code.value, "message": self.message}}
        if self.retry_after is not None:
            payload["error"]["retry_after"] = round(self.retry_after, 3)
        return payload


class InvalidRequestError(GatewayError):
    code = ErrorCode.INVALID_REQUEST


class ConcurrencyLimitedError(GatewayError):
    code = ErrorCode.CONCURRENCY_LIMITED


class RateLimitedError(GatewayError):
    code = ErrorCode.RATE_LIMITED


class DownstreamTimeoutError(GatewayError):
    code = ErrorCode.DOWNSTREAM_TIMEOUT


class DownstreamUnavailableError(GatewayError):
    code = ErrorCode.DOWNSTREAM_UNAVAILABLE


class DownstreamBadResponseError(GatewayError):
    code = ErrorCode.DOWNSTREAM_BAD_RESPONSE


class FallbackFailedError(GatewayError):
    code = ErrorCode.FALLBACK_FAILED


class InternalError(GatewayError):
    code = ErrorCode.INTERNAL_ERROR

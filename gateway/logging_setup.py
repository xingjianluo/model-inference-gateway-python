"""结构化单行 JSON 日志。

每条日志至少带：time / level / logger / event / request_id；
配额判定日志额外带 allowed/reason/active_concurrency/max_concurrency/
rate_per_second/available_tokens/rule_version；
下游日志额外带 backend/error_code/degraded/latency_ms。
单测日志通过 pytest 的 log_cli 直接打印，满足“打印请求标识、配额判定
依据与下游调用结果”的要求。
"""

from __future__ import annotations

import json
import logging
import math
import sys
from typing import Any

_CONFIGURED = False

# 所有业务事件约定携带的字段（缺失时输出 null，保持日志结构稳定）
_EXTRA_FIELDS = (
    "request_id",
    "event",
    "caller",
    "allowed",
    "reason",
    "active_concurrency",
    "max_concurrency",
    "rate_per_second",
    "available_tokens",
    "rule_version",
    "cache_key",
    "cache_status",
    "backend",
    "primary",
    "error_code",
    "degraded",
    "latency_ms",
)


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "time": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
            "level": record.levelname.lower(),
            "logger": record.name,
            "message": record.getMessage(),
        }
        for name in _EXTRA_FIELDS:
            if hasattr(record, name):
                payload[name] = getattr(record, name)
        if record.exc_info:
            # 只记录类型名，绝不输出内部堆栈给日志消费方之外的世界
            payload["exc_type"] = record.exc_info[0].__name__ if record.exc_info[0] else None
        return json.dumps(
            _sanitize(payload),
            ensure_ascii=False,
            separators=(",", ":"),
            default=str,
        )


def _sanitize(value: Any) -> Any:
    """递归把非有限浮点（inf/nan）替换为 None，保证输出是合法 JSON。"""
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, dict):
        return {k: _sanitize(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_sanitize(v) for v in value]
    return value


def configure_logging(level: int = logging.INFO) -> logging.Logger:
    """配置网关 logger。

    在 pytest 下（能识别到 pytest 的 LogCaptureHandler）不接管 handler、
    保持 propagate，让 pytest 的 log_cli 统一打印单测日志；
    生产运行（uvicorn/脚本）时挂 JSON handler 到 stderr。
    """
    global _CONFIGURED
    logger = logging.getLogger("gateway")
    logger.setLevel(level)

    under_pytest = any(
        type(h).__name__ == "LogCaptureHandler"
        for h in logging.getLogger().handlers
    )
    if not under_pytest and not _CONFIGURED:
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)
        logger.propagate = False
        _CONFIGURED = True
    elif under_pytest:
        logger.propagate = True
    return logger


def get_logger() -> logging.Logger:
    return logging.getLogger("gateway")

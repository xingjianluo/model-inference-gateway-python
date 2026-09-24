"""日志配置：统一输出请求标识、配额判定依据与下游调用结果。

单测中通过 ``caplog`` 即可捕获（logger 默认向 root 传播）。
"""

from __future__ import annotations

import logging

_FORMAT = "%(asctime)s %(levelname)s [%(name)s] %(message)s"
_configured = False


def configure_logging(level: int = logging.INFO) -> None:
    global _configured
    if _configured:
        return
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(_FORMAT))
    root = logging.getLogger("gateway")
    root.setLevel(level)
    root.addHandler(handler)
    root.propagate = True
    _configured = True


def get_logger(name: str) -> logging.Logger:
    """返回 gateway 命名空间下的 logger。"""
    if not name.startswith("gateway"):
        name = f"gateway.{name}"
    return logging.getLogger(name)

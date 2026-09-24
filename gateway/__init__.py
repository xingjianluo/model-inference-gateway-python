"""本地推理网关。

职责边界：
- errors:     对外错误码与归一化异常
- config:     配额/缓存/降级配置与热更新存储
- quota:      调用方维度的并发与速率配额准入
- cache:      短窗口单飞合并与结果缓存
- downstream: 下游后端抽象、超时包装与内置桩后端
- service:    编排准入、合并、缓存、下游调用与降级
"""

__all__ = [
    "errors",
    "config",
    "quota",
    "cache",
    "downstream",
    "service",
]

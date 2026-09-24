# 模型推理网关（Python / FastAPI）

一套可在本地完整运行的模型推理网关，把推理请求的**准入、合并与失败处理**统一成稳定的对外语义：
并发压力、下游异常与配置热变更下行为均可预期，且**不依赖任何外部服务或真实凭据**。

## 能力概览

| 领域 | 语义 |
| --- | --- |
| 并发配额 | 按调用方（caller）精确计数；超限拒绝为 `concurrency_limited`（HTTP 429） |
| 速率配额 | 每调用方令牌桶；超限拒绝为 `rate_limited`（HTTP 429，带 `Retry-After`） |
| 热更新 | `PUT /admin/quota`、`PUT /admin/config` 即时生效，不重启进程；只换限额、不重置计数，不超卖 |
| 结果缓存 | 相同 model+inputs 命中 TTL 缓存；只缓存成功结果，超时/失败绝不写缓存 |
| 短窗口合并 | single-flight：合并窗口内相同输入只发起一次下游调用，follower 共享同一结果/异常，返回值语义一致 |
| 降级 | `fail`（明确失败）或 `fallback`（转备用路径）；按**受理时配置快照**执行，切换期已受理请求行为一致 |
| 错误归一化 | 超时/不可用/异常结果/降级失败各有可区分错误码；内部异常与堆栈不外泄 |
| 可观测 | 结构化事件 + 日志：请求标识、配额判定依据（in_flight/上限/tokens）、下游结果（target/ok/latency/failure） |

## 本地运行

```bash
uv sync
uv run uvicorn main:app --reload
# 或：uv run fastapi dev main.py
```

健康检查：

```bash
curl -s http://127.0.0.1:8000/healthz
```

发起推理（内置脚本后端，开箱即用）：

```bash
curl -s -X POST http://127.0.0.1:8000/v1/infer \
  -H 'Content-Type: application/json' \
  -H 'X-Caller: tenant-a' \
  -d '{"model":"gpt-demo","inputs":{"prompt":"hello"}}'
```

## 本地验证（测试）

```bash
uv run pytest -q
```

全部测试基于进程内的「脚本化下游」（`ScriptedBackend`：可编排延迟/超时/不可用/异常返回），
**无需网络、无需 API Key**。覆盖：

- `tests/test_quota.py`：并发拒绝、速率拒绝、热更新缩容/扩容、高压下不超卖、超时释放槽位
- `tests/test_cache.py`：命中/未命中可观测、相同输入只调一次、合并角色标记、失败不写缓存、TTL 过期
- `tests/test_cancel.py`：follower 取消不影响 leader、leader 取消传播并终止下游、超时终止下游且释放配额
- `tests/test_fallback.py`：fail/fallback 两策略、备路径也失败、异常结果分类、切换期一致性
- `tests/test_api.py`：HTTP 状态码/错误码映射、热更新端点、日志内容断言

查看日志（pytest 会打印 `request_id`、`quota_admit/reject` 判定依据、`downstream_result`）：

```bash
uv run pytest -s -o log_cli=true -o log_cli_level=INFO
```

查看结构化事件：

```bash
curl -s "http://127.0.0.1:8000/admin/events?request_id=<id>"
```

## 主要端点

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/v1/infer` | 推理（准入/缓存/合并/降级全部生效） |
| GET | `/healthz` | 存活检查 |
| GET | `/admin/config` | 当前配置快照 |
| PUT | `/admin/config` | 全量热更新配置（JSON 见 `config.example.json`） |
| PUT | `/admin/quota` | 调整单个调用方配额 `{caller,max_concurrency,rate_per_second}` |
| PUT | `/admin/fallback` | 切换 `{"mode":"fail"|"fallback"}` |
| GET | `/admin/events?request_id=` | 结构化事件 |

## 对外错误码

| code | HTTP | 含义 |
| --- | --- | --- |
| `invalid_request` | 400 | 请求体不合法 |
| `concurrency_limited` | 429 | 调用方并发已达上限 |
| `rate_limited` | 429 | 调用方速率超限（带 `retry_after`） |
| `downstream_timeout` | 504 | 主下游超时 |
| `downstream_unavailable` | 502 | 主下游不可用/连接失败 |
| `downstream_bad_response` | 502 | 下游返回结构不合法 |
| `fallback_failed` | 502 | 主路径与备用路径均失败 |
| `internal_error` | 500 | 兜底内部错误（不回显细节） |

详细语义见 [`docs/SEMANTICS.md`](docs/SEMANTICS.md)。

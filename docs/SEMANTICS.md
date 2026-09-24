# 网关语义说明

本文档详细说明配额与准入、合并与缓存、降级策略，以及如何在本地复现验证。

所有语义均有对应自动化测试（见 `tests/`），运行：

```bash
uv run pytest -q
```

## 1. 配额与准入

### 1.1 按调用方匹配

- 请求通过请求体 `caller` 或 HTTP 头 `X-Caller` 标识调用方；未提供按 `anonymous`。
- 配置中每个 caller 有独立的 `max_concurrency` 与 `rate_per_second`；未配置的 caller
  使用 `default_max_concurrency` / `default_rate_per_second`。

### 1.2 并发配额（在飞请求数）

- 每 caller 维护一个精确 `in_flight` 计数。
- `acquire` 在同一把 asyncio 锁内完成「读配置快照 → 比较 → 自增」，因此：
  - **不会超卖**：并发的准入请求串行通过临界区，超过 `max_concurrency` 立即拒绝；
  - **不会计数漂移**：槽位在服务层 `try/finally` 的 `release` 中递减并钳制到非负，
    成功、失败、客户端取消、下游超时各路径都保证恰好释放。
- 拒绝码：`concurrency_limited`（429）。

### 1.3 速率配额（令牌桶）

- 每 caller 一个令牌桶，容量与补充速率均为 `rate_per_second`（初始满桶）。
- 准入顺序：**先判并发，再判速率，通过后在同一临界区内扣令牌 + 占槽**。
- 令牌不足 1 个即拒绝为 `rate_limited`（429），响应体含 `retry_after`（秒），
  并回 `Retry-After` 头。`rate_per_second=0` 表示完全禁止。

### 1.4 配置热更新与并发安全

- 配置以**不可变快照**（frozen dataclass）保存，`ConfigStore.replace` 为单次引用原子替换。
- 热更新只改限额数值，**不触碰 `in_flight` 计数与令牌桶**：
  - 缩小并发：新请求按新限额拒绝，已在执行的请求自然排空，不被抢占；
  - 扩大并发/调整速率：下一个准入请求立即读到新值，无需重启。
- 管理接口：`PUT /admin/quota`（单 caller）、`PUT /admin/config`（全量）。

## 2. 合并与缓存

### 2.1 缓存键

`key = model + ":" + sha256(model, 规范化 JSON(inputs, sort_keys))[:16]`。
仅模型与输入参与；caller 不同但输入相同会命中同一缓存。

### 2.2 结果缓存（TTL）

- 仅在**主路径成功**后 `put_success`；下游超时、不可用、异常结果、取消均不写
  （测试：`test_failed_downstream_does_not_populate_cache`）。
- TTL 惰性失效（读取时比较单调时钟）；命中返回深拷贝，调用方无法污染缓存。
- 响应中 `source="cache"` 表示命中。

### 2.3 短窗口合并（single-flight）

流程（`gateway/cache.py::Coalescer`）：

1. 首个请求（leader）注册 in-flight 后进入 `coalesce_window` 合并窗口；
2. 窗口内到达的相同 key 请求成为 follower，挂入等待队列，**不发起下游调用**；
3. 窗口结束后 leader 执行唯一一次下游调用（含降级逻辑）；
4. follower 共享 leader 的结果（成功）或异常（失败），各自获得独立深拷贝。

关键性质：

- **合并请求与单独请求结果语义一致**：同一输出内容；
  follower 响应 `source="coalesced"`，leader 为 `downstream`/`fallback`。
- follower 仍**各自占用 caller 的并发槽**（它们是已受理请求），客户端断开只取消自己，
  不影响 leader（测试：`test_follower_cancel_does_not_affect_leader`）。
- leader 被取消时，共享下游任务被 `cancel()`，取消传播给 follower，
  下游协程随之终止，所有槽位在 finally 中释放
  （测试：`test_leader_cancel_propagates_and_cleans_up`）。
- 降级成功时 leader 响应信封带降级标记，follower 同样可见。

### 2.4 可观测性

每个请求产生事件：`quota_admit`/`quota_reject`、`cache_lookup(hit/expired)`、
`cache_put`、`coalesce(leader/follower)`、`downstream_result`、`fallback_decision`，
可通过 `GET /admin/events?request_id=` 查看；日志同步打印相同字段。

## 3. 降级与错误归一化

### 3.1 失败类别（内部归一化）

| 场景 | 归一化类别 | 对外 code / HTTP |
| --- | --- | --- |
| `asyncio.wait_for` 超时 | timeout | `downstream_timeout` / 504 |
| 连接错误/任意底层异常 | unavailable | `downstream_unavailable` / 502 |
| 返回非 dict 或 `output` 缺失/为 null | bad_response | `downstream_bad_response` / 502 |
| 开启 fallback 且备用也失败 | — | `fallback_failed` / 502 |

- 底层异常文本只进服务端日志，响应只含安全消息（测试断言不泄漏
  `"simulated connection failure"`、`"Traceback"`）。
- 未预期异常由全局 handler 兜底为 `internal_error`（500），不带堆栈。
- `asyncio.CancelledError` 不被包装、不被吞掉，保证取消正确传播。

### 3.2 策略与切换期一致性

- `fallback_mode = fail`：主路径失败即返回对应归一化错误。
- `fallback_mode = fallback` 且路由配置了 `fallback` target：
  主路径失败后以**独立超时** `fallback_timeout` 调用备用路径；
  成功响应 `source="fallback"`、`degraded=true`、`fallback_target=<target>`；
  备用也失败返回 `fallback_failed`。
- **快照隔离**：请求在准入时持有一份不可变 `GatewayConfig`，受理之后的开关切换
  不影响该请求（测试：`test_switch_during_inflight_keeps_admitted_semantics`），
  而切换后新受理的请求按新策略执行——切换期两类请求各自结果可解释。
- 降级结果不写入主缓存，避免把降级输出固化为正常结果。

## 4. 本地复现手册

```bash
uv sync

# 1) 跑全部无外部依赖的测试
uv run pytest -q

# 2) 带日志观察 request_id / 配额依据 / 下游结果
uv run pytest -s -o log_cli=true -o log_cli_level=INFO

# 3) 起服务手动体验（内置脚本后端）
uv run uvicorn main:app --reload
curl -s http://127.0.0.1:8000/healthz
curl -s -X POST http://127.0.0.1:8000/v1/infer \
  -H 'Content-Type: application/json' -H 'X-Caller: tenant-a' \
  -d '{"model":"gpt-demo","inputs":{"prompt":"hi"}}'

# 4) 热更新：把 tenant-a 并发改成 1（立即生效）
curl -s -X PUT http://127.0.0.1:8000/admin/quota \
  -H 'Content-Type: application/json' \
  -d '{"caller":"tenant-a","max_concurrency":1,"rate_per_second":5}'

# 5) 切换降级策略
curl -s -X PUT http://127.0.0.1:8000/admin/fallback \
  -H 'Content-Type: application/json' -d '{"mode":"fail"}'
```

## 5. 模块索引

| 文件 | 职责 |
| --- | --- |
| `gateway/quota.py` | 并发计数 + 令牌桶（单锁临界区） |
| `gateway/cache.py` | TTL 缓存 + single-flight 合并器 |
| `gateway/downstream.py` | 超时控制、失败归一化、脚本化后端 |
| `gateway/fallback.py` | 按配置快照执行 fail/fallback |
| `gateway/service.py` | 编排：准入→缓存/合并→下游→降级 |
| `gateway/config.py` | 不可变配置模型、JSON 解析 |
| `gateway/wiring.py` | 内存配置存储与默认装配 |
| `gateway/app.py` | HTTP 端点、热更新接口、全局错误处理 |
| `gateway/events.py` | 结构化事件记录（测试断言依据） |

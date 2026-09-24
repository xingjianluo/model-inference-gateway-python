# Model Inference Gateway（本地推理网关）

在推理调用方与下游模型后端之间，统一处理 **准入配额、相同输入合并/缓存、下游失败降级与错误归一化**，
使“高并发、下游异常、配置在线变更”三种情况下的对外行为稳定、可预期、可观测。

零外部依赖、零真实凭据：内置确定性桩后端（`echo` / `null` / `flaky`），本地即可复现全部行为与测试。

---

## 1. 快速开始

```bash
# 安装依赖（uv）
uv sync

# 启动服务
uv run uvicorn main:app --reload
# 也可用示例配置文件启动：
# GATEWAY_CONFIG=config.example.json uv run uvicorn main:app --reload

# 发起推理
curl -s -X POST localhost:8000/v1/infer \
  -H 'Content-Type: application/json' \
  -d '{"caller":"demo","model":"m1","inputs":{"prompt":"你好"},"request_id":"req-1"}'
```

运行测试（会通过 pytest live log 打印**请求标识、配额判定依据、下游调用结果**）：

```bash
uv run pytest                       # 全部 34 个用例
uv run pytest -k quota -s           # 只看配额相关
uv run pytest tests/test_service.py # 端到端语义
```

---

## 2. HTTP 接口

| 方法 & 路径 | 说明 |
| --- | --- |
| `POST /v1/infer` | 推理入口。请求体：`caller`、`model`、`inputs`（非空对象）、可选 `request_id`（缺省自动生成） |
| `GET  /health` | 健康检查 |
| `GET  /admin/config` | 查看当前生效配置（配额 / 缓存 / 超时 / 降级） |
| `PUT  /admin/config/quotas` | 热更新全部调用方配额，**不重启进程** |
| `PUT  /admin/config/fallback` | 热切换降级策略（`mode` + `fallback_backend` + `retry_on`） |
| `POST /admin/config/reload` | 从 `GATEWAY_CONFIG` 指向的文件重新加载 |
| `GET  /admin/stats` | 缓存命中/合并计数与各调用方并发计数 |

成功响应：

```json
{
  "request_id": "req-1",
  "output": { "...": "下游结果" },
  "cache_status": "miss",          // miss | hit | coalesced
  "backend": "primary",
  "degraded": false,
  "quota": {"caller": "demo", "reason": "ok", "rule_version": 0,
            "active_concurrency_at_admission": 1}
}
```

错误统一信封（只含可对外信息，**绝不包含内部异常文本或堆栈**）：

```json
{"error": {"code": "rate_limited", "message": "rate limit exceeded",
           "detail": {"caller": "demo"}}}
```

### 对外错误码（可区分）

| code | HTTP | 触发条件 |
| --- | --- | --- |
| `rate_limited` | 429 | 调用方速率令牌不足 |
| `concurrency_limited` | 429 | 调用方在途并发已达上限 |
| `invalid_request` | 400 | 缺少 `caller/model/request_id`，或 `inputs` 不是非空对象 |
| `downstream_timeout` | 504 | 下游在 `downstream_timeout_seconds` 内未返回 |
| `downstream_error` | 502 | 下游抛异常（内部类型/消息不外泄） |
| `downstream_bad_result` | 502 | 下游返回 `null` / 非对象 / 空对象 |
| `fallback_exhausted` | 502 | 主、备路径都失败（detail 给出两段错误码） |
| `internal_error` | 500 | 兜底未知错误（响应体不含细节，细节仅进服务端日志） |

---

## 3. 配额与准入语义

按 **调用方（`caller`）** 维度分别维护：

- **并发配额** `max_concurrency`：该调用方同时在途的请求数。
- **速率配额** `rate_per_second`：令牌桶，桶容量等于速率（允许短突发），按时间匀速补充；`null` 表示不限。
- 未显式配置的调用方使用 `default_quotas`。

准入规则（`QuotaManager.try_acquire`，**非阻塞**）：

1. 并发已满 → 直接拒绝 `concurrency_limited`，**不排队、不计数**；
2. 并发未满但令牌不足 → 拒绝 `rate_limited`，同样不占并发槽；
3. 两者都满足 → 占用 1 个并发槽、扣 1 个令牌，进入处理。

一致性保证：

- 配额的全部读写在**同一把锁、无 `await` 让出点**的临界区内完成；准入、释放、热更新因此严格串行，
  在“降配”和“请求准入”高频交错时也**不会超卖、计数不会漂移**（见 `test_concurrent_admit_never_oversells`、
  `test_admit_and_lower_limit_interleave_no_drift`）。
- **降配立即生效**：已在途请求不被中断，但槽位只减不增；新请求按新限额判定。
  **升配立即放行**。每次变更 `rule_version` 递增，响应与日志可追溯当时依据。
- 槽位在请求**成功、失败、超时、调用方取消**时必定归还且只归还一次；重复归还被忽略，计数永不为负。
- **被合并的等待者在挂起前释放自己的槽位**（实际下游工作由 leader 单槽承担），因此等待者被取消或
  超时都不会继续占用后续配额。

配额热更新：

```bash
curl -s -X PUT localhost:8000/admin/config/quotas \
  -H 'Content-Type: application/json' \
  -d '{"quotas":[{"caller":"demo","max_concurrency":1,"rate_per_second":1.0}]}'
```

整份配置在一把异步锁内原子替换，监听器同步完成限额调整，不存在“读到半份新配置”的窗口。

---

## 4. 合并与结果缓存规则

缓存键：`sha256(规范化 JSON(model, inputs))`（与 `caller`、`request_id` 无关；相同模型+输入即同键）。

每次请求三级判定，结果通过响应中的 `cache_status` 与服务端日志可观测：

- `miss`：无缓存且无在途相同请求 → 成为 **leader**，调用下游。
- `coalesced`：已有相同 key 的在途请求 → 挂接到同一个 future，**只产生一次下游调用**。
  合并者与单独请求拿到**完全一致的结果或错误**。
- `hit`：命中未过期缓存（TTL 内）直接返回，不再请求下游。

规则与保证：

- `cache_ttl_seconds`：成功结果的 TTL；到期惰性失效。**只有下游成功结果写缓存**。
- 下游**超时、抛错、返回非法结果时绝不写缓存**（`test_failure_is_never_cached`、
  `test_failure_does_not_poison_cache`、`test_timeout_releases_quota_and_caches_nothing`）。
- 合并等待者用 `asyncio.shield` 挂接：**等待者被取消不会取消 leader**，leader 仍可完成并回填缓存。
- leader 自己被取消时：leader 收到取消、槽位回收、在途条目清除；传播给其他等待者的是归一化的
  `downstream_error`（内部取消语义不外泄），等待者不会悬挂。
- `coalesce_window_seconds`（默认 `0` 关闭）：> 0 时，leader 失败后失败条目会在短窗口内继续合并
  瞬时重试，避免失败风暴反复冲击下游；这不是结果缓存，窗口过后自动允许新 leader。

---

## 5. 降级策略与错误归一化

配置：

```json
"fallback": {
  "mode": "fail",                 // fail：主路径失败即归一化失败
                                  // fallback：满足条件时转备用后端
  "fallback_backend": "backup",   // 备用后端名
  "retry_on": ["downstream_timeout", "downstream_error", "downstream_bad_result"]
}
```

- 主路径出现的异常先被归一化（超时/调用失败/非法结果三类，见上表）。
- 当 `mode=fallback`、配置了不同的备用后端、且错误码属于 `retry_on` 时，转调备用后端；
  成功则响应 `degraded=true` 且 `backend` 为备用后端名。
- 主备都失败 → 单一的 `fallback_exhausted`，detail 中给出主、备各自的错误码，便于解释。
- **策略快照在准入时确定**：请求一旦受理，处理途中即使降级开关被热切换，该请求仍按受理时的策略
  执行到底（`test_fallback_switch_during_in_flight_uses_admission_snapshot`），
  已受理请求的结果与错误因此一致、可解释；新请求立即使用新策略。

下游超时由 `downstream_timeout_seconds` 控制（用 `asyncio.wait_for` 包装），超时即取消下游调用。

### 内置本地后端（无需任何外部服务/凭据）

| kind | 行为 |
| --- | --- |
| `echo` | 回显 `model/inputs`，可选 `delay` 模拟耗时 |
| `null` | 返回 `null`，用于制造 `downstream_bad_result` |
| `flaky` | `behavior=error|bad_result|slow`，可控失败/非法结果/超时 |

---

## 6. 可观测性（日志）

- 生产：每条业务日志输出为单行 JSON（stderr），含 `request_id`、`event` 等结构化字段。
  - 配额事件 `quota_decision`：`allowed/reason/active_concurrency/max_concurrency/rate_per_second/available_tokens/rule_version`；
  - 缓存事件：`cache_hit / coalesced`，含 `cache_key/cache_status`；
  - 下游事件：`downstream_success`（`backend/degraded/latency_ms`）、`downstream_failure`（`backend/error_code`）、`fallback_success`。
- 测试：`pytest` live log 直接打印同样信息，例如：
  `quota decision | event=quota_decision request_id=r1 caller=c1 allowed=True reason=ok ... rule_version=0`

计数观测：`GET /admin/stats` 返回缓存 `hits/misses/coalesced` 与各调用方在途并发。

---

## 7. 代码结构

```
gateway/
  errors.py          错误码、对外错误类型与 HTTP 映射（归一化出口）
  config.py          配置模型、原子热更新存储 ConfigStore、JSON 解析
  quota.py           并发槽 + 令牌桶 QuotaManager（锁内无让出点）
  cache.py           单飞合并 + TTL 缓存 CoalescingCache（hit/coalesced/miss）
  downstream.py      Backend 协议、invoke 超时/归一化包装、echo/null/flaky 桩后端
  service.py         编排：校验→准入→合并/缓存→下游→降级→归一化响应
  logging_setup.py   JSON 结构化日志
main.py             FastAPI 入口、/v1/infer、/admin/* 配置与统计端点
tests/              quota / cache / service / app 四组共 34 个本地可复现用例
```

## 8. 本地验证清单

```bash
uv run pytest                 # 并发配额、缓存一致性、超时取消、降级路径全覆盖
curl ... /v1/infer            # miss 再请求一次变 hit
PUT /admin/config/quotas      # 在线降/升配额，立即观察 429 与恢复
PUT /admin/config/fallback    # fail / fallback 切换，观察错误码与 degraded
GET /admin/stats              # 核对 hits/coalesced 与在途并发计数
```

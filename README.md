# Model Inference Gateway（本地推理网关）

在推理调用方与下游模型后端之间，统一处理 **准入配额、相同输入合并/缓存、下游失败降级与错误归一化**，
使“高并发、下游异常、配置在线变更”三种情况下的对外行为稳定、可预期、可观测。

在此之上，为对正确性敏感的调用方与日常运营提供三块治理能力：

- **按次一致性声明**：请求可声明 `consistency="strong"`，绕过缓存/合并与备用路径，只要主路径新鲜结果；
- **可治理缓存**：缓存按调用方隔离、有容量上限并按 LRU 回收，占用与淘汰次数可查询；
- **来源与恢复语义**：响应明确给出结果来源（新算/复用）、路径（主/备）与产出时刻；降级结果不沉淀，
  下游恢复后同样的输入自动回到主路径。

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

运行测试（会通过 pytest live log 打印**请求标识、配额判定依据、一致性判定与结果来源**）：

```bash
uv run pytest                          # 全部 58 个用例
uv run pytest -k quota -s              # 只看配额相关
uv run pytest tests/test_service.py    # 端到端语义
uv run pytest tests/test_consistency.py tests/test_cache.py  # 一致性与缓存治理
```

---

## 2. HTTP 接口

| 方法 & 路径 | 说明 |
| --- | --- |
| `POST /v1/infer` | 推理入口。请求体：`caller`、`model`、`inputs`（非空对象）、可选 `request_id`（缺省自动生成）、可选 `consistency`（`default`/`strong`，缺省 `default`） |
| `GET  /health` | 健康检查 |
| `GET  /admin/config` | 查看当前生效配置（配额 / 缓存 / 超时 / 降级） |
| `PUT  /admin/config/quotas` | 热更新全部调用方配额，**不重启进程** |
| `PUT  /admin/config/fallback` | 热切换降级策略（`mode` + `fallback_backend` + `retry_on`） |
| `POST /admin/config/reload` | 从 `GATEWAY_CONFIG` 指向的文件重新加载 |
| `GET  /admin/stats` | 缓存命中/合并/淘汰计数与各调用方并发计数 |

成功响应：

```json
{
  "request_id": "req-1",
  "output": { "...": "下游结果" },
  "cache_status": "miss",          // miss | hit | coalesced | bypassed
  "backend": "primary",
  "degraded": false,
  "consistency": "default",        // 本次请求受理的一致性等级
  "source": "fresh",               // fresh | cache | coalesced：新算还是复用
  "path": "primary",               // primary | fallback：主路径还是备用路径
  "generated_at": 1791164000.123,  // 结果产出的墙钟时刻（epoch 秒）；缓存结果保留原产出时刻
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
| `consistency_unavailable` | 503 | 强一致请求此刻拿不到主路径新鲜结果（detail 给出底层错误码）；绝不以旧缓存或降级结果顶替 |
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

缓存键：`sha256(规范化 JSON(caller, model, inputs))` —— **缓存与合并都按调用方隔离**：
同一个模型+输入，不同 `caller` 是不同条目，一个调用方永远不会命中或合并到另一个调用方留下的结果。

每次请求三级判定，结果通过响应中的 `cache_status` 与服务端日志可观测：

- `miss`：无缓存且无在途相同请求 → 成为 **leader**，调用下游。
- `coalesced`：已有相同 key 的在途请求 → 挂接到同一个 future，**只产生一次下游调用**。
  合并者与单独请求拿到**完全一致的结果或错误**。
- `hit`：命中未过期缓存（TTL 内）直接返回，不再请求下游。
- （强一致请求见第 5 节，其 `cache_status` 为 `bypassed`。）

规则与保证：

- `cache_ttl_seconds`：成功结果的 TTL；到期惰性失效。**只有下游成功结果写缓存**。
- 下游**超时、抛错、返回非法结果时绝不写缓存**（`test_failure_is_never_cached`、
  `test_failure_does_not_poison_cache`、`test_timeout_releases_quota_and_caches_nothing`）。
- **降级（备用路径）结果不写入结果缓存**：可兑现给当时的合并者，但不沉淀——下游恢复后，
  同样的输入会重新走主路径（见第 6 节）。
- `cache_max_entries`（默认 `1024`）：结果缓存的条目上限。超过上限时按 **LRU（最久未使用）**
  顺序回收；命中会刷新条目的“最近使用”位。回收只作用于已完成的结果缓存，
  **绝不触碰在途合并条目**，因此淘汰不会搞乱进行中的合并、也不会让任何请求拿到错误结果
  （`test_eviction_never_touches_in_flight_entries`）。容量可通过配置热更新，缩容立即按 LRU 回收。
- 合并等待者用 `asyncio.shield` 挂接：**等待者被取消不会取消 leader**，leader 仍可完成并回填缓存。
- leader 自己被取消时：leader 收到取消、槽位回收、在途条目清除；传播给其他等待者的是归一化的
  `downstream_error`（内部取消语义不外泄），等待者不会悬挂。
- `coalesce_window_seconds`（默认 `0` 关闭）：> 0 时，leader 失败后失败条目会在短窗口内继续合并
  瞬时重试，避免失败风暴反复冲击下游；这不是结果缓存，窗口过后自动允许新 leader。

运营指标：`GET /admin/stats` 的 `cache` 段给出 `hits/misses/coalesced/cached_entries/
max_entries/evictions/inflight_keys`，可据此观察缓存占用与淘汰情况。

---

## 5. 按次一致性等级

请求体可选字段 `consistency`（缺省 `default`）：

| 取值 | 语义 |
| --- | --- |
| `default` | 标准路径：配额准入 → 缓存/合并 → 主路径 → （按策略）备用路径。 |
| `strong` | 强一致：**绕过缓存与合并、禁用备用路径**，必须拿到主路径的新鲜结果。 |

`strong` 请求的判定与行为：

- **不读缓存**（不会命中旧结果）、**不合并**到在途请求、也**不被**普通请求合并；
  结果**不写缓存**——两类请求同时打进来时互不污染。
- **不走备用路径**：主路径失败时不拿降级结果顶替，而是以可区分的
  `consistency_unavailable`（HTTP 503）明确失败，`detail` 中给出底层错误码
  （如 `downstream_timeout`/`downstream_error`）。
- 配额准入与普通请求一致（照样占并发槽、扣令牌）；强一致失败不留下任何缓存/在途痕迹。
- 判定过程可观测：响应 `cache_status="bypassed"`、`consistency="strong"`；
  日志有 `consistency_bypass` 事件，且 `quota_decision`/`downstream_success` 均带 `consistency` 字段。

非法取值（如 `eventually`）按 `invalid_request`（HTTP 400）拒绝。

---

## 6. 结果来源与恢复语义

每个成功响应都带来源说明，调用方无需猜测结果从哪来：

| 字段 | 取值 | 含义 |
| --- | --- | --- |
| `source` | `fresh` / `cache` / `coalesced` | 本次新算 / 复用缓存 / 复用在途请求的结果 |
| `path` | `primary` / `fallback` | 主路径产出 / 备用路径产出（`degraded=true` 时必为 `fallback`） |
| `generated_at` | epoch 秒 | 结果产出的墙钟时刻；缓存/合并复用时保留**原产出时刻** |

恢复语义：

- 某个输入因下游异常走过备用路径后，该降级结果**不会**被当成正常结果缓存起来；
  下游恢复后，同样的输入再进来会重新走主路径并反映主路径结果
  （`test_degraded_result_not_cached_and_primary_recovers`）。
- 降级策略被反复热切换时：已受理请求按**受理时快照**的策略执行到底，其结果与错误自洽、可解释；
  新请求立即使用新策略（`test_fallback_policy_flapping_keeps_results_self_consistent`、
  `test_fallback_switch_during_in_flight_uses_admission_snapshot`）。

---

## 7. 降级策略与错误归一化

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
- **降级结果不写入结果缓存**（见第 4、6 节）：备用路径的成功只兑现当次请求与其合并者。
- 声明 `consistency="strong"` 的请求**不走备用路径**（见第 5 节）。
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

## 8. 可观测性（日志）

- 生产：每条业务日志输出为单行 JSON（stderr），含 `request_id`、`event` 等结构化字段。
  - 配额事件 `quota_decision`：`allowed/reason/active_concurrency/max_concurrency/rate_per_second/available_tokens/rule_version/consistency`；
  - 缓存事件：`cache_hit / coalesced`，含 `cache_key/cache_status/consistency`；
  - 一致性事件：`consistency_bypass`（强一致请求绕过缓存与备用路径）；
  - 下游事件：`downstream_success`（`backend/degraded/latency_ms/generated_at/consistency`）、
    `downstream_failure`（`backend/error_code`）、`fallback_success`。
- 测试：`pytest` live log 直接打印同样信息，例如：
  `quota decision | event=quota_decision request_id=r1 caller=c1 allowed=True reason=ok ... rule_version=0 consistency=default`

计数观测：`GET /admin/stats` 返回缓存 `hits/misses/coalesced/cached_entries/max_entries/evictions`
与各调用方在途并发。

---

## 9. 代码结构

```
gateway/
  errors.py          错误码、对外错误类型与 HTTP 映射（归一化出口，含 consistency_unavailable）
  config.py          配置模型、原子热更新存储 ConfigStore、JSON 解析（含 cache_max_entries）
  quota.py           并发槽 + 令牌桶 QuotaManager（锁内无让出点）
  cache.py           单飞合并 + TTL 缓存 + LRU 容量回收 CoalescingCache（hit/coalesced/miss）
  downstream.py      Backend 协议、invoke 超时/归一化包装、echo/null/flaky 桩后端
  service.py         编排：校验→准入→（按一致性等级）合并/缓存→下游→降级→带来源的归一化响应
  logging_setup.py   JSON 结构化日志
main.py             FastAPI 入口、/v1/infer、/admin/* 配置与统计端点
tests/              quota / cache / service / consistency / app 五组共 58 个本地可复现用例
```

## 10. 本地验证清单

```bash
uv run pytest                 # 并发配额、缓存一致性/隔离/容量、超时取消、降级恢复、强一致全覆盖
curl ... /v1/infer            # miss 再请求一次变 hit；响应含 source/path/generated_at
curl ... /v1/infer -d '{"consistency":"strong",...}'  # cache_status=bypassed，不读不写缓存
PUT /admin/config/fallback    # fail / fallback 切换，观察错误码与 degraded；降级结果不入缓存
PUT /admin/config/quotas      # 在线降/升配额，立即观察 429 与恢复
GET /admin/stats              # 核对 hits/coalesced/evictions/cached_entries 与在途并发计数
GET /admin/config             # 查看 cache_max_entries 等当前生效配置
```

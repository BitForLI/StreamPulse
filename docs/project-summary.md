# StreamPulse 项目全量总结与简历事实库

> 用途：作为以后针对不同岗位改写简历、求职信、项目介绍和面试答案时的统一事实来源。
> 原则：只把有代码或实验记录支持的内容标为“已实现/已验证”；路线图与未完成项单独列出。
> 最后核对：2026-08-30，Windows 11 + Docker Desktop 本地环境。
> 核心实现提交：`f41efc5 feat: complete StreamPulse resilience and demo evidence`。

## 1. 一句话定位

StreamPulse 是一个可重复运行的实时 CDN 遥测数据应用：它接收合成的
delivery、routing 和 player 事件，通过 Kafka 与 Apache Flink 按事件时间
处理乱序、迟到、重复和坏数据，将明细与窗口指标写入 ClickHouse，并由 Go
服务基于规则、EWMA 和 rolling median/MAD 生成带证据、TTL 与安全约束的
shadow 路由权重建议，最后通过 Grafana 和可审计 API 展示结果。

它不是 DNS 在线请求路径的一部分，也不是只有 Docker Compose 和 Dashboard
的组件拼装项目。项目重点是 CDN 领域模型、流处理正确性、故障恢复、建议安全
边界和可复现实验。

## 2. 项目解决的问题

项目围绕以下工程问题展开：

1. 如何从连续 CDN 日志中识别某个 location、network/ISP 或 edge node 的尾延迟、
   5xx、缓存和容量异常。
2. 如何区分 CDN 服务端可观测到的 delivery quality proxy 与必须由播放器上报的
   startup/rebuffer 等真实 QoE。
3. Kafka 分区乱序、重复投递、迟到事件和无效事件存在时，分钟聚合如何保持可解释
   和可修正。
4. Flink TaskManager、ClickHouse 或下游服务发生故障时，数据如何恢复、积压如何
   消化，错误建议如何被拒绝。
5. 如何在 latency、error、cache miss/origin cost、capacity saturation 之间做多目标
   权衡，同时避免权重突变和建议振荡。
6. 如何让每一个简历数字都能追溯到 scenario、seed、manifest、原始结果和报告。

## 3. 当前完成状态

### 3.1 已实现并完成本地验证

- 四种 versioned v1 JSON Schema：`DeliveryEvent`、`RoutingEvent`、`PlayerEvent`、
  `RecommendationEvent`。
- Kafka topic、key、partition、retention 和 DLQ contract。
- Go deterministic event generator，支持 Kafka/JSONL sink、固定 seed/start time、
  ground-truth manifest、批量发送和故障注入。
- Java/Flink 主作业：parse/validate、脱敏 DLQ、event-time watermark、idle partition、
  event-ID dedup、allowed lateness、revision、too-late side output、三类窗口聚合。
- ClickHouse Kafka Engine、Materialized View、raw/aggregate/audit 表和 TTL。
- Grafana provisioned datasource 与 20 个业务/平台面板，真实 Dashboard PNG 已保留。
- Go Recommendation API 的分层实现、ClickHouse repository、Kafka publisher、
  acknowledgement/outcome audit store。
- fixed rule、EWMA、rolling median/MAD 检测以及 bounded multi-objective scorer。
- stale/future/sample/health/capacity/candidate/step/TTL/dwell/benefit guardrails。
- generator -> Kafka -> Flink -> ClickHouse -> recommendation -> Kafka -> ClickHouse
  的故障场景端到端验证。
- ClickHouse pause/catch-up、Flink TaskManager checkpoint recovery、exact replay、
  idle partition、allowed-late revision、too-late DLQ 等可靠性实验。
- 三个独立 seed 的 detector comparison。
- 本地与 GitHub-hosted CI、五分钟自动 demo 脚本、文档、实验报告和简历证据草稿。

### 3.2 已设计或保留 contract，但尚未实现

- EdgeRoute 读取 RecommendationEvent 的真实 shadow adapter。
- Vector 从真实 NGINX/EdgeRoute JSON 日志采集到 Kafka。
- PlayerEvent 与 DeliveryEvent 的 session-level join 和完整真实 QoE 聚合。
- Isolation Forest、LightGBM 或其他训练型 ML 模型。
- hot-content prefetch executor、crawler concurrency limiter 和 useful-prefetch 实验。
- Prometheus 全套指标、告警和生产级容量测试。
- 多 broker、多 TaskManager、多地区云部署、rescale/savepoint 升级流程。
- narrated five-minute screen recording。

### 3.3 明确没有做、不能在简历中声称

- 没有处理 TikTok 或任何公司的真实生产 CDN 日志。
- 没有处理真实 IP、Cookie、Token、账号、设备 ID 或版权播放日志。
- 没有实现生产全球 CDN，也没有运行真实跨区域网络。
- 没有证明端到端 exactly-once；当前诚实语义是 at-least-once 加应用层
  dedup/revision/ReplacingMergeTree。
- 没有证明真实用户 rebuffer 降低、真实美元成本节省或生产 SLA。
- 没有训练神经网络，也没有用 LLM 或“AI”直接控制路由。

## 4. 系统架构与数据流

```text
Synthetic Go Generator
  ├─ DeliveryEvent ──> cdn.delivery.v1 ──> Flink CDN Analytics Job
  │                                          ├─ parse/validate
  │                                          ├─ redacted DLQ
  │                                          ├─ event-ID dedup
  │                                          ├─ timestamp/watermark/idleness
  │                                          ├─ node 1m windows
  │                                          ├─ network 1m windows
  │                                          ├─ content 5m windows
  │                                          └─ too-late audit
  ├─ RoutingEvent ──> cdn.routing.v1                 │
  └─ PlayerEvent  ──> cdn.player.v1                  │
                                                      v
Kafka raw/aggregate/recommendation/DLQ topics
  │
  v
ClickHouse Kafka Engine -> Materialized Views
  ├─ raw_delivery / raw_routing / raw_player
  ├─ node_metrics_1m / network_metrics_1m / content_metrics_5m
  ├─ dead_letters
  ├─ recommendations + evidence
  └─ recommendation_acknowledgements / recommendation_outcomes
          │                                  │
          v                                  v
       Grafana                      Go Recommendation API
                                      ├─ repository/history
                                      ├─ fixed + EWMA/MAD detector
                                      ├─ multi-objective scorer
                                      ├─ guardrails
                                      └─ shadow RecommendationEvent -> Kafka
```

关键边界：

- Flink 当前只对 DeliveryEvent 执行业务窗口计算；RoutingEvent 和 PlayerEvent 由
  ClickHouse Kafka Engine 直接持久化，用于 Dashboard 和后续扩展。
- Recommendation API 只读取已完成的 node-minute 窗口，不读取未完成窗口。
- Recommendation API、Kafka、Flink、ClickHouse 都不在 DNS 请求 hot path。
- EdgeRoute 即使未来接入，也只能把建议作为可过期的 shadow 输入；在线健康检查、
  last-known-good 和 fallback 必须由 EdgeRoute 自己负责。

## 5. 上游复用与本人实现边界

### 5.1 复用的成熟组件

| 组件/来源 | 版本或 commit | 在项目中的用途 | 是否宣称自己实现 |
|---|---|---|---|
| Apache Flink Playgrounds | `6115f8e6d083b1b69f7c82b19d5723a90aed95a1` | Kafka/Flink Docker runtime scaffold 和原始 ClickCount 示例 | 否 |
| Apache Kafka | `3.9.0` | 原始、聚合、建议和 DLQ 消息总线 | 否 |
| Apache Flink | `1.16.0` | stateful event-time stream processing | 否 |
| ClickHouse | `26.7.3.19` | Kafka ingestion、明细/聚合/审计分析存储 | 否 |
| Grafana | `13.1.3` | provisioned Dashboard | 否 |
| Grafana ClickHouse datasource | `4.20.0` | read-only Dashboard queries | 否 |
| `segmentio/kafka-go` | `v0.4.49` | Go generator 与 recommendation publisher | 否 |
| Jackson | `2.15.4` | Java JSON parsing/encoding | 否 |
| JUnit 5 | `5.10.2` | Flink/Java tests | 否 |
| `yaml.v3` | `v3.0.1` | scenario YAML parsing | 否 |

Adobe Klickhaus 只作为 CDN 字段组织、ClickHouse 表和 Dashboard 思路参考，没有把
Adobe 业务代码或配置复制成自己的实现。

### 5.2 项目自己实现的部分

- CDN 事件字段、JSON Schema、兼容性、隐私和跨项目 contract。
- Kafka topic/key/partition/retention 决策与 topic bootstrap script。
- deterministic synthetic generator、五类故障、乱序/重复/坏数据注入、manifest、
  stable partition bucket 和批量 sink。
- Flink parse/validation、event-time、水位线、idle partition、dedup state、window、
  exact percentile、revision、late/DLQ 业务逻辑。
- ClickHouse DDL、Kafka Engine、MV、ReplacingMergeTree key/revision、TTL、只读
  Grafana 账号和 recommendation audit migration。
- 20 个 Grafana SQL panels 及 Dashboard provisioning。
- Go API 的 domain/use-case/repository/publisher/audit/http 分层。
- detector 组合、feature interpretation、多目标 scorer、recommendation identity、
  confidence、reason code、guardrail 和 failure semantics。
- 所有 scenario、自动化 PowerShell scripts、contract/unit/integration/fault tests、
  raw evidence 和诚实的实验报告。

这体现的是成熟工程中的“复用基础设施 + 实现有决策含量的业务与可靠性层”，不是
重新发明 Kafka、Flink、数据库或统计公式。

## 6. 技术栈与已验证版本

| 层 | 技术 | 使用方式 |
|---|---|---|
| 语言 | Java | Flink analytics job；编译目标 Java 11，CI 使用 Temurin 17 |
| 语言 | Go | event generator 和 Recommendation API；module target Go 1.23 |
| 语言 | Python | JSON Schema 和 detector evaluator tests；没有 Python ML 模型 |
| 事件总线 | Kafka 3.9.0 | 单 broker KRaft 本地环境，raw/aggregate/recommendation/DLQ topics |
| 流计算 | Flink 1.16.0 | DataStream API、Kafka connector、state、watermark、window、checkpoint |
| 分析存储 | ClickHouse 26.7.3.19 | Kafka Engine、MV、MergeTree/ReplacingMergeTree、TTL |
| 可视化 | Grafana 13.1.3 | provisioning、20-panel Dashboard、静态验证截图 |
| 服务/API | Go `net/http` | REST endpoints、timeouts、graceful shutdown、health/readiness |
| 本地编排 | Docker Compose | pinned upstream overlay 与 StreamPulse services |
| 构建测试 | Maven/JUnit、Go test/vet、Python unittest | 本地 gate 与 GitHub Actions workflow |
| 自动化 | PowerShell、Makefile、shell | migration、benchmark、E2E、recovery、demo |

本地验证环境包括 Windows 11、Docker Desktop 4.53.0、Linux engine 29.0.1。
上游原始 `bitnami/kafka:3.9.0` 已无法解析，因此项目保留失败证据，并通过显式 overlay
替换为 Apache 官方 `apache/kafka:3.9.0`，没有静默改写上游代码。

## 7. 数据契约与数据治理

### 7.1 四类事件

| Event | 粒度 | 主要字段/用途 |
|---|---|---|
| DeliveryEvent | 每个 CDN object request | request/session/content/node/location/network、cache status、HTTP status、bytes、TTFB、transfer、origin、segment duration、bitrate、synthetic capacity labels |
| RoutingEvent | 每次路由决策 | candidates、selected node、policy、quality snapshot age、weight、fallback level、reason codes |
| PlayerEvent | 每个模拟播放器生命周期/分片事件 | session/content、event type、sequence、bitrate、download、buffer、startup/rebuffer |
| RecommendationEvent | 每条算法建议 | scope、current/proposed weights、TTL、mode、evidence window、reason codes、expected deltas、confidence、model/config/query version |

### 7.2 时间语义

- `event_time`：业务事件真实发生时间，驱动 Flink 窗口。
- `ingest_time`：进入采集链时间，用于 collection delay。
- Kafka record timestamp：传输诊断字段，不替代 event time。
- processing time：计算节点处理时间，不用于业务窗口。
- 所有公开时间使用 UTC RFC 3339；ClickHouse 使用 UTC DateTime64(3)。

### 7.3 Schema evolution

- v1 producer 可以增加 optional 字段；consumer 忽略未知 optional 字段。
- required 字段不能在相同 major version 删除、重命名或改变单位/语义。
- 删除后的字段名不能复用表达新含义。
- enum 扩展前 consumer 必须存在明确 `UNKNOWN` 分支。
- breaking change 需要新 major schema，通常也需要新 Kafka topic。
- contract tests 覆盖 unknown optional field、required field、enum、numeric bounds、
  expiry 和隐私字段。

### 7.4 Routing/Recommendation 跨项目 contract

paired fixture 强制：

- recommendation 的 `current` 和 `proposed` node key 完全相同；
- 所有 recommended node 必须存在于 RoutingEvent candidates；
- proposed weights 之和为 1.0；
- 当前只允许 `mode=shadow` 和 `action=adjust_node_weights`。

这证明未来 adapter 能安全解析合同，不代表 adapter 已实现。

### 7.5 隐私与公开数据边界

禁止完整 IP、Cookie、Authorization、signed query、真实 User-Agent、用户账号、设备
ID、手机号、邮箱、私有 host 或版权视频地址。允许的 URL 相关字段只有 synthetic/hash
`content_id`、枚举 `object_type` 和无 query 的 `path_template`。公开 `network_id` 只能是
`as-synthetic-*`、匿名 hash 或 `UNKNOWN`；session 使用 `sha256:` 前缀的一向哈希。

DLQ 不保留原始 payload，只保留 SHA-256、有限 error code/message 与脱敏 preview。

## 8. Kafka 设计

| Topic | 分区 | Key | Retention | 用途 |
|---|---:|---|---:|---|
| `cdn.delivery.v1` | 6 | `location|network_id|stable_bucket` | 24h | CDN delivery events |
| `cdn.routing.v1` | 6 | `request_id` | 24h | routing decisions |
| `cdn.player.v1` | 6 | `session_id_hash` | 24h | ordered player events |
| `cdn.metrics.node.1m.v1` | 3 | `window_start|node_id` | 7d | node minute metrics |
| `cdn.metrics.network.1m.v1` | 3 | `window_start|location|network_id` | 7d | network minute metrics |
| `cdn.metrics.content.5m.v1` | 3 | `window_start|content_id` | 7d | content demand/cost metrics |
| `cdn.recommendations.v1` | 3 | `location|network_id` | 30d | shadow recommendations |
| `cdn.dead-letter.v1` | 3 | `source_topic` | 7d | redacted invalid/too-late records |

设计取舍：

- delivery 不按 `content_id` 分区，因为爆款会造成 hot partition。
- location/network 保留地域局部性，再用 deterministic session bucket 分散负载。
- player 按 session key 保证单 session 顺序。
- 本地 replication factor 是 1，因此不能声称 broker 高可用。
- topic script 同时兼容 PATH 中的 `kafka-topics.sh` 和 Apache 镜像的
  `/opt/kafka/bin/kafka-topics.sh`。

实测 22,800-request integration run 中，23,226 条 delivery records 分布到六个分区：
3,302 / 3,982 / 3,432 / 3,576 / 4,935 / 3,999，全部分区有数据，最大/中位比
为 1.306。该结果被保留为真实 hash 分布，而没有包装成完美均匀。

## 9. Deterministic Go Event Generator

### 9.1 生成内容

- 每个 base request 生成 RoutingEvent 和 DeliveryEvent；按配置比例生成 PlayerEvent。
- 使用显式 seed 和 simulation start time，保证属性序列和故障窗口可重复。
- 输出 JSONL envelope 或直接写 Kafka。
- manifest 保存 run ID、seed、时间范围、scenario、component versions、记录计数、
  label windows 和 SHA-256。

### 9.2 支持的故障与数据质量场景

1. node latency spike；
2. node 5xx spike；
3. 某 network/ISP 到单节点质量下降；
4. content popularity shift 引发 cache miss/origin amplification；
5. node capacity pressure；
6. bounded out-of-order events；
7. exact duplicate records；
8. controlled schema errors。

### 9.3 Kafka sink

- 有界 500-record buffer；
- 每批 synchronous `RequireAll` write；
- close 时 flush 最后一批不足 500 条的记录；
- broker/flush error 返回调用方，不吞错误；
- 避免每条事件一次 broker round trip。

### 9.4 生成器证据

固定 seed `20260827`、10 分钟逻辑时间、5,000 base requests/s 的离线 workload 连续
执行两次：

- 每次 3,000,000 base requests；
- 3,000,000 routing；
- 3,003,054 delivery；
- 750,000 player；
- 3,054 exact duplicates；
- 1,533 controlled schema errors；
- 64 delivery partition keys；
- 两次 manifest SHA-256 都是
  `ff5b6b60361ea1885215ac838bcbeb6eb56d25ebcd7e6faf306e7983bc9b4792`；
- key 最大/中位负载比 1.0081，最大/平均比 1.0084。

该结果只证明生成器确定性、编码能力和 key 分布，不是 Kafka/Flink sustained throughput。

另一个 300-request JSONL smoke 验证了 684 records：300 routing、309 delivery、75
player；9 个 exact duplicates 和 8 个 deliberately invalid records 均按标记被正确接受
或拒绝。

## 10. Flink 流处理实现

### 10.1 主处理阶段

```text
Kafka delivery source
  -> JSON parse and field validation
  -> invalid record redacted DLQ side output
  -> keyBy(event_id) + 25h TTL dedup state
  -> assign event timestamps
  -> 10s bounded-out-of-orderness watermark
  -> 30s idle partition detection
  -> branch by dimension/window
  -> at-least-once Kafka aggregate sinks
```

### 10.2 校验与 DLQ

- 缺少关键字段、非法 cache status、非法 HTTP status/数值进入 DLQ。
- 单条坏 JSON 不导致 job crash loop。
- `DeliveryEventParser` 生成 payload hash 和 redacted preview，不复制 raw payload。
- too-late event 通过 `LateDeliveryMapper` 生成 `TOO_LATE` audit record。

### 10.3 Dedup

- 以 `event_id` key 保存 ValueState；
- state TTL 为 25 小时；
- update on create/write；
- expired state 不返回；
- producer retry 必须复用 event ID。

### 10.4 Event time、watermark 与 lateness

- 最大允许乱序：10 秒；
- idle partition timeout：30 秒；
- allowed lateness：5 秒；
- watermark 内事件正常聚合；
- watermark 后但 allowed lateness 内的事件重算窗口并增加 `revision`；
- 超过 allowed lateness 的事件只进入 redacted audit，不修改聚合。

### 10.5 三类窗口

| Aggregate | Key | Window | 输出 |
|---|---|---|---|
| NodeMetric | location + network + node | 1m tumbling | request、5xx rate、hit ratio、bytes、origin total、TTFB P50/P95/P99、revision |
| NetworkMetric | location + network | 1m tumbling | network-level request/error/cache/bytes/origin/TTFB percentiles、revision |
| ContentMetric | content | 5m tumbling | request、cache hit、bytes、origin total、revision |

MVP percentile 在单窗口内保存样本并计算 nearest-rank exact percentile，便于与手算 fixture
核对。它适合本地正确性验证，但在声称大规模前必须换成可合并的 t-digest/HDR 等结构并
测量误差和状态大小。

### 10.6 Checkpoint 与恢复策略

- checkpoint state 写入持久卷；
- externalized checkpoint 使用 `RETAIN_ON_CANCELLATION`；
- 当前 failure-rate restart：五分钟最多 10 次 failure，重试间隔 5 秒；
- Flink job 恢复 dedup/window state 后继续消费 outage backlog；
- Kafka sinks 显式 `AT_LEAST_ONCE`。

第一次恢复实验使用 `fixedDelayRestart(3, 5s)`，因为唯一 TaskManager 离线时间超过
重试预算而失败，出现 `NoResourceAvailableException`，普通 checkpoint 也被清理。
项目没有删除这个失败，而是保存 exception/job payload，修复 restart/checkpoint policy
后重新实验。

## 11. ClickHouse 数据模型

### 11.1 Ingestion 分层

```text
8 Kafka Engine tables
  -> Materialized Views (JSON extraction/basic validation)
  -> raw/aggregate/recommendation/DLQ target tables
```

八个 consumer 分别对应三种 raw input、三种 aggregate、recommendation 和 DLQ topic。

### 11.2 Target table 设计

- `raw_delivery`、`raw_routing`、`raw_player`：ReplacingMergeTree，按 event identity
  在 `FINAL` 查询中进行应用层 replacement。
- `node_metrics_1m`、`network_metrics_1m`、`content_metrics_5m`：
  ReplacingMergeTree(revision)，按 deterministic window/dimension key 选择最新 revision。
- `recommendations`：30 天 TTL，保存 current/proposed、reason、expected、confidence、
  model version，并通过 migration 增加 input window、query version、config hash 和
  evidence JSON。
- `recommendation_acknowledgements` 和 `recommendation_outcomes`：独立 durable audit 表。
- `dead_letters`：只保存错误元数据、hash 和 redacted preview。
- raw/aggregate/DLQ 本地 TTL 为 7 天；recommendation TTL 为 30 天。

### 11.3 查询与权限

- Grafana 使用只读 `grafana_reader`；
- 权限限制为 `SELECT streampulse.*` 与 `system.kafka_consumers`；
- 三个固定 query baseline 分别覆盖 node quality、location/network anomaly 和
  content across nodes；
- benchmark 清理 ClickHouse 支持的 caches，记录 cold、20 次 hot、P50/P95、rows、bytes；
- 时间包含本地 HTTP client overhead，未强制清空 OS page cache。

## 12. Grafana Dashboard

20 个非 row panels 分为五组：

1. User impact：HLS request success、edge TTFB P95、segment transfer P95、
   rebuffer duration/risk proxy。
2. CDN efficiency：request hit ratio、byte hit ratio、origin GB/min、cost units per
   successful GB。
3. Network and nodes：location × network table、node saturation、fallback rate、
   top anomalous scopes。
4. Data platform health：ClickHouse Kafka consumers、aggregate freshness、DLQ rate、
   Kafka assignments/exceptions。
5. Shadow recommendations：active recommendations、reason/expiry、current vs proposed
   weights、expected deltas/model version。

Dashboard v2 修复了两个真实问题：

- watermark 实验的 future-dated window 会让 naive `now - max(window_end)` 为负；现在只
  选择 `window_end <= now()` 的最新完成窗口。
- ClickHouse Grafana plugin 会把 DateTime table column 当作 time series 并要求升序；
  audit tables 将时间明确格式化为 UTC string，保留 newest-first 审计排序。

所有 20 条 SQL 都直接在 ClickHouse 执行：20 passed、0 failed。最终 Dashboard 使用
官方 Grafana Image Renderer 从 live local stack 生成 `1800x1900` PNG，并人工检查；
最终渲染没有 Grafana query error。截图中 active recommendation 为 0 是 TTL 过期后的
正确状态，历史 reason/evidence/weights 仍保留在审计表。

## 13. Recommendation API

### 13.1 分层

```text
HTTP handler
  -> application service/use case
     -> parameterized ClickHouse repository
     -> detector
     -> scorer
     -> Kafka publisher
     -> ClickHouse audit store
```

评分逻辑没有写进 HTTP handler 或 SQL string。repository、detector、scorer、publisher 和
audit 都通过 interface 注入，便于 unit test 与错误模拟。

### 13.2 Endpoints

- `GET /healthz`：仅进程 liveness，不因 ClickHouse 短暂不可用杀死进程。
- `GET /readyz`：检查 ClickHouse readiness；依赖不可用返回 503。
- `GET /v1/scopes/{location}/{network}/metrics`。
- `GET /v1/scopes/{location}/{network}/recommendations/latest`。
- `POST /v1/recommendations/evaluate`。
- `POST /v1/recommendations/{id}/ack`。
- `POST /v1/recommendations/{id}/outcome`。

HTTP server 配置 5 秒 ReadHeader、10 秒 read/write、60 秒 idle timeout，并在 SIGTERM/
interrupt 后使用 10 秒 context graceful shutdown。

### 13.3 输入与错误语义

- scope location/network 经过 regex validation，只允许 synthetic/anonymous network。
- history query 或 publish failure 返回 dependency unavailable/503，不生成成功结果。
- 无 history：`INSUFFICIENT_HISTORY`。
- 正常窗口：`NO_ANOMALY`，不 publish。
- stale/future：`STALE_METRICS` / `FUTURE_METRICS`。
- 候选不足：`INSUFFICIENT_HEALTHY_CANDIDATES`。
- 收益不够：`BENEFIT_MARGIN_NOT_MET`。
- dwell 未结束：`MINIMUM_DWELL_ACTIVE`。
- Kafka publish 失败不会启动 dwell timer。

### 13.4 审计和身份

- recommendation ID 由 scope、input window end 和 config hash 通过 SHA-256 派生；
- recommendation 保存 schema/revision、created/valid time、scope、weights、evidence、
  query/model version、config hash、reason codes、confidence 和 expected deltas；
- acknowledgement/outcome 是独立持久表，API 重启后仍存在；
- expected delta 与 observed outcome 分字段，synthetic persistence check 不能冒充因果收益。

## 14. 异常检测实现

### 14.1 Fixed combined rule

对每个 node 的最新 completed window：

- requests 至少 100；
- 5xx rate 大于 2%；
- P95 TTFB 大于当前 location 候选节点 P95 中位数的 2 倍；
- 同时满足 latency 与 error 才触发 `RULE_LATENCY_ERROR_ANOMALY`。

它可解释但严格，因此在 latency-only 或 error-only 故障上会漏报。

### 14.2 Past-only EWMA/MAD

- 至少 5 个历史窗口；
- EWMA alpha 默认 0.20；
- 最新窗口不进入自身 baseline，避免 future leakage；
- frozen EWMA 会跳过 robust z 大于 4 的后续尖峰，减少 anomaly 被 baseline 吸收；
- robust z 使用 `0.6745 * (x - median) / MAD`，处理 MAD=0；
- latency anomaly：P95 > 1.5 × EWMA baseline 且 robust z >= 3；
- error anomaly：requests >= 100、error > 2%，且相对 EWMA error baseline 增量 > 1%；
- cache miss anomaly：hit ratio < 35%，并比历史中位数至少低 20 个百分点；
- reason codes 分别为 `EWMA_MAD_LATENCY_ANOMALY`、
  `EWMA_ERROR_RATE_ANOMALY`、`MAD_CACHE_MISS_ANOMALY`。

### 14.3 评估方法

- 使用 generator manifest 的 fault windows 作为 ground truth；
- 使用 completed-window time 计算预测和 detection delay；
- 三个独立 seed 分别从 Kafka -> Flink -> ClickHouse 生成 156 个 node-minute rows；
- 每轮保留 scenario/manifest/metrics SHA-256 与 raw per-window predictions；
- popularity shift 不作为 node failure ground truth；
- 不删除表现差的 combined rule 结果。

## 15. 多目标节点评分与安全约束

### 15.1 Candidate filtering

每个 node 只取最新 metric，过滤：

- window end 超过当前时间 5 秒以上的 future metric；
- 距当前时间超过 2 分钟的 stale metric；
- unhealthy node；
- requests 少于 100；
- saturation >= 0.85。

过滤后至少保留两个候选，否则不生成建议。

### 15.2 Normalization 和 penalty

对 eligible nodes 在当前 scope 内进行 min-max normalization：

```text
penalty = 0.35 * latency_factor
        + 0.25 * error_factor
        + 0.15 * cache_miss/QoE_proxy_factor
        + 0.15 * origin_cost_proxy_factor
        + 0.10 * saturation_factor
```

如果 detector 标记该 node anomalous，再增加 `0.25 * severity`。目标权重与
`1 / (0.05 + penalty)` 成正比并归一化。

这些权重是可配置工程选择，不是训练出的普适真理。

### 15.3 Progressive weight change

- 输入 current weights 只保留 eligible node 并重新归一化；若没有有效 current weights，
  使用均匀权重。
- 计算 target 后选择统一 interpolation lambda。
- lambda 保证每个 node 的绝对权重变化不超过 0.20。
- proposed weights 保持归一化，总和为 1。
- weighted penalty 改善少于 0.0001 时不生成建议。

### 15.4 TTL、dwell 和 confidence

- 所有建议固定 `mode=shadow`；
- TTL 默认 2 分钟；
- 同一 scope minimum dwell 为 1 分钟；
- history window 默认 15 个完成窗口；
- model version：`rule-ewma-mad-v1`；
- query version：`node-quality-v1`；
- confidence 由最大 anomaly severity 和总样本量组合，封顶 0.99，并保留三位小数；
- created/valid TTL invariant 在 publish 前再次检查。

## 16. 可靠性、降级与分布式系统能力

### 16.1 Kafka/Flink

- Kafka 吸收 producer/consumer rate mismatch 和下游短暂停顿。
- Flink 使用 checkpoint、restart policy、Kafka offsets 和 state restore。
- event-ID TTL state 处理重复；window revision 处理 allowed-late correction。
- idle partition 机制防止无数据分区永久阻塞 watermark。
- too-late/invalid event 进入可审计 side output，而不是 crash 或静默 drop。

### 16.2 ClickHouse

- ClickHouse 暂停时 aggregate topic 暂存数据；恢复后 Kafka Engine/MV catch up。
- Recommendation API readiness 失败并停止产生新建议。
- Dashboard freshness 明确显示数据是否过期。
- 现有表没有为了实验被 truncate/delete；实验通过独立时间范围和查询断言隔离。

### 16.3 Recommendation control safety

- 数据 stale、future、样本不足、候选不足、容量不足或无 material benefit 时返回 no-op
  reason，而不是强行给建议。
- ClickHouse/Kafka failure 返回 503；失败 publish 不更新 dwell state。
- TTL、minimum dwell、maximum step 和至少两个候选防止突变和振荡。
- StreamPulse 永不直接修改在线 DNS 权重。

### 16.4 诚实的规模边界

本地 runtime 是单 Kafka broker、一个 JobManager、一个 TaskManager/两个 slots 的验证
环境。它证明了 stateful recovery、backlog catch-up、partition/event-time correctness，
但不能等同于多机 broker quorum、生产高可用或全球调度容量。

## 17. 已完成实验与可追溯数字

### 17.1 上游 runtime baseline

- Apache Playground ClickCount job：4/4 vertices RUNNING；
- stop/restart 唯一 TaskManager 后，同一 Job ID 的 4 个 vertices 在 6 秒内恢复；
- input/output Kafka offsets 继续增长；
- 证明上游 scaffold 在声明的 Kafka image substitution 下可用，不替代 StreamPulse
  自身恢复实验。

### 17.2 Generator baseline

- 3,000,000 base requests 连续生成两次，manifest SHA-256 完全相同；
- 64 keys 的最大/中位负载比 1.0081；
- 证明 fixed-seed reproducibility 和 key distribution，不是 pipeline throughput。

### 17.3 Kafka/Flink integration

- 22,800 base requests，23,226 delivery，22,800 routing，5,700 player；
- 426 deliberate duplicates，219 deliberate schema errors；
- generator wall time 105.018 秒；该时间不是 throughput benchmark；
- delivery source 六分区 lag 全部归零；
- Flink 6/6 vertices RUNNING，19/19 observed checkpoints completed，0 failed；
- aggregate Kafka totals：node 92、network 32、content 324；
- integration DLQ delta 精确 219，与 manifest invalid count 一致。

### 17.4 ClickHouse/Grafana integration

同一 workload 在 `FINAL` 查询下：

| Table | Query-visible rows |
|---|---:|
| raw_delivery | 22,584 |
| raw_routing | 22,800 |
| raw_player | 5,700 |
| node_metrics_1m | 84 |
| network_metrics_1m | 28 |
| content_metrics_5m | 250 |
| dead_letters | 219 |

- 21/21 observed Flink checkpoints completed；
- 8 个 ClickHouse Kafka consumers 无 exception；
- Grafana datasource health OK，20/20 panel SQL 通过。

Delivery physical messages 与 query-visible rows 不能用简单减法解释，因为 duplicate 与
schema-error annotations 可能重叠；报告只声称 application-level replacement/rejection。

### 17.5 ClickHouse query baseline

| Query | Cold | Hot P50 | Hot P95 | Read rows | Read bytes |
|---|---:|---:|---:|---:|---:|
| Node P95/error/hit，最近 15m | 12.746 ms | 9.830 ms | 27.857 ms | 168 | 5,202 |
| Location/network anomaly，最近 24h | 13.032 ms | 8.881 ms | 12.170 ms | 56 | 1,832 |
| Popular content across nodes | 21.256 ms | 16.967 ms | 23.337 ms | 45,168 | 3,501,400 |

这些是本机 synthetic dataset 的查询时间，不是生产 SLA。

### 17.6 ClickHouse pause/catch-up

- ClickHouse consumer lag 在暂停时达到 6,134；
- intentional pause 26.472 秒；stop-to-healthy 29.579 秒；
- restart 后 20.373 秒 catch up 到 lag 0；
- isolated range 新增 delivery/routing/player/node/network = 2572/2582/645/12/4；
- 没有 truncate/delete 原有表。

### 17.7 Flink TaskManager checkpoint recovery + exact replay

- StreamPulse Job ID 保持 `344360c66b87dc6e5ceba0a5bdf84b71`；
- vertices：6 -> 0 -> 6；
- restored checkpoint 15，恢复后 checkpoint 16 完成；
- post-restore checkpoint state size 7,999,960 bytes；
- peak observed delivery-source lag 2,626，final lag 0；
- TaskManager start 到 6/6 RUNNING：12.779 秒；
- exact replay manifests SHA-256 相同；
- 选定完成分钟 raw unique/node sum/network sum 都是 1,198。

这证明一个本地 scoped minute 没有 observed missing/double count，不等于生产 exactly-once。

### 17.8 Idle partition + allowed lateness

- isolated Flink parallelism 2；delivery topic 两分区；
- partition 0 收到 1,201 records，partition 1 保持 0；
- 空分区没有永久阻塞 watermark；
- 初始窗口 600 requests/revision 0；
- allowed-late event 后 601/revision 1；
- too-late event 后仍为 601/revision 1；
- exactly one redacted `TOO_LATE` DLQ row；
- 首次结果 37.712 秒可见，其中包含 30 秒 idle timeout 与本地 polling；
- isolated job 取消，原 TaskManager count 恢复。

### 17.9 Detector comparison（三次独立 seed）

每次 156 node-minute rows、13 positive fault windows；aggregate：

| Strategy | Mean precision | Mean recall | Mean F1 | False alerts/hour | P95 detection delay | Mean missed faults |
|---|---:|---:|---:|---:|---:|---:|
| Fixed threshold | 1.0 | 1.0 | 1.0 | 0 | 30s | 0 |
| Combined rule | 0.666667 | 0.076923 | 0.136508 | 0 | 30s（仅检测到的两轮） | 3 |
| EWMA/MAD | 1.0 | 1.0 | 1.0 | 0 | 30s | 0 |

结论必须写成：在这个小型 fixed-seed synthetic dataset 上，fixed threshold 与 EWMA/MAD
打平；combined rule 因要求 latency+error 同时满足而漏掉 latency-only/error-only window。
不能声称 EWMA/MAD 普遍优于规则或已经达到生产泛化能力。

### 17.10 Recommendation fault E2E

- isolated Flink job 使用独立 consumer group 和 `latest` starting offsets；
- fault 让 `edge-syd-a` 同时出现高 latency 与 5xx；
- rule、EWMA error、EWMA/MAD latency 都触发；
- Kafka recommendation total offset 2 -> 3；
- schema v1 shadow recommendation：`rec-b4861107ffbf3c4cef004a0d`；
- TTL 120 秒，maximum absolute node weight step 0.20；
- `edge-syd-a` 0.3333 -> 0.1988，`edge-syd-b` 0.3333 -> 0.5333；
- 保留 3 个 eligible nodes；
- ClickHouse 保存三条 node evidence、input window、query version、config hash；
- acknowledgement 1、outcome 1；outcome 为 zero-delta persistence check，不是因果收益。

### 17.11 Day 7 demo

- 自动启动/复用 local stack、创建 topics、生成 fresh synthetic fault；
- 使用 isolated Flink job，完成 recommendation Kafka/ClickHouse/ack/outcome 检查；
- demo recommendation `rec-0fcb5f91b74cf89bc219566f`；offset 3 -> 4；
- isolated job 最后取消，upstream ClickCount 恢复；
- 主 StreamPulse job 保持 6/6 RUNNING；API health/ready 通过；
- 临时 demo evidence 写入 `.tmp/demo/`，不覆盖正式实验结果。

## 18. 测试与 CI

### 18.1 已本地通过

- Python JSON Schema/privacy/cross-event contract：8/8。
- Python detector evaluator：4/4。
- Java/Flink Maven tests：8/8，BUILD SUCCESS。
- Event generator：`go test ./...`、`go vet ./...`。
- Recommendation API：`go test ./...`、`go vet ./...`。
- PowerShell demo/recommendation/restart/watermark/migration scripts parser。
- Compose config、CI YAML、evidence JSON parsing、`git diff --check`。
- 真实 Docker runtime、Kafka/Flink/ClickHouse/Grafana/API health 和 E2E gates。

### 18.2 测试内容

- schema minimal/full/optional/invalid/enum/numeric/expiry/privacy；
- deterministic seed、scenario boundaries、duplicate/error injection、partition key spread；
- hand-calculated request/error/hit/bytes/origin/P50/P95/P99；
- watermark/idleness/allowed lateness/revision/too-late DLQ；
- stale/future/history/sample/capacity/candidate/benefit/TTL/dwell/publish failure；
- HTTP 400/503 和 health/readiness；
- parameterized ClickHouse query/decode；
- TaskManager recovery、exact replay、ClickHouse backlog catch-up；
- detector raw predictions、ground-truth manifest 和三 seed aggregate。

### 18.3 GitHub Actions workflow

Workflow 已定义 checkout、Python 3.12、Go 1.23.x、Temurin 17、Maven tests、Go tests/vet、
Python tests 和 Compose config。仓库已重建为 `apache/flink-playgrounds` 的真实 fork，
默认分支为 `main`。GitHub-hosted CI run `33289296683` 在提交 `8ae9362` 上通过；首两轮
失败也保留在 Actions 历史中，并分别暴露、修复了 pip cache dependency path 和遗漏的
`PyYAML` 测试依赖。

## 19. 主要工程决策与取舍

### 19.1 为什么使用 Flink

项目需要明确的 event time、watermark、idle source、allowed lateness、keyed state、
checkpoint restore 和 side output。只靠 ClickHouse MV 可以做聚合，但难以在同一位置表达
这些 stateful event-time 语义与故障恢复证据。

### 19.2 为什么当前使用一个主 Flink job

MVP 把 parse、watermark 和三类 aggregate 放在一个 job 内，减少跨 job contract、部署和
运维复杂度；内部仍按 stage/metric family 分离。成熟版只有在独立扩缩容和 failure domain
收益明确时再拆 job。

### 19.3 为什么不声称 exactly-once

Kafka、Flink at-least-once sink、ClickHouse Kafka Engine/MV 的组合没有被证明具有完整事务
边界。项目采用 event ID、state dedup、deterministic aggregate identity、revision 和
ReplacingMergeTree/FNAL query 获得可解释结果，并通过 exact replay 验证 scoped equality。

### 19.4 为什么 exact percentile 只是 MVP

exact sample percentile 让小型窗口能够与手算 fixture 精确比较，但 state 随 sample 增长。
生产规模应使用可合并近似结构，并报告误差、state size 与并行合并稳定性。

### 19.5 为什么建议只做 shadow

analytics 数据可能 stale、模型可能误判、ClickHouse/Kafka 可能故障。TTL、dwell 和 bounded
step 能降低风险，但不能替代在线本地健康与容量判断。shadow mode 可以先比较当前选择与
建议选择，不把离线数据平台变成 CDN availability dependency。

### 19.6 为什么 expected delta 不能写成实际收益

当前 expected P95/error/cost delta 来自同一 evidence window 的 weighted estimate；它没有
随机对照、真实应用或因果隔离。只有 future EdgeRoute controlled adapter/A-B experiment
产生的 observed delta 才能描述实际改善。

## 20. 对岗位能力的映射

| 岗位能力 | 本项目事实 |
|---|---|
| Linux/系统基础 | Docker Linux containers、进程 health/readiness、timeout、graceful shutdown、文件/持久卷和 CLI 排错 |
| 网络/CDN | node/location/network、HLS manifest/segment、cache hit/byte hit、origin、fallback、TTFB/transfer、capacity |
| 分布式系统 | Kafka partition/lag、Flink state/checkpoint/restart/watermark、ClickHouse catch-up、at-least-once/dedup/revision |
| 高可用与降级 | TaskManager recovery、ClickHouse pause、stale/future refusal、shadow TTL、dependency 503、failure evidence |
| 数据工程 | versioned schema、event time、window aggregation、MV/TTL、DLQ、query benchmark、data quality |
| 后端工程 | Go API layering、interfaces、repository/publisher/audit adapters、timeouts、validation、error semantics、tests |
| 算法/数据分析 | fixed/EWMA/MAD baseline、time-safe history、multi-seed evaluation、precision/recall/F1/delay |
| 资源与成本 | saturation、origin proxy、cache miss、cost units、多目标 penalty、capacity filtering |
| 工程沟通 | provenance、失败实验保留、claim boundary、reproducible reports、resume evidence mapping |

## 21. 针对不同简历方向的侧重点

### 21.1 分布式系统 / Infrastructure

优先强调：Kafka partition/lag、Flink event-time state、checkpoint restore、TaskManager
12.779 秒恢复、lag 2626 -> 0、ClickHouse backlog 6134 -> 0、at-least-once + dedup/revision。

推荐技术栈副标题：

```text
Apache Flink, Kafka, ClickHouse, Java, Go, Docker Compose
```

可用英文 bullets：

- Built a stateful event-time CDN telemetry pipeline with Kafka and Flink,
  handling bounded disorder, idle partitions, 25-hour event-ID deduplication,
  revisioned late updates, and redacted dead-letter routing.
- Recovered the six-vertex analytics job from checkpoint 15 after terminating
  its only TaskManager, returning `6 -> 0 -> 6` vertices to RUNNING in 12.779s
  and reducing observed Kafka lag from 2,626 to zero.
- Verified application-level replay correctness for a completed minute with
  1,198 unique raw, node-aggregate, and network-aggregate requests, while
  documenting the pipeline as at-least-once rather than overstating exactly-once.

### 21.2 Data Engineering / Streaming

优先强调：四种 schema、topic/key 设计、watermark/late/revision、三类窗口、Kafka Engine/MV、
22,800-request integration、20/20 queries、query benchmark。

可用英文 bullets：

- Designed versioned CDN delivery, routing, player, and recommendation
  contracts plus partition/retention policies, privacy enforcement, and a
  deterministic generator for out-of-order, duplicate, and invalid events.
- Processed a 22,800-request fixed-seed workload through Kafka, Flink, and
  ClickHouse, reaching zero source lag with 21/21 observed checkpoints and an
  exact 219-record DLQ delta for 219 injected contract errors.
- Provisioned eight ClickHouse Kafka Engine ingestion paths, revision-aware
  analytical tables, and a 20-panel Grafana dashboard whose SQL queries passed
  20/20 direct database checks.

### 21.3 CDN / Network / Video Infrastructure

优先强调：location/network/node、cache request/byte hit、origin、TTFB tail、HLS segment、
fallback、QoE proxy boundary、shadow routing suggestion。

可用英文 bullets：

- Modelled CDN delivery and routing telemetry by edge node, logical region,
  synthetic network/ISP, cache outcome, and HLS object type, separating
  server-side delivery risk proxies from player-reported QoE.
- Aggregated node/network one-minute and content five-minute metrics including
  P50/P95/P99 TTFB, 5xx rate, cache-hit ratio, origin time, bytes, fallback, and
  saturation for ClickHouse/Grafana analysis.
- Generated expiring shadow weight recommendations from latency, error, cache,
  origin-cost, and capacity evidence while keeping StreamPulse outside the DNS
  request path.

### 21.4 Go Backend / Platform Engineering

优先强调：API 分层、interfaces、parameterized repository、Kafka publisher、audit tables、
health/readiness、timeouts、graceful shutdown、503/no-op semantics、unit tests。

可用英文 bullets：

- Implemented a layered Go recommendation service with HTTP, application,
  ClickHouse repository, scoring, Kafka publisher, and durable audit adapters,
  exposing seven versioned health/metrics/evaluation/ack/outcome endpoints.
- Defined explicit failure semantics: dependency/query/publish errors return
  503, normal windows return `NO_ANOMALY`, failed publishes do not start dwell,
  and liveness remains independent from ClickHouse readiness.
- Persisted query/config versions, input windows, per-node evidence,
  acknowledgements, and outcomes so recommendations remain explainable across
  API restarts.

### 21.5 Data Analytics / Applied ML

优先强调：不是深度 ML；使用 fixed、combined rule、EWMA/MAD；past-only baseline；三 seed；
F1/recall/delay；保留差结果；模型只做 shadow evidence。

可用英文 bullets：

- Evaluated fixed-threshold, combined-rule, and past-only EWMA/MAD anomaly
  detectors across three independently seeded Kafka/Flink/ClickHouse runs,
  preserving manifests, hashes, features, and per-window predictions.
- Measured fixed and EWMA/MAD mean F1 of 1.0 with 30s completed-window P95
  detection delay on the synthetic dataset, while retaining the stricter
  combined rule's 0.136508 mean F1 and explaining its missed fault modes.
- Prevented time leakage by excluding the current window from EWMA/MAD history
  and rejecting future-dated metrics independently in both ClickHouse queries
  and the scorer.

### 21.6 Reliability / SRE

优先强调：失败注入、首次失败保留、restart policy 修复、lag/freshness、ClickHouse catch-up、
readiness、DLQ、自动 cleanup/finally。

可用英文 bullets：

- Built reproducible fault gates for TaskManager loss, ClickHouse outage,
  idle Kafka partitions, late events, duplicate replay, malformed payloads,
  and stale/future analytics data.
- Restored ClickHouse Kafka consumers from an observed lag of 6,134 to zero in
  20.373s after restart without truncating existing tables, then verified all
  isolated target-table counts.
- Retained a failed three-retry recovery attempt, diagnosed exhausted restart
  budget and cleaned checkpoints, then moved to bounded failure-rate restart
  and retained externalized checkpoints before rerunning the successful gate.

### 21.7 Graduate Software Engineer / General Backend

优先强调：多语言、可运行系统、API/data model/testing/CI、明确 ownership、不需要塞入所有数字。

建议选三点：一条完整 pipeline、一条可靠性、一条 detector/recommendation；不要同时堆满
所有组件名。

## 22. 可组合的简历事实库

以后写三条 bullet 时可以从下表每列各选一个，避免重复：

| 维度 | 可选事实 |
|---|---|
| 构建内容 | versioned CDN contracts；deterministic generator；Flink analytics；ClickHouse model；Go recommendation service；Grafana dashboard |
| 核心技术 | event time/watermark；state TTL dedup；revisioned lateness；Kafka Engine/MV；EWMA/MAD；multi-objective scorer |
| 可靠性 | 12.779s TaskManager recovery；lag 2626 -> 0；ClickHouse lag 6134 -> 0 in 20.373s；stale/future no-op；redacted DLQ |
| 数据规模 | 3,000,000 deterministic generator baseline；22,800-request real pipeline integration；23,226 delivery messages |
| 正确性 | 219 injected errors -> exactly 219 DLQ delta；600/rev0 -> 601/rev1；1,198 raw/node/network equality |
| 算法 | three-seed comparison；fixed/EWMA F1 1.0 synthetic tie；combined F1 0.136508 retained |
| 建议安全 | 120s TTL；60s dwell；0.20 max step；>=2 candidates；0.85 saturation limit；shadow-only |
| 查询/展示 | 20/20 panel SQL；three query baselines；1800x1900 live Dashboard screenshot |

一份简历通常只写 3 个 bullets，每个 bullet 应包含“做了什么 + 为什么/如何 + 一个可追溯
结果”。不要把上表所有数字塞进同一版本。

## 23. 面试讲解要点

### 23.1 “这是不是只做了模块拼接？”

回答重点：Kafka/Flink/ClickHouse/Grafana 是成熟组件；项目没有重造它们。原创工作在于
CDN contracts、partitioning、event-time state、window/revision、storage keys、detector/scorer、
guardrails、API semantics、fault scripts 和 evidence。只有 Compose 不能产生 600->601 revision、
exact DLQ count、checkpoint restore 和 recommendation audit 这些证据。

### 23.2 “watermark 为什么是 10 秒？”

回答重点：这是本地 synthetic disorder 的明确 MVP bound，不是通用生产参数。它和 30 秒
idle timeout、5 秒 allowed lateness 分开；测试覆盖一个 idle partition、within-lateness update
和 beyond-lateness audit。生产值必须从 collection delay distribution 和 SLO 调整。

### 23.3 “怎么证明迟到修正没有重复？”

回答重点：event-ID state 先去重；窗口 identity 加 revision；ClickHouse 使用
ReplacingMergeTree(revision)/FINAL 读取最新版本。实验观测 600/rev0 -> 601/rev1，too-late
后不变，并产生 exactly one audit record。

### 23.4 “为什么不是 exactly-once？”

回答重点：Flink sink 明确 AT_LEAST_ONCE，ClickHouse Kafka Engine/MV 没有和 Flink
checkpoint 形成统一事务。项目验证的是 application-level dedup/revision 和 scoped replay
equality，不把局部证据扩大成全链路 guarantee。

### 23.5 “CDN 日志能证明卡顿吗？”

不能。server delivery 可以计算 segment transfer、TTFB、success、delivery slack 和
`transfer_ms > segment_duration_ms` 风险代理；真实 startup、buffer、rebuffer、bitrate switch
必须由 PlayerEvent。当前没有 session-level player aggregate，因此简历不能写降低真实卡顿。

### 23.6 “机器学习在哪？”

当前实现的是可解释 statistical detection，不是训练型 ML：fixed、EWMA、median/MAD 和
multi-objective scoring。Isolation Forest 只在路线图。项目的算法价值来自 time-safe
evaluation、baseline comparison、reason codes 和安全集成，而不是把统计公式包装成 AI。

### 23.7 “如何避免建议来回振荡？”

current-to-target interpolation、每节点 0.20 maximum step、60 秒 minimum dwell、120 秒 TTL、
benefit margin、candidate/capacity filtering。当前没有 progressive recovery feedback loop，
所以仍只输出 shadow suggestion。

### 23.8 “ClickHouse 挂了会怎样？”

视频请求不受影响；Kafka aggregate topics 暂存数据；API readiness 失败并返回 503，不生成新
建议；Dashboard freshness 变 stale；恢复后 Kafka Engine/MV catch up。实测 lag 6134 在
restart 后 20.373 秒归零。

### 23.9 “哪部分来自上游？”

Apache Playground 提供原始 operations Compose、ClickCount example 和基础 runtime。
baseline tag 以前是上游；CDN schema、generator、analytics job、ClickHouse/Grafana、Go API、
experiments 和 docs 是本项目实现。Adobe Klickhaus 只是设计参考。

### 23.10 “如果扩大到生产规模，先改什么？”

多 broker/replication、多 TaskManager、checkpoint/object storage、approximate percentile、
backpressure/metrics、hot-key mitigation、schema registry、secure auth、real player telemetry、
real EdgeRoute shadow adapter、canary/A-B、capacity and cost calibration。

## 24. 可安全声称与禁止夸大

### 24.1 可以安全声称

- built/implemented a local reproducible CDN telemetry data application；
- handled out-of-order, duplicate, allowed-late, too-late and invalid events；
- implemented versioned contracts、Flink business logic、ClickHouse model、Go API、
  detector/scorer/guardrails；
- verified local recovery/catch-up with报告中的具体数字；
- evaluated fixed/EWMA/MAD on three fixed-seed synthetic runs；
- emitted TTL-bound, explainable, shadow-only routing weight recommendations；
- measured local synthetic query latency；
- reused Apache/Kafka/Flink/ClickHouse/Grafana rather than implementing them。

### 24.2 禁止或必须改写

| 不应写 | 正确写法 |
|---|---|
| processed billions of TikTok logs | processed a measured local synthetic workload，写真实 count |
| production-grade global CDN AI platform | reproducible local CDN telemetry and shadow-recommendation data application |
| end-to-end exactly-once | at-least-once with event-ID deduplication and revision-aware storage |
| reduced rebuffering by X% | improved/estimated delivery-risk proxy；除非以后有 player controlled experiment |
| saved $X | changed synthetic cost units/origin proxy；没有真实账单不写美元 |
| built Kafka/Flink/ClickHouse | integrated/reused Kafka/Flink/ClickHouse and implemented domain logic |
| ML model outperformed rules | fixed and EWMA/MAD tied on current synthetic dataset |
| high availability | recovered in a scoped local single-TaskManager experiment |
| EdgeRoute integration completed | shared versioned contract only；adapter remains pending |

## 25. 当前限制与下一步

按优先级：

1. 录制 narrated five-minute demo。
2. EdgeRoute 实现 shadow adapter：验证 schema/TTL/candidates，再比较 real vs shadow choice，
   仍不应用权重。
3. 接入 Vector + NGINX/EdgeRoute synthetic JSON logs，比较 schema coverage。
4. 实现 PlayerEvent session join 和真实 player QoE aggregate。
5. hot-content prefetch recommendation 和 useful-prefetch/waste/origin amplification 实验。
6. 只有在 baseline、时间切分、特征/模型 hash 和 held-out evaluation 完整后才增加
   Isolation Forest。
7. 多 broker/TaskManager、backpressure、rescale/savepoint 和持续负载 capacity matrix。
8. 后续功能形成新的稳定里程碑且用户明确要求时，再发布下一版本。

## 26. 关键证据文件索引

| 主题 | 文件 |
|---|---|
| 项目入口和边界 | `README.md` |
| 上游来源 | `THIRD_PARTY.md`、`docs/upstream-baseline.md` |
| 架构和 ownership | `docs/architecture.md` |
| event-time 语义 | `docs/event-semantics.md` |
| QoE 边界 | `docs/qoe-metrics.md` |
| topics/data/privacy/compatibility | `contracts/*.md` |
| schemas/fixtures/tests | `schemas/`、`contracts/fixtures/`、`tests/schema/` |
| generator | `services/event-generator/` |
| Flink analytics | `jobs/cdn-analytics/` |
| ClickHouse/Grafana | `infra/clickhouse/`、`infra/grafana/` |
| Recommendation API | `services/recommendation-api/` |
| generator evidence | `experiments/reports/generator-baseline/` |
| Kafka/Flink evidence | `experiments/reports/flink-integration/` |
| ClickHouse/Grafana/query evidence | `experiments/reports/clickhouse-grafana/` |
| recommendation E2E | `experiments/reports/recommendation-api/` |
| detector comparison | `experiments/results/detector-comparison/` |
| ClickHouse catch-up | `experiments/results/clickhouse-catchup/` |
| Flink recovery/replay | `experiments/results/flink-restart/` |
| watermark/lateness | `experiments/results/watermark-lateness/` |
| Dashboard image | `docs/streampulse-dashboard.png` |
| demo | `scripts/demo.ps1`、`docs/demo.md` |
| verification ledger | `docs/verification.md` |
| short resume drafts | `docs/resume-bullets.md` |

## 27. 运行与复核命令

### 27.1 快速本地测试

```powershell
python -m unittest discover -s tests/schema -v
python -m unittest discover -s tests/experiments -v
go -C services/event-generator test ./...
go -C services/event-generator vet ./...
go -C services/recommendation-api test ./...
go -C services/recommendation-api vet ./...
mvn -f jobs/cdn-analytics/pom.xml test
docker compose -f compose.yaml config --quiet
```

### 27.2 五分钟 synthetic demo

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/demo.ps1
```

### 27.3 关键实验

```powershell
make recommendation-e2e
make detector-test
make detector-e2e
make watermark-lateness-test
make clickhouse-benchmark
```

部分 fault test 会启动/停止指定的本地 project container 或 isolated Flink job，应先阅读脚本
和对应报告。脚本在 `finally` 中恢复 TaskManager/job 状态，但生产环境不能直接照搬。

## 28. 以后根据本总结写简历时的规则

1. 先选岗位方向，再从第 21、22 节挑事实；不要每份简历都写同样三点。
2. 一条 bullet 只讲一个主能力，最多带 2–3 个相关技术名。
3. 任何数字都必须能指向第 17、26 节的报告；没有报告的数字不写。
4. 明确使用 local、synthetic、shadow、observed 等限定词。
5. expected/model estimate 与 observed result 永远分开。
6. upstream/reused component 与自己实现永远分开。
7. 新功能只有在代码、测试和 E2E 证据都完成后，才能从“路线图”移到“已实现”。
8. GitHub CI、release、EdgeRoute adapter、player QoE、Isolation Forest 等状态变化时，
   必须先更新本文件，再据此更新简历。

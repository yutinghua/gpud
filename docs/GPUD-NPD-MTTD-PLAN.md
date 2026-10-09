# 基于 gpud + NPD 的 GPU 故障 MTTD 检测方案

## 1. MTTD 是什么

MTTD（Mean Time To Detect，平均故障检测时间）：从 GPU 故障（或亚健康状态）发生，
到监控系统感知并完成标记的时间间隔。链路公式：

```
MTTD = 内核报错产生 + Agent 捕获 + 上报/事件化 + 控制面标记
```

它与 MTTR（修复时长）、节点自愈率（自动修复占比，成熟集群 85%–95%+）共同构成
自愈闭环的三大指标。

为什么 GPU 场景对 MTTD 极端敏感：

- 万卡训练中单卡故障会让全部 Worker 卡死在下一个 AllReduce 同步点，
  MTTD 每多一秒都是无效算力消耗。
- 传统 K8s 链路（kubelet 10s 心跳 + Controller Manager 判定）默认 40s~数分钟；
  **MTTD ≤ 30s 即行业第一梯队水平**。

评估 30s 含金量的两个试金石：

- **显性故障 vs 亚健康**：XID/掉卡/NVLink 断连有明确 error code，能做到秒级；
  降频、静默丢包类"慢节点"只能靠性能指标周期检测，分钟级是正常的。
  MTTD 目标主要约束前者。
- **Push vs Pull**：依赖 Prometheus/vmagent 周期 pull + 告警 `for:` 评估的链路，
  天然 45–60s+；能进 30s 的方案，节点侧捕获都是事件驱动的。

## 2. 四个阶段与测量策略

以最典型的 XID/SXID 错误为例，全链路拆为四个时间戳（T0→T3）：

| 阶段 | 动作 | 时间戳来源 | gpud 现状 |
|---|---|---|---|
| 1. 硬件报错 T0 | NVIDIA 驱动写 XID/SXID 行到 `/dev/kmsg` | 内核 µs 时间戳 | 原生解析并换算为绝对时间，随事件入库、经 `/v1/events` 暴露 |
| 2. Agent 捕获 T1 | gpud 流式读取 → 正则匹配 → 去重 → SQLite | gpud 侧时钟 | follow 模式阻塞读，内核写入瞬间返回，毫秒级；未单独落盘（设计上 <1s，暂不需要测） |
| 3. 上报/事件化 T2 | 到达监控系统或 K8s APIServer | scrape 时刻 / Event 创建时间 | 取决于链路：pull 等 vmagent 下一轮 scrape；push 为桥写入时刻（Event 时间戳免费） |
| 4. 控制面标记 T3 | Operator 完成告警或打 Taint | patch 完成时刻 | gpud 不做，需 Operator（NPD 也不打 taint） |

**MTTD = T3 − T0。**

### 测量策略：先总量，后拆阶段

- **先只测总量**。vmagent 10–15s scrape + 即时告警规则（`increase(...) > 0`
  是边沿触发，**不加 `for:`**）→ 总 MTTD ≈ 15–25s，可进 30s 线。
- **30s 达标则零二次开发**：只交付一个总量直方图的 P50/P95/P99 即可。
- 理由：阶段 1 是物理事件，阶段 2 设计上 <1s，瓶颈几乎必然在阶段 3/4。
  总量不超标时拆阶段没有信息增益，且低频事件的分位数本身就是噪声
  （6 小时 5 次 XID，`histogram_quantile` 算出来的 P99 不可信）。
- **升级条件**：P99 持续破 30s（或要冲 3–5s）时，先加 push 桥把 T2 压到秒级；
  仍不够再考虑在 gpud 埋 T1（唯一需要动 gpud 源码的点，几十行）。

## 3. gpud vs DCGM：为什么 DCGM 不能当 MTTD 引擎

| 维度 | DCGM / dcgm-exporter | gpud |
|---|---|---|
| 数据源头 | 轮询 NVML / GPU 寄存器 | 内核日志流（`/dev/kmsg`）+ NVML |
| 触发机制 | 内部定时采样 | 内核写日志瞬间流式读到，毫秒级 |
| 对外出口 | 仅 Prometheus pull | `/metrics` pull + `/v1/events`、`/v1/states` |
| XID 79 掉卡时 | NVML 调用挂起/超时，只知道"指标没了"，不知道哪张卡、几点掉的 | 不依赖 GPU 存活，从 kmsg 行提取 UUID/PCI + 精确 T0 |
| SXID/NVSwitch | 单卡视角，级联故障需二次计算 | 行级解析 SXID，switch 维度（per-code 指标见 `SXID-METRICS-DESIGN.md`） |
| T0 保真 | 无，只有抓取时刻 | 原生透传内核时间戳 |
| 系统上下文 | 仅 GPU | GPU + PCIe + 磁盘 IO + D-State 等全栈 |
| 可扩展性 | 闭源库，受 NVIDIA 版本约束 | 开源 Go，可按需扩展 |

DCGM 不能承担 MTTD 检测引擎的四个硬伤：

1. **无原生对外 push**：`dcgmRegisterForEvents` 回调本质仍是内部轮询，且无 webhook 出口。
2. **恰好在最需要时失效**：GPU 彻底掉卡（最需要秒级检测的场景）时 NVML/DCGM 调用阻塞，
   exporter scrape 超时，得到的是"数据消失"而非结构化故障事件。
3. **Pull-only 出口**：MTTD 下限 = scrape 间隔 + 告警评估周期，与优化目标直接冲突。
4. **T0 丢失**：即使配置事件回调，也没有内核报错时刻，四阶段计时无从谈起。

**但 DCGM 需要保留**：温度/功耗/利用率是渐变量，15s 采样足够，适合做监控大屏；
`dcgmi diag -r 3` 用于 MTTR 阶段的恢复校验。双轨并行：**gpud 管救火（低 MTTD），
DCGM 管体检（趋势与诊断）**。

## 4. 总体方案（gpud + NPD）

```
GPU 节点（gpud DaemonSet：/dev/kmsg 流式捕获，T0 毫秒级入手）
   │
   ├─ Pull 管道（Phase 0，现有架构）─────────────────────────────
   │    /metrics ──10~15s──► vmagent ──► VictoriaMetrics
   │                                        └─ vmalert（即时规则）──► Alertmanager ──► webhook
   │
   └─ Push 管道（Phase 1，按需）────────────────────────────────
        /v1/events ◄──(1~2s 轮询 localhost)── NPD custom plugin / 轻量 bridge
                            │  → Node Condition（GPUUnhealthy=True，annotations 带 T0）
                            ▼
                     Draino / medik8s / 自研 Operator
                            │  → Taint（按 code 策略表）+ Drain
```

### Phase 0 — 零二次开发，交付总量指标

1. gpud DaemonSet（现有）+ vmagent 10–15s scrape `/metrics`；
2. xid 计数器已有（`accelerator_nvidia_xid_errors_total`）；
   sxid per-code 计数器按 `SXID-METRICS-DESIGN.md` 落地（纯加法，不影响现有 sxid 监控）；
3. vmalert 即时规则（不加 `for:`）：`increase(accelerator_nvidia_xid_errors_total[2m]) > 0`
   → Alertmanager → 自愈入口；
4. MTTD 口径：每次事件（真实 + 演练）记一个总时长样本
   （T0 = 事件内核时间戳 / 注入时刻，T3 = 告警触发时刻），落成 `mttd_total_seconds`，
   周报 P50/P95/P99；
5. 判定：P99 ≤ 30s → 方案交付，就此打住。

### Phase 1 — 若 P99 破线，加 push 桥（~100–300 行，不动 gpud 源码）

- NPD custom plugin 每 1–2s 增量拉取 `127.0.0.1:15132/v1/events?startTime=...`
  → 写 Node Condition；
- Draino / medik8s / 自研 Operator watch condition → 打 Taint + Drain
  （K8s Event 创建时间 = T2 免费，taint patch 时刻 = T3）；
- 账本：T0→T1 <1s，T1→T2 ≈ 2s，T2→T3 <1s，**总 MTTD 3–5s**。

### Phase 2 — 可选，拆阶段瓶颈

仅当仍需定位瓶颈时：在 gpud 事件入库处补记 T1（唯一动 gpud 源码的点），
配合 T2/T3 出分阶段直方图 `mttd_stage_duration_seconds`，
在 Grafana 上对比三条曲线即可看出是谁拉高了 P99。

## 5. 补充与校正（基于 gpud 源码核对）

1. **gpud 无原生 webhook / K8s Event 推送**。文档常见的 `event_triggers` YAML 配置、
   "Webhook 直连"在源码中不存在；gpud 唯一的原生 push 是登录其云端控制面的上报，
   私有化 vmagent 场景用不上。**桥必须自建，但可以完全建在 gpud 外面**
   （T0 已在 `/v1/events` 中），gpud 零改动。
2. **只用 vmagent pull 时，gpud 同样吃 pull 瓶颈**。gpud 的真实优势是 T0→T1 这一跳
   （毫秒级捕获 + T0 精确透传 + 掉卡时内核级认诊），不是"换上就全链路秒级"；
   MTTD 的优化战场在 T1→T3。
3. **MTTD 要做成可持续测量的 SLI**：真实 GPU 故障低频，靠等故障攒样本 P99 永远是噪声。
   用现成的 `gpud inject-fault`（写 `/dev/kmsg`）做定期合成演练（如每日一次），
   演练样本与真实样本分开打标，MTTD 才能进 SLO 体系持续考核。
4. **启动盲区要有兜底告警**：gpud 的 HTTP 监听在 login/machine-info 之后才打开
   （约 2 分钟预算）。kmsg 启动回放 + 3 天去重保证此期间的故障不丢，
   但窗口内 `/metrics` 拉不到——`up == 0` 必须有独立告警，
   否则"检测器挂了"本身就是最大的 MTTD 黑洞。
5. **MTTD 与自愈规则严格解耦**：检测层只报事实 + 时间戳（"T0 时刻 GPU-0 报 XID 79"）；
   XID/SXID → Ignore / NoSchedule / NoExecute 的策略表放 Operator 的 ConfigMap，
   改策略不需要重启节点上的 gpud。检测层的唯一 KPI 是延迟。
6. **时钟前提**：跨节点计算 T3−T0 要求 NTP/chrony 毫秒级对齐
   （GPU 集群跑 NCCL 本来就要求严格时钟）；单节点内 T0/T1 同钟，无漂移问题。

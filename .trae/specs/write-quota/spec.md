# Pebble 按用户键范围写入配额（Write Quota）- 产品需求文档

## Overview

- **Summary**：为 Pebble 增加一套**默认关闭**的写入准入控制能力。调用方可以配置若干条按用户键（UserKey）区间定义的配额规则，对落入区间的写入从四个维度进行占用量限制：待提交字节数、待提交操作数量、未 Flush 内存占用、并发 Batch 数量；不足时按规则策略立即失败、FIFO 等待、按请求优先级排队或对跨范围批次整批拒绝。所有用户写入（Batch、Merge、RangeDelete、RangeKey 写入、LogData、SSTable 摄取）在进入实际提交（seqnum 分配、WAL 写入/sync、memtable 内存占用）之前必须先完成配额预留，提交结束或任一步骤失败时完整、且仅释放一次预留。
- **Purpose**：在多业务共享键空间（典型为多租户/多表前缀共用一个 Pebble 实例）的场景下，让单个热键区的大批量写入不能耗尽全局的 WAL 同步槽、memtable 内存与提交并发，为其它键区提供可配置的写入隔离与背压/拒绝手段，且不牺牲引擎默认形态下的性能与正确性。
- **Target Users**：把 Pebble 作为嵌入式存储引擎、需要在应用层对不同键区间做写入资源治理的高级调用方（如 CockroachDB 一类的上层系统）。

## Goals

- 默认关闭；未配置时现有写入顺序、WAL、WriteStall、性能行为保持不变。
- 多条互不重叠的键范围规则；也允许配置重叠规则，此时以显式数值优先级**独占匹配**（同键只命中优先级最高的一条，同优先级重叠配置非法）。
- 每条规则可独立配置四个维度的占用上限（0 表示该维度不限），并独立选择不足时的策略。
- 覆盖普通提交管线全部用户写入口与 SSTable 摄取入口；预留先于任何资源消耗，失败全量回滚，杜绝部分消耗。
- 并发安全：高并发下无重复扣减、无负数计数、无死锁、跨范围无饥饿；支持 Context 取消与 DB 关闭唤醒。
- 嵌套 Batch（Batch.Apply 摊平）与跨范围 Batch 有明确、可测试的归属与回滚语义。
- 配置可运行时切换（闭包选项，沿用 SpanPolicyFunc 先例）；切换瞬间已准入的写入绑定旧规则快照直至释放，读路径与后台任务看不到中间状态。
- 数据库重开后，未 Flush 维度的占用统计可依据恢复出的 flushable 队列重建，与实际未完成写入对账。
- 错误可通过 `errors.Is` 区分配额耗尽、Context 取消、DB 关闭、底层写入失败四类。
- 提供按规则的可观测指标、排队耗时与事件通知。

## Non-Goals

- 不做基于租户身份/账号的鉴权或配额；归属维度只有 UserKey 区间（"用户键范围"）。
- 不做速率限制（tokens/s、bytes/s）；四个维度均为**瞬时占用量（gauge）**，不是时间窗口速率。
- 不为了配额拆分跨范围 Batch 的原子提交（一个 Batch 仍分配连续 seqnum、整体可见或整体不可见）。
- 不改变 WAL failover、flush/compaction 重试、ingest 后台校验重试等既有内部行为本身；只要求这些内部重试不导致二次计费或绕过预留。
- 不持久化配额配置与配额计数器到磁盘；重启后仅对"未 Flush 内存"维度做对账重建（见 FR-12），其余维度从零开始（在途提交在崩溃时本就不完整可见）。
- 不限制读路径、Compaction、Flush、Snapshot、Iterator、Checkpoint、table stats、ingest 后台校验等非用户写路径。
- 不引入跨 DB 实例的全局配额协调。

## Background & Context

代码调研结论（本仓库为较新版本 Pebble，Go 1.25.3）：

- 全部普通用户写（Set/Delete/DeleteSized/SingleDelete/DeleteRange/Merge/LogData/RangeKeySet/Unset/Delete/Batch.Commit/Apply/ApplyNoSyncWait）唯一漏斗是 `DB.applyInternal`（[db.go:789-853](file:///Users/tog/my/code/swe/workspaces/pebble/pebble-key-range-write-quota-T04/db.go#L789-L853)）；`d.commit.Commit`（[db.go:837](file:///Users/tog/my/code/swe/workspaces/pebble/pebble-key-range-write-quota-T04/db.go#L837)）返回的 error 会被 `Logger.Fatalf` 终结进程。因此**配额拒绝这类业务错误必须在 `d.commit.Commit` 之前返回**。
- 提交串行点在 commitPipeline（`p.mu`，[commit.go:456-471](file:///Users/tog/my/code/swe/workspaces/pebble/pebble-key-range-write-quota-T04/commit.go#L456-L471)），seqnum 分配与 WAL 写入在其内；memtable 应用并发进行（[db.go:855-889](file:///Users/tog/my/code/swe/workspaces/pebble/pebble-key-range-write-quota-T04/db.go#L855-L889)）。
- SSTable 摄取走独立路径 `d.ingest`（[ingest.go:1741](file:///Users/tog/my/code/swe/workspaces/pebble/pebble-key-range-write-quota-T04/ingest.go#L1741)）：加载/校验文件（ingestLoad）→ 排序互斥校验 → link/copy 进库目录（ingestLinkLocal，[ingest.go:1786](file:///Users/tog/my/code/swe/workspaces/pebble/pebble-key-range-write-quota-T04/ingest.go#L1786)）→ `ingestSem` + `AllocateSeqNum`（[ingest.go:2052-2053](file:///Users/tog/my/code/swe/workspaces/pebble/pebble-key-range-write-quota-T04/ingest.go#L2052-L2053)）；普通 ingest 不写 WAL、直接落 LSM，flushable ingest 经 `directWrite` 写独占 WAL 并以 ingestedFlushable 入 flushable 队列。失败已有 `ingestCleanup` 回滚本地链接（[ingest.go:834-858](file:///Users/tog/my/code/swe/workspaces/pebble/pebble-key-range-write-quota-T04/ingest.go#L834-L858)）。
- Batch 编码为连续 repr 字节；`Batch.Reader()` 可顺序遍历全部 record；`Batch.Count()` 为 memtable 修改操作数；`batch.memTableSize` 为 memtable 节点悲观上界（[batch.go:571-615](file:///Users/tog/my/code/swe/workspaces/pebble/pebble-key-range-write-quota-T04/batch.go#L571-L615)）；`Batch.Len()/Repr()` 为 WAL 逻辑字节；range-del 的 key/value 即 [start,end)，range-key 的 end 需从 value 解码（`rangekey.DecodeEndKey`）。嵌套 Batch 只有 `Batch.Apply` 一种形式，且是**字节摊平拷贝**（[batch.go:622-714](file:///Users/tog/my/code/swe/workspaces/pebble/pebble-key-range-write-quota-T04/batch.go#L622-L714)），子 batch 不独立提交。
- 大 batch（≥ largeBatchThreshold）在 applyInternal 内被构造成 `flushableBatch`（[db.go:830-836](file:///Users/tog/my/code/swe/workspaces/pebble/pebble-key-range-write-quota-T04/db.go#L830-L836)），提交成功后 `batch.data` 所有权移交，故归属扫描必须在此之前完成。
- 未 flush 字节真值来自 flushable 队列（`flushable.inuseBytes()/totalBytes()`，[flushable.go:22-45](file:///Users/tog/my/code/swe/workspaces/pebble/pebble-key-range-write-quota-T04/flushable.go#L22-L45)）；flush 完成于 compaction.go flush1（[compaction.go:1618](file:///Users/tog/my/code/swe/workspaces/pebble/pebble-key-range-write-quota-T04/compaction.go#L1618) 起）；Open 恢复在 replayWAL 后立即调度并等待 flush（[open.go:449-452](file:///Users/tog/my/code/swe/workspaces/pebble/pebble-key-range-write-quota-T04/open.go#L449-L452)），未形成完整 WAL record 的在途写重启后消失（[recovery.go:552-555](file:///Users/tog/my/code/swe/workspaces/pebble/pebble-key-range-write-quota-T04/recovery.go#L552-L555)）。
- 本版本无整份 Options 热替换 API；动态配置先例是闭包字段（如 `SpanPolicyFunc func() SpanPolicy`，[options.go:1371-1451](file:///Users/tog/my/code/swe/workspaces/pebble/pebble-key-range-write-quota-T04/options.go#L1371-L1451)），compaction 以局部快照变量绑定旧策略（[compaction.go:2861-2903](file:///Users/tog/my/code/swe/workspaces/pebble/pebble-key-range-write-quota-T04/compaction.go#L2861-L2903)）。
- 现成区间工具：`base.KeyRange`、`UserKeyBounds`（Overlaps/Contains/Valid/Clone，[internal/base/key_bounds.go:15-259](file:///Users/tog/my/code/swe/workspaces/pebble/pebble-key-range-write-quota-T04/internal/base/key_bounds.go#L15-L259)）。关闭信号 `closedCh`（[db.go:1864](file:///Users/tog/my/code/swe/workspaces/pebble/pebble-key-range-write-quota-T04/db.go#L1864)）。错误哨兵先例 `ErrClosed`/`ErrReadOnly`（[db.go:57-71](file:///Users/tog/my/code/swe/workspaces/pebble/pebble-key-range-write-quota-T04/db.go#L57-L71)）。指标/事件/每次写耗时分别走 `Metrics`、`EventListener`（EnsureDefaults+MakeLogging+Tee 三处同步）、`BatchCommitStats`。
- 已确认的产品决策（用户拍板）：①新增带 ctx 的写 API，`WriteOptions` 增加 Priority；②跨范围批次逐操作归属、整批原子预留；③重叠规则按优先级独占匹配；④等待中 DB 关闭返回可 `errors.Is` 的 ErrClosed 错误。

## Functional Requirements

### 配置与开关

- **FR-1（默认关闭）**：新增 Options 实验字段，形态为无参闭包（nil 即关闭），并提供静态配置构造器。字段注释按仓库惯例独占一行 `// Experimental.`。EnsureDefaults 不把它改成非 nil；Clone 不破坏闭包语义；该字段不进 OPTIONS 文本序列化（与其它闭包选项一致）。
- **FR-2（规则定义）**：每条规则至少包含：稳定名称（RuleName，非空、同一份配置内唯一，用于指标与错误归因）、`[Start, End)` 用户键区间（Start/End 允许为空表示 ±∞，沿用 KeyRange 语义）、整数优先级（数值越大越优先）、策略、四个维度上限（待提交字节数、待提交操作数、未 Flush 字节数、并发 Batch 数；均 ≥0，0=该维度不限制）。
- **FR-3（配置校验）**：Options.Validate 在配额字段非 nil 时校验：闭包返回值每次调用必须可用；区间合法（Valid、Compare(Start,End)<0 或空边界）；名称非空且唯一；策略枚举合法；各维度上限 ≥0。**不重叠**配置：任意两规则区间重叠即报错；**重叠**配置仅当重叠规则优先级互异时合法，由调用方自行选择是否使用重叠。校验在 Open 时（EnsureDefaults 之后）与每次闭包切换出新配置时生效；非法新配置不得影响在用旧配置（拒绝切换并经 Logger/事件暴露）。
- **FR-4（归属匹配）**：点键按 `Comparer.Compare` 二分匹配"包含该键且优先级最高"的规则；未命中任何规则的键不受配额约束（默认放行）。range-del/range-key 操作按其原始 `[start,end)` 与规则求交：**对每一个与之重叠的规则**都计入该操作的字节与 1 个操作数（独占匹配只用于点键；范围操作天然跨区）。MVCC 后缀不影响区间归属（以完整 UserKey 比较；不使用 Split 截断）。

### 维度计量口径

- **FR-5（四个维度的计量与持有窗口）**：
  1. **待提交字节数 PendingBytes**：一次预留的 repr 逻辑字节（= 提交给 WAL 的逻辑字节，按 record 逐条归属求和；LogData 按其 record 字节计入）。持有窗口：准入后 → 本次提交对调用方完成（普通 Apply 为发布+WAL sync 结束；ApplyNoSyncWait 为 `Batch.SyncWait()` 返回；ingest 为 d.ingest 返回）。WAL failover 导致的物理重写**不重复计费**。
  2. **待提交操作数 PendingOps**：点键/range 操作按 `Batch.Count()` 口径逐条归属求和（LogData 不计操作数）；ingest 按摄取的 SST 文件数计（excise 计 1）。持有窗口同 PendingBytes。
  3. **未 Flush 内存占用 UnflushedBytes**：按 `memTableEntrySize` 口径逐操作归属求和（与 `batch.memTableSize` 同口径，range-del/range-key 也计入，LogData/普通 ingest 为 0）。持有窗口：准入后 → 容纳该写入的 flushable（memtable / flushableBatch / ingestedFlushable）**成功 flush 出队**；大 batch 与 flushable ingest 持有到各自 flush 完成；直接落 LSM 的普通 ingest 该维度为 0、不留存占用。flush 失败重试期间继续持有（数据仍未 flush），flush 致命失败进程终止时无需再释放。
  4. **并发 Batch 数 ConcurrentBatches**：每次普通提交计 1，每次 ingest 调用计 1（与其内文件数无关）；持有窗口同 PendingBytes。
- **FR-6（跨范围批次的原子归属）**：准入前对 Batch repr 做**一次**扫描（大 batch 必须在 `batch.data` 移交前），得到"规则 → 四维需求量"向量；range 操作的字节与操作数计入每个重叠规则。预留对向量中所有规则、所有维度**同时成立或全部不发生**；排队等待期间不得硬占用任何规则的可用量（不允许"先占 A 再等 B"），以杜绝跨范围 hold-and-wait 死锁。
- **FR-7（嵌套 Batch）**：`Batch.Apply` 已摊平字节，嵌套层级不产生独立预留；归属只在最终提交的 Batch 上扫描一次，字节/操作数不重复计算。子 Batch 单独 Commit 时按其自身内容独立预留。
- **FR-8（Ingest 归属与准入点）**：以 loadResult 中各 SST/对象元数据的总大小作为 PendingBytes（含 attached blob 对象大小），文件数（excise +1）作为 PendingOps，覆盖的用户键区间（point/range key bounds 的并集）做规则匹配；flushable ingest 额外按对象大小预留 UnflushedBytes 直到其 flush 完成。准入点在 ingestLoad 与排序/互斥校验完成后、ingestLinkLocal（消耗本地磁盘）与 ingestSem/AllocateSeqNum（分配 seqnum）之前。ingest 在准入之后任一步失败走既有 cleanup，必须完整释放预留。

### 策略与等待

- **FR-9（四种策略）**：
  - `Reject`（立即失败）：当前可用量不能立即满足全部需求 → 立刻返回配额耗尽错误，不进入等待。
  - `Wait`（等待）：进入单一全局等待队列，按入队顺序 FIFO；可被 ctx 取消与 DB 关闭唤醒。
  - `Priority`（按优先级排队）：队列按 `WriteOptions.Priority` 从大到小排序，同优先级按入队顺序 FIFO；Priority 缺省为 0。
  - `RejectRange`（按范围拒绝）：批次触及 ≥2 条规则时，若所有触及规则**不能此刻同时满足**，整批立即拒绝（错误标识第一条不满足的规则与维度），绝不为跨范围批次部分等待；只触及 1 条规则时等价于 Reject。
- **FR-10（混合策略与公平性）**：一个跨范围批次触及多条不同策略规则时：任一触及规则为 Reject 且该规则当前不可满足 → 立即拒绝；触及 ≥2 规则且任一为 RejectRange → 适用 RejectRange 语义；否则进入等待，只要触及规则中有 Priority 即按请求优先级排序，否则 FIFO。为防止饥饿：等待者按队列顺序获得**软预留**（其需求量对其后的授权不可用，但不增加已用量），容量释放时从队首开始判定，队首不可满足时后续等待者只能使用队首软预留之外的余量。优先级只改变满足者的挑选顺序，不消耗更早等待者受保护的量，因此任何已入队请求都会在有限时间内得到满足（在容量被持续释放的前提下）。
- **FR-11（取消与关闭）**：等待通过 ctx 可取消：取消时移除等待者、释放其软预留并唤醒后继，返回 `context.Canceled`/`context.DeadlineExceeded`（可用 `errors.Is` 判定）。等待期间 DB.Close 发生：关闭信号唤醒全部等待者，返回 `ErrClosed`（可用 `errors.Is` 判定；这是等待准入阶段的错误返回，不改变 applyInternal 入口处已关闭即 panic 的既有惯例）。已获得预留后的提交阶段遇 Close 仍按引擎既有语义处理。
- **FR-12（配置切换绑定旧规则）**：闭包返回新配置时，管理器原子切换规则表：新准入请求只按新规则匹配与计费；已存在的硬预留与软预留始终绑定其准入时刻的**不可变规则快照**，释放动作直达该快照对象（不按名称重新查表）。被删除规则的旧快照在仍有未释放占用时保留计数（指标中可标记为 retired），全部释放后回收。切换是一次原子指针发布，任何写入、等待、指标读取都不会看到半更新状态。
- **FR-13（重启对账）**：Open 完成 WAL replay 后、对外提供服务前，依据恢复出的 flushable 队列重建 UnflushedBytes 占用：flushableBatch 按其点键/范围碎片逐规则归属；ingestedFlushable 按其 UserKeyBounds 与对象大小归属（计入所有重叠规则）；普通 memtable 取其实际 inuseBytes 与键边界归属到重叠规则（边界跨多规则时对每个重叠规则计全量，保守不低估）。PendingBytes/PendingOps/ConcurrentBatches 重启后为零（崩溃时在途提交本就不完整可见）。重建结果可经指标按规则读出，且与队列实际 totalBytes/inuseBytes 可对账（单规则场景下严格相等）。

### 失败与隔离

- **FR-14（先预留后提交、失败全释放）**：普通写的准入发生在 applyInternal 内、`committing` 置位与 memTableSize 刷新/flushableBatch 构造完成之后、`d.commit.Commit` 之前；其后的任何校验失败、WAL 写入/sync 失败、memtable 准备/应用失败、提交失败，无论错误如何返回（含 Fatalf 前的 defer 执行），预留都被完整释放。一个预留句柄的每一段（提交段、flush 段）释放保证恰好一次（重复释放被吞掉并在 invariants 构建下可断言），任何维度计数不得为负。Ingest 失败（link、attach、seq 分配后失败、MANIFEST 致命错误路径）同理；seqnum/文件号不回收是引擎既有语义，与配额释放无关。
- **FR-15（错误分类）**：调用方可区分四类失败——①配额耗尽：新哨兵 `ErrWriteQuotaExceeded`（包装错误携带规则名与维度，`errors.Is(err, ErrWriteQuotaExceeded)` 成立）；②Context 取消：`errors.Is(err, context.Canceled/DeadlineExceeded)`；③DB 关闭：`errors.Is(err, ErrClosed)`（等待阶段）或既有 panic(ErrClosed)（入口阶段）；④底层写入失败：维持现状（WAL/manifest 致命错误走 Logger.Fatalf，不与配额错误混用）。
- **FR-16（非写路径零侵入）**：Compaction、Flush 调度与执行、Snapshot、Iterator、Get、Checkpoint（WithFlushedWAL 触发的 LogData 除外，其本身是用户写，正常计费）、table stats、ingest 后台校验重试、WAL failover 内部重放：不得调用配额准入、不得被配额锁阻塞、不因配置切换读到中间状态。配额管理器使用独立锁，严格遵守既有加锁顺序（不与 commit.mu/d.mu 形成反向依赖；持有配额锁期间不做 I/O、不等待 flush）。
- **FR-17（内部重试不重计费）**：任何已准入操作在引擎内部发生的重试（WAL failover 物理重放、flush 失败再调度、ingest 后台校验重排队列、compaction 取消后重挑）都不二次申请/扣减配额；不存在任何绕过准入点的用户数据重入路径（若实现中发现新的重入，必须改走同一准入点）。

### 可观测性

- **FR-18（指标/耗时/事件）**：
  - `Metrics` 增加配额子结构：按规则名给出四维**当前占用 gauge**、当前等待者数（gauge）、累计准入次数、累计拒绝次数（按策略/维度）、累计等待次数与累计等待时长、配置版本（generation）；retired 规则在排空期间仍可见并带 retired 标记。DB.Metrics() 快照与管理器状态一致。
  - `BatchCommitStats` 增加 `QuotaWaitDuration`（仅排队等待耗时，不含提交本身）。
  - `EventListener` 增加配额相关事件（至少：拒绝事件、配置切换事件），并同步 EnsureDefaults（no-op）、MakeLoggingEventListener、TeeEventListener 三处。

## Non-Functional Requirements

- **NFR-1（关闭态零开销）**：未配置配额时，写热路径仅增加一次 nil 判断（可被内联），无额外 repr 扫描、无额外内存分配、无新锁、无 goroutine；写入顺序、WAL 编码与 sync 时机、WriteStall 触发与耗时口径字节级/时序级保持现状。
- **NFR-2（启用态低开销）**：启用后每批次仅一次线性 repr 扫描（与既有 refreshMemTableSize 同阶 O(record 数)）；准入热路径在无等待时为短暂的无 I/O 临界区；规则匹配为有序切片上的二分。
- **NFR-3（并发正确性）**：在 `-race` 下通过高并发准入/取消/关闭/切换压力测试；任何时序下无负数、无重复扣减、无死锁、无永久饥饿（有持续释放时等待者最终推进）。
- **NFR-4（可维护性）**：并发准入核心独立为 internal 子包，仅依赖 base 类型与标准库，不反向依赖根包；根包负责公共配置类型、repr 归属扫描与各入口接线。
- **NFR-5（兼容性）**：公开 API 只做加法；既有 API 签名与行为不变；老的写入口（无 ctx）等价于以 `context.Background()` 调用新入口。

## Constraints

- **Technical**：Go 1.25.3，模块 `github.com/cockroachdb/pebble`；遵循仓库 lint/crlfmt；测试惯例为 datadriven testdata + `leaktest.AfterTest` + vfs.NewMem()；关键并发测试需 `-race` 与 stress 友好。
- **Business**：实验能力，API 允许后续演进；默认行为零变化是硬约束。
- **Dependencies**：优先复用 `base.KeyRange/UserKeyBounds`、cockroachdb/errors、现有 Event/Metrics 惯例；并发等待原语可使用仓库已依赖的 `github.com/cockroachdb/crlib/fifo`（加权/FIFO/可取消）或在 internal 子包内自建支持优先级与软预留的等待队列（fifo 不支持跨多规则原子获取与优先级，预计需要自建薄队列层）。不得新增不必要的第三方依赖。

## Assumptions

- "待提交字节数/操作数量"按瞬时占用量理解（在途未完成的提交所持有的字节数/操作数），而非每秒速率；上限 0 表示该维度不限制。
- 规则优先级为 int32，数值越大越优先；同优先级重叠为非法配置；策略零值默认为 `Reject`（最安全），由 Validate/EnsureDefaults 明确。
- `WriteOptions.Priority` 为 int32，缺省 0；仅影响 Wait/Priority 策略下的等待队列顺序，不影响 seqnum/WAL 顺序。
- 普通 ingest 直接落 LSM、不占 memtable，故 UnflushedBytes 为 0；其 PendingBytes 以 SST/对象文件总大小计。
- 关闭态语义：进入等待之前 DB 已关闭仍 panic(ErrClosed)（现状不变）；仅在配额等待过程中被 Close 唤醒时返回 ErrClosed 错误。
- 配额配置闭包在 Open 时被调用一次建立管理器，之后每次准入时廉价调用以发现新版本（静态构造器返回同一个不变配置指针，开销为一次间接调用）。

## Acceptance Criteria

### AC-1：默认关闭且关闭态行为零变化
- **Type**: `rule`
- **Given**：Options 未配置配额字段（nil）
- **When**：执行任意现有写 API 并跑既有测试与基准
- **Then**：写路径不发生配额扫描/分配/加锁；既有全部测试通过；写入相关 bench 与基线相比无方向性回退
- **Pass Condition**：`go test ./...`（含 race 关键包）全绿；代码中 nil 快路径位于 applyInternal/ingest 入口且禁用态下不触达管理器；关闭态 bench 对比（BenchmarkBatchSet 等）差异在噪声范围内
- **Evidence**：测试命令输出、bench 对比结果、关键代码位置

### AC-2：规则配置校验完备
- **Type**: `rule`
- **Given/When/Then**：覆盖合法不重叠规则、合法优先级重叠规则；非法用例（空名、重名、非法区间、重叠且同优先级、未知策略、负上限、闭包返回 nil 之外的非法值）
- **Pass Condition**：合法用例 Open/Validate 通过；每个非法用例 Validate 返回错误（或切换被拒绝且旧配置继续生效），均有 table-driven/datadriven 测试
- **Evidence**：options/write_quota 校验测试输出

### AC-3：所有用户写入口与 Ingest 均"先预留后提交"
- **Type**: `rule`
- **Given**：启用至少一条覆盖全部键的 Reject/Wait 规则
- **When**：分别调用 Set/Delete/Merge/DeleteRange/RangeKeySet/Unset/Delete/LogData、Batch.Commit、Apply、ApplyNoSyncWait、嵌套 Apply 后提交、Ingest/IngestExternalFiles/IngestAndExcise（含 flushable ingest 分支）
- **Then**：配额计数/事件证明预留发生在 seqnum 分配、WAL 写、ingest link/seqnum 之前；准入拒绝时无 seqnum 消耗（后续写入 seqnum 连续、无被拒写入痕迹）、无 WAL 字节、无 ingest 文件残留
- **Pass Condition**：集成测试断言拒绝前后 seqnum 增量为 0、WAL 大小不变、ingest 输入文件仍在且库目录无链接；代码评审确认每个入口的预留点先于资源消耗点
- **Evidence**：测试断言与代码位置清单

### AC-4：四维计量正确且按期释放
- **Type**: `rule`
- **Given**：单规则分别只开一个维度（其余为 0=不限）
- **When**：提交已知 repr 字节/操作数/memTableSize 的批次（含 range-del/range-key/LogData）、大 flushableBatch、普通 ingest、flushable ingest，并等待 flush
- **Then**：Metrics 中各维度占用在持有窗口内等于按 FR-5 口径的期望值；提交完成后 PendingBytes/PendingOps/ConcurrentBatches 归零；flush 完成后 UnflushedBytes 归零；0 上限维度永不拒绝
- **Pass Condition**：逐维度 table-driven 测试全部通过（含 flush 后轮询指标为 0）
- **Evidence**：测试输出与期望值表

### AC-5：任何失败路径完整且仅一次释放
- **Type**: `rule`
- **Given/When**：注入/触发：准入前校验失败（非法 batch、errNoSplit、WAL disabled+sync）、ingest link/attach 失败、WAL 写入致命错误路径、SyncWait 取消场景；以及对同一预留句柄模拟重复释放
- **Then**：失败后所有维度占用与失败前持平（无泄漏、无部分扣减）；计数器永不为负；重复释放不产生第二次扣减（invariants 下可断言）
- **Pass Condition**：失败注入测试 + 计数器快照对比通过；代码中释放为 defer 配对且句柄有 once 保护
- **Evidence**：测试输出、代码位置

### AC-6：四种策略语义正确
- **Type**: `rule`
- **Given**：容量为 N 的单规则，分别配置 Reject/Wait/Priority/RejectRange
- **When**：占满后发起后续写：Reject 立即得 ErrWriteQuotaExceeded；Wait 阻塞并在释放后按 FIFO 完成；Priority 下高优先级请求先于更早入队的低优先级请求完成；RejectRange 对跨两规则且其一不可用的批次立即拒绝、对单规则批次等价 Reject、跨范围且全部可用时成功
- **Then**：完成顺序、错误类型、等待耗时字段与策略定义一致
- **Pass Condition**：四类策略各有确定性测试（用通道同步顺序，不依赖 sleep 玄学）
- **Evidence**：策略测试输出

### AC-7：跨范围批次原子预留与 range 操作多规则计费
- **Type**: `rule`
- **Given**：两条相邻不重叠规则 R1/R2；构造含 R1 点键、R2 点键、横跨边界的 DeleteRange/RangeKeySet 的单批次
- **Then**：需求向量同时包含两条规则；任一规则容量不足时批次被拒/等待且另一规则占用不增加；成功时 range 操作同时计入 R1、R2；提交段/flush 段释放后两规则均归零
- **Pass Condition**：跨范围归属与原子性测试通过
- **Evidence**：测试输出

### AC-8：嵌套 Batch 归属一次
- **Type**: `rule`
- **Given**：父 Batch.Apply 多个子 Batch（含多层），子 Batch 也单独提交
- **Then**：父提交只产生一次预留，字节/操作数等于摊平后的总量；子单独提交各自独立预留；无重复计数
- **Pass Condition**：嵌套测试断言指标增量
- **Evidence**：测试输出

### AC-9：并发安全（无负数/重扣/死锁/饥饿）
- **Type**: `rule`
- **Given**：M 个 goroutine 并发对多条交叉范围做随机大小写、随机 ctx 超时、随机 Close、运行时随机切换配置，`-race`
- **When**：持续运行固定迭代轮数后排空
- **Then**：全程无 panic（预期的关闭 panic 除外）、无负数计数、无重复扣减；排空后所有规则四维占用归零；无死锁（测试有总超时）；跨范围饥饿场景中：R1+R2 批次在 R1 被持续占用、R2 空闲再释放的节奏下，每个批次在有限轮内完成（软预留有效性断言）
- **Pass Condition**：`go test -race -run Quota` 通过且带饥饿专项断言；stress 标签下可重复
- **Evidence**：race 测试输出

### AC-10：配置热切换的旧规则绑定
- **Type**: `rule`
- **Given**：配置 v1（规则 R，策略 Wait 容量小）；一批写入已准入并在途/在等待
- **When**：切换为 v2（删除 R 或把 R 改小/改策略/改名），随后新写入进来
- **Then**：在途写入释放计入 v1 的 R（retired），v2 新写入只受 v2 约束；任何时刻 Metrics 不出现半新半旧状态；v1 排空后 retired 条目消失；非法 v3 被拒绝且 v2 继续服务
- **Pass Condition**：热切换确定性测试通过（通道控制在途释放时机）
- **Evidence**：测试输出

### AC-11：Context 取消语义
- **Type**: `rule`
- **Given/When**：经 ApplyWithContext 提交，容量占满后入队，再 cancel ctx（含 deadline 超时）；以及 cancel 与授予同时发生的竞态反复运行
- **Then**：取消的请求返回 context 取消错误、不留占用、后继等待者被唤醒推进；授予竞态下结果恰好二选一（得到预留并完成提交，或取消且无占用），不出现"取消成功但占用泄漏/提交了但返回取消"
- **Pass Condition**：取消测试 + 竞态压力测试（-race）通过
- **Evidence**：测试输出

### AC-12：DB 关闭中断等待
- **Type**: `rule`
- **Given/When**：多个写在配额队列等待时调用 Close
- **Then**：等待中的调用返回 ErrClosed（errors.Is 成立）且无遗留等待者/goroutine 泄漏（leaktest）
- **Pass Condition**：关闭测试通过
- **Evidence**：测试输出

### AC-13：重启对账
- **Type**: `rule`
- **Given**：启用配额写入若干数据，在 flush 完成前（用手动 flush 控制/故障点）制造未 flush 状态后关闭重开（同一份配额配置）
- **When**：Open 完成后立即读 Metrics
- **Then**：Pending 三维为 0；UnflushedBytes 按规则等于恢复队列中 flushableBatch/ingestedFlushable/memtable 的归属值；等待 replay 触发的 flush 完成后归零；重开前后"规则 → 未 flush 字节"对账一致
- **Pass Condition**：重开对账测试通过（覆盖大 batch 与 flushable ingest 两种恢复形态）
- **Evidence**：测试输出

### AC-14：错误分类可判定
- **Type**: `rule`
- **Given/When**：分别制造四类失败
- **Then**：errors.Is 对 ErrWriteQuotaExceeded / context.Canceled(+DeadlineExceeded) / ErrClosed 判定正确，且配额错误的包装文本/结构化字段可读出规则名与维度；底层致命错误仍走 Fatalf 不被包装成配额错误
- **Pass Condition**：错误分类测试通过
- **Evidence**：测试输出

### AC-15：非写路径零侵入
- **Type**: `rule`
- **Given/When**：配额启用且处于满额/等待/切换中，并发执行 Get/NewIter/Snapshot/Checkpoint/手动 Compact/自动 Flush-Compaction/ingest 后台校验
- **Then**：这些路径不阻塞、不报错、不取配额管理器锁（代码证据）；读不到中间配置状态
- **Pass Condition**：并发隔离测试通过；grep/评审确认这些路径无管理器调用
- **Evidence**：测试输出 + 调用点清单

### AC-16：内部重试不重复计费
- **Type**: `rule`
- **Given/When**：启用 WAL failover（次级目录）触发物理重写，以及 flush 失败再调度、ingest 后台校验重排队列的既有测试场景下跑配额写
- **Then**：每次用户操作的累计准入计数为 1，占用不被重试放大；释放后归零
- **Pass Condition**：failover/重试场景测试通过（如既有故障注入设施不足，允许以内部测试钩子模拟）
- **Evidence**：测试输出

### AC-17：可观测性完备
- **Type**: `rule`
- **Given/When**：启用配额发生拒绝、等待、切换
- **Then**：Metrics 配额子结构字段齐全且与状态一致；BatchCommitStats.QuotaWaitDuration ≥ 实际排队耗时口径；拒绝/切换事件经 EventListener（含 Tee/Logging）触发且信息含规则名
- **Pass Condition**：指标/事件测试（含 testdata 黄金输出）通过
- **Evidence**：测试输出

### AC-18：整体工程质量（评审维度）
- **Type**: `rubric`
- **Dimension**：设计与引擎既有抽象的契合度、并发正确性论证（无死锁/饥饿的构造性理由）、测试对需求的覆盖与确定性、文档注释质量
- **Scale**: 1-5
- **Anchors**：1 = 改动侵入提交主循环造成默认行为风险或并发设计有明显漏洞；3 = 功能可用但接线分散、测试依赖时序 sleep、边界口径有歧义；5 = 准入核心独立可测、入口接线最小且对称（预留/释放成对）、失败与切换语义有构造性论证、测试稳定确定性强、公共 API 注释与实验标记完整
- **Pass Threshold**: >= 4
- **Evidence**：独立评审（代码阅读 + 测试运行 + race/stress 结果）

## Open Questions

- [x] Context 与请求优先级透传方式 → 新增 `DB.ApplyWithContext`/`Batch.CommitWithContext`，`WriteOptions` 增加 `Priority int32`（用户已确认）
- [x] 跨范围批次归属 → 逐操作归属 + 整批原子预留（用户已确认）
- [x] 重叠规则匹配 → 按优先级独占匹配，同优先级重叠非法（用户已确认）
- [x] 等待中 Close → 返回 errors.Is 可判定的 ErrClosed（用户已确认）
- [ ] 等待队列实现选型（crlib/fifo + 自建跨规则协调层 vs 全自建）留待 Plan/实现阶段按测试需要决定，不改变本文件语义。

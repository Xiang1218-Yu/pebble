# Pebble 按用户键范围写入配额 - 实施计划

说明：所有任务的测试与代码位于 /Users/tog/my/code/swe/workspaces/pebble/pebble-key-range-write-quota-T04。
测试命令基线：`go build ./... && go vet ./...`；包内测试 `go test ./internal/writequota/...`；
根包配额专项 `go test -race -run 'WriteQuota|Quota' ./...`；全量 `go test ./...`（长耗时，放最后）。
AC 编号对应 spec.md。

## Task 1: internal/writequota 并发准入核心包
- **Status**: `completed`
- **Priority**: high
- **Depends On**: None
- **Completion Evidence**:
  - 新增 internal/writequota/writequota.go：Manager 单 mutex + used/reserved 双计数 + 优先级/FIFO 等待堆 + 调度顺序软预留授予扫描（grantPendingLocked，按 blocked 聚合保护更早等待者，余量可被后来的无关范围请求使用）；Grant 两段 once 释放；Configure 旧规则 retired 直达；Close 广播；Metrics 快照；ErrExceeded/ExceededError（带规则名/维度/策略）。
  - 新增 internal/writequota/writequota_test.go（17 个测试）覆盖 TR-1.1~1.6：四策略、跨范围原子性、软预留公平、取消/授予竞态 500 次、Close、Reconfigure 绑定与 retired 排空、混合策略、重复释放。
  - 验证命令：`go test -race -count=10 ./internal/writequota/` 全绿；`go test -tags invariants -count=2 ./internal/writequota/` 全绿。
  - TR-1.7 rubric 自评分 5：单锁临界区无 I/O、等待交接在锁内一次决定、授予算法有构造性公平论证、核心零根包依赖可独立压测；待独立复核。
- **Description**：
  - 新建 `internal/writequota/` 包，仅依赖 `internal/base`、标准库与仓库已有依赖，不反向依赖根包。
  - 定义策略枚举（Reject/Wait/Priority/RejectRange）、规则规格（稳定 ID/名称、四维上限、策略、优先级——匹配与区间重叠判定在根包编译期解决，核心按规则 ID/下标工作）、不可变配置快照（generation）、四维需求向量（PendingBytes/PendingOps/UnflushedBytes/ConcurrentBatches）与授权句柄 Grant。
  - 实现 Manager：单一 mutex 保护；`used`（硬占用）与 `reserved`（排队者软预留）两套计数；Admit(ctx, demand, priority, closedCh)（原子满足判定→立即授予 / Reject 系立即拒绝 / 入队等待）；单等待队列支持 FIFO 与 priority+FIFO 顺序；唤醒时从队首扫描，队首需求受软预留保护，后续等待者仅可使用其保护量之外的余量（构造性防饥饿）。
  - Grant 两段释放：ReleaseCommitted（释放 PendingBytes/PendingOps/ConcurrentBatches）与 ReleaseUnflushed，每段 sync.Once 恰好一次，释放后断言非负（invariants）并唤醒等待者。
  - Reconfigure(newSnapshot)：原子切换活动快照；旧规则状态对象在有未释放 used/reserved 时保留为 retired 且 Grant 直达旧对象；排空后移除。返回切换是否接受（非法快照由根包 Validate 拦截，核心仍需防御）。
  - Close(closeCh 联动)：唤醒全部等待者；Metrics 快照（每规则四维 used、等待者 gauge、累计准入/拒绝/等待次数与时长、generation、retired 标记）。
  - 等待可被 ctx.Done/closedCh 唤醒：取消时移除等待者、撤软预留、唤醒后继；取消与授予竞态必须恰好二选一（channel 交接在锁内决定）。
- **Acceptance Criteria Addressed**: AC-6（策略核心语义）、AC-9（核心并发）、AC-10（旧规则绑定核心）、AC-11（取消核心）、AC-12（关闭核心）、AC-18
- **Test Requirements**:
  - `rule` TR-1.1：容量占满后 Reject 立即拒绝、Wait FIFO 顺序授予、Priority 高优先级先授予（同优先级 FIFO）、RejectRange 在多规则需求向量下整批拒绝/全可用时成功；证据：go test 输出。
  - `rule` TR-1.2：跨两规则需求向量等待期间两规则 used 均不增加；授予时同时增加；任一段失败回滚不留痕；证据：单元测试断言。
  - `rule` TR-1.3：软预留防饥饿——队首需要 A+B，持续只释放/占用 A 的节奏下，B 一旦可用队首立即被授予且其需求量从未被后来者消耗；证据：确定性测试（通道同步，无 sleep 依赖）。
  - `rule` TR-1.4：ctx 取消（含 deadline）与授予竞态 1000 次重复，结果恰好二选一，无占用泄漏、无 goroutine 泄漏；证据：`go test -race -count` 输出。
  - `rule` TR-1.5：Close 唤醒所有等待者并返回关闭错误；Reconfigure 后旧 Grant 释放计入 retired 旧对象、新 Admit 走新对象、retired 排空后消失；证据：单元测试。
  - `rule` TR-1.6：重复释放安全（once），任何计数断言非负；invariants 构建下内部不一致直接 panic/失败；证据：测试输出。
  - `rubric` TR-1.7：并发原语设计质量；scale 1-5；anchors 1=多锁交叉有死锁面/状态散落；3=可用但等待与授予路径耦合难读；5=单锁临界区短小无 I/O、状态机清晰、等待交接在锁内一次决定、可独立压测；threshold >=4；证据：代码评审+压测。
- **Notes**：可评估以 crlib/fifo.Semaphore 做单规则 FIFO 闸门，但跨规则原子获取、优先级、软预留需要自有协调层；不对外导出超过接线所需的 API。

## Task 2: 公共配置类型、Options 接线与校验
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 1
- **Completion Evidence**:
  - 新增 write_quota.go：WriteQuotaPolicy 别名与四常量、WriteQuotaRule（Name/Bounds/Priority/Policy/四限）、WriteQuotaConfig、WriteQuotaConfigFunc、MakeStaticWriteQuotaConfig（拷贝+panic 校验）；validate（空名/重名/区间/负值/策略枚举/重叠同优先级，±∞ 用零长边界）；compiledQuotaRule/quotaConfig 编译（写核心 Configure 快照、按 Start 排序、重叠标记）；pointRule（重叠线性取最高优先级/不重叠二分）、rangeRules（区间重叠，含±∞）；writeQuotaManager（nil=禁用快路径、原子 current() 热切换、非法新配置记日志保留旧配置、close/metrics）。
  - Options 新增实验闭包字段 WriteQuotaConfig（含 Experimental 注释，不进 String）；Options.Validate 接入；DB 新增 writeQuota 字段；open.go 构建（Validate 后的初始配置）与失败清理；DB.Close 唤醒等待者。
  - 新增 write_quota_test.go 8 个测试（校验非法用例表、静态不可变、panic、点/范围匹配、重叠优先级、Open 校验、默认禁用、OPTIONS 文本排除）。
  - 验证：`go build ./...`、`go vet .`、`go test -race -count=1 -run 'TestWriteQuota|TestQuotaMatcher|TestMakeStatic' .` 全绿。
- **Description**：
  - 根包新增公共类型（write_quota.go）：WriteQuotaPolicy（常量 WriteQuotaReject/WriteQuotaWait/WriteQuotaPriority/WriteQuotaRejectRange）、WriteQuotaRule（Name、Bounds 用 UserKeyBounds 或 KeyRange[Start,End)、Priority int32、Policy、PendingBytes/PendingOps/UnflushedBytes/ConcurrentBatches int64）、WriteQuotaConfig（Generation 或由闭包携带、Rules）。
  - Options 增加实验闭包字段 `WriteQuotaConfig func() WriteQuotaConfig`（nil=关闭），注释含 `// Experimental.`；不加入 Options.String 序列化（与 SpanPolicyFunc 等闭包选项一致，需核实并在测试中锁定）。
  - 提供 MakeStaticWriteQuotaConfig(rules) 辅助（排序/拷贝/编译，返回稳定闭包）；Validate(cmp)：名称非空唯一、区间 Valid、上限≥0、策略合法、重叠规则必须优先级互异（同优先级重叠报错）；EnsureDefaults：策略零值默认 Reject；Clone 保持闭包共享。
  - Options.Validate 在字段非 nil 时调用一次闭包并校验返回配置；Open 时用通过校验的配置构建 internal/writequota.Manager（DB 字段，nil=禁用的快路径形态）。
- **Acceptance Criteria Addressed**: AC-1（关闭 nil）、AC-2、AC-18
- **Test Requirements**:
  - `rule` TR-2.1：合法不重叠、合法优先级重叠配置 Open 成功；每类非法配置（空名/重名/非法区间/同优先级重叠/未知策略/负上限）Validate 报错文本稳定；证据：table-driven 测试。
  - `rule` TR-2.2：nil（默认）时 DB 不构建管理器、写路径只走 nil 快路径；Options 克隆后闭包仍生效；OPTIONS 文本不含该字段；证据：测试 + 代码检查。
  - `rule` TR-2.3：MakeStatic 返回不可变配置（外部修改入参 slice 不影响已构建配置）；证据：隔离测试。

## Task 3: Batch/Ingest 归属扫描器（repr → 需求向量）
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 2
- **Completion Evidence**:
  - 新增 write_quota_scan.go：quotaConfig.scanBatch（单次 repr 线性扫描；record 字节按 Reader 位置差精确计算；点键独占匹配、range-del/range-key 全量计入每个重叠规则、LogData 仅计字节、Ingest/Excise 跳过；输出含 ConcurrentBatches=1）；buildIngestDemands（文件大小/文件数/excise，多规则重叠全量）、buildIngestUnflushed（flushable ingest 用）、unflushedBreakdown。
  - 新增 write_quota_scan_test.go 6 个测试：点键/范围操作跨规则/LogData/未命中/多层嵌套摊平/ingest+excise 口径。
  - 验证：`go test -race -count=1 -run 'TestScanBatch|TestBuildIngest' .` 全绿。
- **Description**：
  - 在根包实现单次线性扫描（与 refreshMemTableSize 同阶）：输入 Batch 与编译后的规则表，输出"规则 → 四维需求"：点键（Set/Delete/SingleDelete/DeleteSized/Merge/SetWithDelete）按 Compare 二分归属优先级最高的包含规则；record 字节用相邻偏移差计算（参照 batch.go Apply 内偏移差用法）；未 flush 字节按 memTableEntrySize 口径；range-del 用 rangedel.Decode、range-key 用 rangekey.DecodeEndKey 求 [start,end)，对所有重叠规则计入完整 record 字节与 1 操作；LogData 计字节不计操作、不计 unflushed；未命中键不计量。
  - 大 batch 场景确保扫描发生在 batch.data 移交前（applyInternal 中 flushable 构造之后、Commit 之前）。
  - Ingest 需求构建器：从 loadResult 元数据汇总 PendingBytes（SST+attached blob 总大小）、PendingOps（文件数，excise +1）、ConcurrentBatches=1、键边界并集匹配；普通 ingest Unflushed=0，flushable ingest 计对象大小。
  - 输出结果可直接翻译为 writequota.Admit 的需求向量（含每个命中规则的策略所需信息）。
- **Acceptance Criteria Addressed**: AC-4（计量口径输入）、AC-7、AC-8、FR-4/FR-5/FR-6/FR-7/FR-8
- **Test Requirements**:
  - `rule` TR-3.1：构造覆盖全部 kind 的 batch，逐规则四维期望值与扫描结果完全相等；range-del/range-key 横跨 R1/R2 时两规则各计全量；LogData 只计字节；证据：table-driven 单测。
  - `rule` TR-3.2：多层 Batch.Apply 摊平后扫描总量=摊平 repr 口径，只扫一次；子 batch 独立提交各自独立需求；证据：单测。
  - `rule` TR-3.3：ingest 需求构建器对普通 ingest（Unflushed=0）、flushable ingest、external 文件 span、excise 的取值正确；证据：单测（可用现有 ingest 测试辅助构造 loadResult）。

## Task 4: 普通写路径接线（准入/释放/ctx/优先级/错误/耗时）
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 3
- **Completion Evidence**:
  - WriteOptions 增加 Priority+GetPriority；DB.ApplyWithContext、Batch.CommitWithContext；applyInternal 增加 ctx 形参，既有入口透传 Background。
  - 准入位于 applyInternal 内 flushableBatch 构造之后、d.commit.Commit 之前；扫描+Admit；配额错误（ErrWriteQuotaExceeded 哨兵+WriteQuotaExceededError 别名）/ctx 错误/ErrClosed（等待段 map 到 pebble.ErrClosed）均在 Fatalf 之前返回；拒绝路径复位 committing。
  - Grant 始终挂 batch.quotaGrant；普通路径 defer 释放提交段；NoSyncWait 在 SyncWait 释放（batch.go 已接线）；QuotaWaitDuration 写入 BatchCommitStats。
  - write_quota_integration_test.go 8 个集成测试：拒绝（含 Merge/DeleteRange/Batch、未命中范围不受影响、被拒后 Reset 复用）、Wait+Flush 排空、优先级跨 flush 顺序、ctx 取消与无泄漏、Close 唤醒 ErrClosed、NoSyncWait 持有到 SyncWait、QuotaWaitDuration。
  - 验证：`go test -race -count=1 -run TestWriteQuota . ./internal/writequota` 全绿；核心既有写/打开测试回归通过。
- **Description**：
  - WriteOptions 增加 `Priority int32`（零值默认 0，注释说明仅影响配额等待顺序）。
  - 新增 `DB.ApplyWithContext(ctx, batch, opts)` 与 `Batch.CommitWithContext(ctx, opts)`；既有 Apply/Commit/ApplyNoSyncWait 以 context.Background() 复用同一内部函数；applyInternal 增加 ctx 参数（内部签名调整，所有既有调用点透传 Background）。
  - 准入点：applyInternal 内 closed/ReadOnly/Sync+DisableWAL/format/noSplit 等既有校验之后、`d.commit.Commit` 之前（含 flushableBatch 构造之后），调用扫描器+Manager.Admit；排队等待耗时写入 BatchCommitStats.QuotaWaitDuration（在 batch stats 计时口径内新增）。
  - Grant 挂在 Batch 上（不导出字段）：普通路径在 d.commit.Commit 返回后（含 sync 等待）ReleaseCommitted；ApplyNoSyncWait 路径延至 Batch.SyncWait 返回时释放；任何提前返回（校验失败发生在准入之后的分支）经 defer 配对释放；Grant 两段 once 保证。
  - 错误：新增 `ErrWriteQuotaExceeded` 哨兵与携带规则名/维度的包装错误类型（errors.Is 成立）；ctx 错误原样返回（context.Canceled/DeadlineExceeded）；等待中 Close 返回 ErrClosed；这些错误均在 d.commit.Commit 之前返回，绝不进入 Fatalf 分支；准入前既有的 panic/ErrReadOnly/errNoSplit 等行为不变。
  - 禁用态：Manager 为 nil，单次 nil 判断直通，不扫描、不分配。
- **Acceptance Criteria Addressed**: AC-1、AC-3（普通写部分）、AC-5、AC-6（端到端）、AC-11、AC-12、AC-14、NFR-1/NFR-2/NFR-5
- **Test Requirements**:
  - `rule` TR-4.1：Reject 规则满额时 Set/Merge/DeleteRange/RangeKey*/LogData/Apply/嵌套批次均返回 ErrWriteQuotaExceeded；拒绝前后下一次成功写入的 SeqNum 连续（被拒写不占 seqnum），WAL 字节不增；证据：集成测试。
  - `rule` TR-4.2：Wait/Priority/RejectRange 端到端顺序与 Task1 核心语义一致；Priority 不同的并发提交完成顺序确定；证据：通道同步测试。
  - `rule` TR-4.3：ApplyWithContext 取消返回 ctx 错误且无占用、后继被唤醒；授予/取消竞态压力（-race）无泄漏；等待中 Close 返回 ErrClosed 且 leaktest 干净；证据：测试输出。
  - `rule` TR-4.4：准入后任何失败路径（含大 batch flushable 构造失败虽在准入前、WAL 致命错误的 defer 路径）释放完整；普通 Apply 与 SyncWait 两个释放时点正确（NoSyncWait 场景在 SyncWait 前占用不降、之后归零）；证据：测试断言。
  - `rule` TR-4.5：四类错误 errors.Is 判定正确；配额错误可读出规则名与维度；证据：错误分类测试。
  - `rule` TR-4.6：禁用态下写路径无新增分配（testing.AllocsPerRun 对比或等价断言）、无 manager 触达；证据：测试 + bench。

## Task 5: Unflushed 维度登记与 flush 释放
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 4
- **Completion Evidence**:
  - 新增 write_quota_flush.go：flushableQuota（mutex+grants，匿名嵌入 memTable/flushableBatch/ingestedFlushable，编译期接口断言保证提升）；addGrant 支持并发 commitApply；releaseWriteQuotaUnflushed 恰好一次（与 Grant.once 双保险）。
  - commitApply 在 mem.apply 后把 grant 登记到目标 memtable；大 batch 在 Commit 前登记到 flushableBatch；compaction.go flush1 成功出队（queue[:n] 摘除，仅 err==nil）调用 releaseFlushablesWriteQuota；flush 失败重试不释放。
  - 集成测试 TestWriteQuotaWaitAndFlushIntegration 等验证：写入后未 flush 占用=entrySize、Flush 后归零、阻塞写在 flush 后推进；`go test -race` 全绿。
- **Description**：
  - 普通批次：commitApply 确定目标 memtable 后，把该批 UnflushedBytes 按规则聚合登记到 memtable（小 mutex 保护 map；仅启用配额时分配），与并发 mem.apply 兼容。
  - flushableBatch：Grant 直接挂到 flushableBatch（它自身即独立 flushable）。
  - 释放点：flush 成功、flushable 出队的位置（compaction.go flush1/runIngestFlush 完成 VersionEdit 后及队列移除处），按登记的规则聚合量 ReleaseUnflushed；失败重试不释放（数据仍在队列）；确保每个 flushable 的登记恰好释放一次。
  - Metrics 的未 flush gauge 因此在"准入后→flush 完成"窗口内准确。
- **Acceptance Criteria Addressed**: AC-4（Unflushed 维度）、AC-5（flush 段释放）、AC-13（重建所需的归属登记口径）
- **Test Requirements**:
  - `rule` TR-5.1：写入后 flush 前 UnflushedBytes=逐规则 memTableEntrySize 汇总；手动 Flush 后归零；range-del/range-key 跨规则两规则均归零；证据：Metrics 轮询测试。
  - `rule` TR-5.2：大 flushableBatch 从准入到其 flush 完成期间占用正确、完成后归零；多次轮换 memtable 后每表聚合计账正确；证据：测试。
  - `rule` TR-5.3：memtable 归属登记在并发 commitApply 下 -race 无数据竞争；flush 重试（既有故障注入或测试钩子）期间不提前释放；证据：race 测试。

## Task 6: SSTable 摄取接线
- **Status**: `pending`
- **Priority**: high
- **Depends On**: Task 5
- **Description**：
  - 在 d.ingest 的 ingestLoad 与 ingestSortAndVerify 完成后、ingestLinkLocal 之前准入（需求由 Task3 构建器给出）；被 Reject/cancel/close 时在消耗磁盘与 seqnum 之前干净返回错误。
  - d.ingest 全部退出路径（成功、ingestCleanup、attach 失败、prepare/apply 失败）经 defer 释放 Pending 三维；flushable ingest 分支把 Unflushed Grant 挂到 ingestedFlushable，在 runIngestFlush 成功完成时释放（复用 Task5 机制）。
  - 覆盖入口：Ingest/IngestWithStats/IngestExternalFiles/IngestAndExcise/IngestAndExciseWithBlobs；ingestSem 与 AllocateSeqNum 严格在准入之后。
  - 等待支持 ctx：Ingest 系列当前无 ctx，新增 `IngestWithContext`/`IngestExternalFilesWithContext` 等是否全量补齐在实现时按最小面处理（至少提供 d.ingest 级 ctx 透传与一个公开带 ctx 入口；无 ctx 入口用 Background），并在 spec 偏差处记录。
- **Acceptance Criteria Addressed**: AC-3（ingest 部分）、AC-4（ingest 计量）、AC-5（ingest 失败释放）、AC-11/AC-12（ingest 等待）
- **Test Requirements**:
  - `rule` TR-6.1：满额 Reject 时 ingest 返回配额错误；库目录无新链接文件、输入文件仍存在、seqnum 无新增；证据：ingest 集成测试（vfs.MemFS 可断言目录内容）。
  - `rule` TR-6.2：普通 ingest 成功后 Pending 三维归零且 Unflushed 始终为 0；flushable ingest（制造重叠 memtable 条件）Unflushed 持有到其 flush 完成后归零；external/excise 路径计量值正确；证据：测试。
  - `rule` TR-6.3：attach/link 失败注入后占用全量释放、无泄漏（-race + 计数器前后快照）；等待中 cancel/Close 语义正确；证据：测试输出。

## Task 7: Open 重启对账重建
- **Status**: `pending`
- **Priority**: medium
- **Depends On**: Task 6
- **Description**：
  - Open 中 replayWAL 完成后、开放写入前：遍历恢复的 flushable 队列，用与 Task3 相同的口径构建归属并直接以"初始占用"方式灌入 Manager 的 Unflushed 维度（不经 Admit、不产生等待）：flushableBatch 用其点键 offsets/tombstones/rangeKeys 精确归属；ingestedFlushable 用 UserKeyBounds+对象大小对所有重叠规则计全量；普通 memtable 取 skl 首末用户键与 inuseBytes，对重叠规则计全量（保守口径，写入注释）。
  - 重建登记同样挂接 flush 释放（复用 Task5），使 replay 后调度的 flush 完成时归零；Pending 三维初始为 0。
  - 配置在重开时仍为 nil 则不重建（能力关闭）。
- **Acceptance Criteria Addressed**: AC-13
- **Test Requirements**:
  - `rule` TR-7.1：制造含大 batch / flushable ingest / 普通 memtable 的未 flush 状态后重开，Metrics 按规则值与实际队列 totalBytes/inuseBytes 对账一致（单规则严格相等；跨规则 memtable 采用保守全量口径并在注释/测试中固定）；replay flush 完成后归零；证据：重开测试。
  - `rule` TR-7.2：崩溃产生半截 WAL record 时重开不产生幽灵占用；证据：既有 strictWALTail 设施下的测试。

## Task 8: 运行时配置热切换端到端
- **Status**: `pending`
- **Priority**: high
- **Depends On**: Task 4
- **Description**：
  - DB 侧在准入廉价点调用闭包并以不可变配置指针/版本比对发现切换；调用 Manager.Reconfigure；非法新配置：保留旧快照、Logger 记录并经事件通知（不 panic、不影响服务）。
  - 切换与等待/授予/Metrics 读取并发安全（核心已在 Task1 保证，端到端接线与事件在此完成）。
  - 静态构造器路径版本恒定，不产生重复编译开销。
- **Acceptance Criteria Addressed**: AC-10、FR-12
- **Test Requirements**:
  - `rule` TR-8.1：在途等待者/已授予者在切换后仍按旧快照等待/释放；新写入按新配置（拒绝/容量变化）被处理；retired 规则排空后从 Metrics 消失；证据：通道控制时机的确定性测试。
  - `rule` TR-8.2：非法新配置被拒绝、旧配置继续服务且有日志/事件；证据：测试。

## Task 9: Metrics、BatchCommitStats 与 EventListener 可观测面
- **Status**: `pending`
- **Priority**: medium
- **Depends On**: Task 8
- **Description**：
  - Metrics 增加 WriteQuota 子结构（按规则名：四维当前占用、等待者数、累计准入/拒绝/等待次数与累计等待时长、generation、retired 标记；拒绝可按维度粗分）；DB.Metrics() 在管理器快照上一致填充；zero 时不打印（FilterFn 惯例）。
  - BatchCommitStats 增加 QuotaWaitDuration，写路径计时填充。
  - EventListener 增加 WriteQuotaRejected（规则名、维度、策略）与 WriteQuotaReconfigured（generation 变化/失败）；同步 EnsureDefaults no-op、MakeLoggingEventListener、TeeEventListener；Info 实现 String/SafeFormatter。
- **Acceptance Criteria Addressed**: AC-17、AC-10（事件面）、AC-13（指标读面对账）
- **Test Requirements**:
  - `rule` TR-9.1：Metrics 黄金输出（testdata）覆盖空态/满额/retired；DB.Metrics 与核心快照一致；证据：datadriven 测试。
  - `rule` TR-9.2：拒绝与切换事件经普通/Tee/Logging listener 各触发一次且字段完整；QuotaWaitDuration 统计被等待过的提交、未等待为 0；证据：事件测试。

## Task 10: 综合并发/隔离/重试测试与性能门禁
- **Status**: `pending`
- **Priority**: high
- **Depends On**: Task 7, Task 8, Task 9
- **Description**：
  - 根包压力测试（可加 stress 构建标签/长模式）：多 goroutine 随机跨范围大小写、随机策略、随机 ctx 超时、随机切换配置、中途 Close；-race 下排空后四维全部归零、无 panic（预期关闭除外）、无死锁（总超时）。
  - 隔离测试：满额+等待+切换进行中并发 Get/NewIter/Snapshot/Checkpoint/Compact，断言不阻塞/不报错；评审+grep 确认这些路径与 WAL failover/flush/compaction/table-stats 路径无 Manager 调用。
  - 内部重试不重计费：WAL failover 次级目录故障注入场景（或既有测试设施）下累计准入次数=操作次数、占用不被物理重写放大；覆盖 Task 报告中枚举的重试点结论。
  - 性能门禁：禁用态 BenchmarkBatchSet 等与改动前基线对比（benchstat 或等价统计），确认无方向性回退；启用态单规则轻载开销给出来量级数据。
  - 全量质量门：`go build ./...`、`go vet ./...`、`gofmt/crlfmt`（按 Makefile）、`go test ./...`。
- **Acceptance Criteria Addressed**: AC-1、AC-9、AC-15、AC-16、AC-18
- **Test Requirements**:
  - `rule` TR-10.1：`go test -race -run 'WriteQuota|Quota' ./...` 连续多轮通过；压测排空归零断言稳定；证据：命令输出。
  - `rule` TR-10.2：读/后台路径在配额动荡期零影响；代码扫描证明零调用点；证据：测试 + 调用点清单。
  - `rule` TR-10.3：failover/重试场景单次计费；证据：测试输出。
  - `rule` TR-10.4：禁用态 bench 与基线无统计显著回退（同机多次运行对比），全量 go test ./... 通过；证据：bench/测试输出。
  - `rubric` TR-10.5：整体工程质量（对应 AC-18）；scale 1-5；anchors 见 AC-18；threshold >=4；证据：独立评审综合代码、测试、race/bench 证据。

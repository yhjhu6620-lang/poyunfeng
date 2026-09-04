明白，信息已经足够了。我已经把项目的核心代码都读完了（包括恰好拿到的 BatchUtils/LeoListenerManager 底层源码），下面基于**现有代码**给出完整的面试准备答案。

---

# 〇、先讲业务全景与设计逻辑（面试开场必背）

**业务是什么**：Semporna（仙本那）是 push 券活动的承接服务，"聚合优惠券"是其中一个活动玩法——一个活动页聚合多个坑位（slot）的优惠：**品类券**（算法决定面额）+ **省钱助手券**，由算法服务 Zambezi 做个性化排序。入口有 4 种：落地页（带 urlToken 防伪）、首页弹窗、消息渠道、团弹窗。

**两条核心链路**（`AggregatePromotionInnerServiceImpl`）：

```
查询链路 queryAggregatePromotion（只读、高频、RT 敏感）:
  来源打点 → urlToken校验(仅落地页) → 渠道映射(Leo) → 查活动资格缓存(Bat2, 1天)
  → ★BatchUtils 并行 4 路下游任务(统一超时200ms)
  → 实验通过? 风控通过? → 无缓存则调算法(Zambezi)并回写缓存
  → 组装券列表(已领状态/可领/不可领) → 返回

发券链路 sendAggregateCoupon（写操作、有资损风险）:
  ★@UserLock 用户级分布式锁 → 实验再校验 → 活动资格缓存校验 → 批次风控
  → 券详情校验(内部含"已领记录"判断, 非可领状态拒绝)
  → ★发券(bizId幂等键 → Stockholm) → 写领券记录缓存(Bat2, 1天) → 返回
```

**下游依赖**：Washington（实验大盐）、Moras（省钱助手）、Stockholm（券系统：批次风控/发券/券状态查询）、LocationIP（IP 定位）、Zambezi（算法）、Bat2（Redis 风格的 KV，做缓存 + 分布式锁）。

**三个亮点在架构中的定位**：BatchUtils 解决查询链路的 **RT 问题**；分布式锁+幂等解决发券链路的 **资损问题**；Leo 解决营销活动的 **敏捷性问题**。一句话总结：**读链路追求快（并行+超时+降级），写链路追求稳（锁+幂等+记录），配置追求活（热更新）**。

---

# 一、第一块：BatchUtils 并行编排 4 路 RPC

## 第一层：业务背景

**Q1：为什么需要同时调用 4 路下游 RPC？**

查询接口要组装出完整的活动页，需要 4 份互不依赖的数据（见 `queryAggregatePromotion` 步骤 5）：

| 任务 | 依赖 | 强/弱 | 用途 |
|---|---|---|---|
| `abResultTask` | Washington/AbTrigger | **强** | 5 层实验校验（消息大盐→内外部发券大盐→发券玩法大盐→活动实验→分渠道实验），决定用户**能不能看到活动** |
| `validBatchQueryTask` | Stockholm | **强** | 批次风控校验，返回 `validBatchSnMap`，决定**哪些批次可领** |
| `smaResultTask` | Moras | 弱 | 省钱助手资格，决定省钱助手坑位**展示与否** |
| `locationNameTask` | LocationIP | 弱 | IP 省份，纯**页面展示** |

关键点：这 4 路的入参（uid、渠道、IP、批次列表）在请求进来时就全部可用——批次列表来自 Leo 配置 `couponGroupConfig`，**不依赖任何前序调用的结果**，所以天然可以全部并行。这是"**能并行是因为做了依赖分析**"，不是硬凑的。

**Q2：串行调用会带来什么问题？**

- **RT 线性累加**：假设每路 RPC 30~80ms，串行就是 150~300ms+；并行后整体 RT ≈ 最慢那一路。这是 push 触达场景，用户点开落地页/弹窗，**RT 直接影响跳出率和转化率**。
- **吞吐放大恶化**：高频接口 RT 翻倍 → Tomcat/Dubbo 工作线程占用时间翻倍 → 同样机器数可承载 QPS 减半 → 高峰期线程池打满 → 雪崩。
- **木桶效应**：任何一路下游抖动（比如 IP 定位服务慢了），串行结构下会拖垮整个接口；并行 + 统一超时能把慢的隔离掉。

## 第二层：具体实现

**Q3：怎么用 BatchUtils 做并行编排？**

```java
Task<Boolean> abResultTask = Tasks.strongDependenceTask("abResultTask",
        () -> abTestAdapter.passAllAb(request));
Task<QuerySmaDiversionNoAlgoResponse> smaResultTask = Tasks.weakDependenceTask("smaResultTask",
        () -> smaInfoAdapter.querySmaInfo(context, saveMoneyAmount), null);
// validBatchQueryTask / locationNameTask 同理
int executeTime = getBatchUtilExecuteTime();   // Leo读取，默认200ms
batchUtils.run(executeTime, abResultTask, smaResultTask, validBatchQueryTask, locationNameTask);
```

底层机制（我读过 bronya-core 源码）：`BatchUtils.run(timeout, tasks...)` 内部把每个 Task 包成 `CompletableFuture.runAsync(callable, executor)`，然后 `CompletableFuture.allOf(...).get(timeoutInMs)` 统一等待。超时或异常后，对未完成任务调 `onFailure` 标记失败 → 结果回填 `defaultResult` → 最后**遍历检查强依赖任务，失败则抛 `TaskException`**。所以主线程的语义是：**最多等 200ms，要么全要，要么按强/弱规则止损**。

**Q4：强依赖和弱依赖怎么定义？失败后各怎么处理？**

定义标准不是"重不重要"，而是"**失败后主流程还能不能给出正确结论**"：
- **强依赖**（AB 实验、批次风控）：结果直接决定"放行/拒绝"。拿不到结果就不能贸然放行——放行一个该被实验拦住的用户 = 资损风险。所以失败 = 抛 `TaskException`，**快速失败，宁可这次请求报错，也不能给错结果**。
- **弱依赖**（省钱助手、IP 省份）：只影响展示细节。失败回填默认值 `null`，业务侧用 `Optional` 处理：省钱助手查不到 → 该坑位不渲染（`CouponQueryAdapter` 里 `saveMoneyAssistant == null` 直接 return null 跳过）；省份查不到 → 页面不显示地区。**降级不报错**。

## 第三层：稳定性

**Q5：为什么还需要统一超时和快速失败？各解决什么问题？**

两者解决的是不同维度的问题：
- **统一超时（200ms，Leo 可调）解决"木桶效应"**：并行后 RT = max(4 路)，如果一路 hang 住，并行就白做了。`allOf().get(200ms)` 给整个批次封顶，保证查询接口 RT 有上界，下游再慢也传染不到用户。
- **快速失败解决"故障传染"**：强依赖失败立刻抛异常终止，不让请求带着残缺数据继续往下走（比如实验结果缺失还继续查算法、调下游，浪费下游资源）；弱依赖失败立刻降级，不让用户等一个可有可无的数据。

补充深挖点：超时值为什么放 Leo？因为不同时期下游表现不同，大促前可以适当放宽、下游抖动时可以收紧，**不用发版就能调**。

**Q6：某一路 RPC 特别慢，会不会一直占用线程？**

**会占用，但有界**。关键细节：`get(timeout)` 超时只是主线程不等了，**并不会中断已经在执行的任务线程**（CompletableFuture 超时不 cancel），那个线程会继续跑到 RPC 自身超时才归还。所以防线程被拖死靠三道防线：
1. **每路 RPC 客户端自身有超时**（`RpcUtils.getResult` 统一封装，Dubbo consumer 超时兜底），保证线程最坏也只占用"RPC 超时时长"，不会永久挂死；
2. **独立线程池隔离**：慢任务占用的是 batchJob 线程池，不会传染到 Tomcat/Dubbo 主工作线程；
3. **监控**：BatchUtils 对失败任务打 `Cat.logEvent("failed_batch_run_task", taskName)`，某一路持续超时能第一时间在 Cat 上看到，配合 Leo 收紧超时或降级。

## 第四层：底层

**Q7：线程池怎么配置的？为什么？**

`SempornaCommonApplication` 里：

```java
Executor executor = new ThreadPoolExecutor(800, 800,
        0L, TimeUnit.MILLISECONDS, new SynchronousQueue<>(),
        new ThreadFactoryBuilder().setNameFormat("common-batchJob-pool-%d").build());
```

- **固定 800 线程（core=max）**：任务全是 IO 密集型（等 RPC 响应），线程数可以远超 CPU 核数；固定大小避免运行期扩容抖动，容量可预估。
- **SynchronousQueue（零容量）**：这是最关键的设计——**不排队**。没有空闲线程时提交直接 `RejectedExecutionException`，任务要么立刻有线程执行，要么立刻失败，**绝不堆积**。
- **800 怎么算的**：每请求 4 个并行任务、统一超时 200ms，单个线程 1 秒可执行 5 个任务，800 线程理论支撑 800×5÷4 = **1000 QPS** 的查询请求，覆盖峰值流量。
- **命名线程工厂**：出问题 dump 线程栈时能一眼认出是这个池子。

**Q8：高峰期队列堆积怎么避免拖垮服务？**

先亮出设计答案：**我们的池子根本没有队列**（SynchronousQueue），所以"堆积"这个状态在这个设计里不存在——这是有意为之。如果换成无界队列/大容量有界队列，堆积会导致：任务在队列里排队 → 实际执行时早已超过 200ms 的业务时限 → 结果回来也没用了（做了无效功）→ 线程被无效任务占满 → 上游看到 RT 飙升 → 重试 → 进一步堆积 → 雪崩。

所以防拖垮是组合拳：
1. **SynchronousQueue 快速拒绝**——压力直接顶回去，不做无效功；
2. **统一超时**保证已占用的线程在有限时间内归还；
3. **RPC 自身超时**兜底最坏情况；
4. **Cat 监控**任务失败率/拒绝率，配合 Leo 动态调整超时；
5. 极端流量下由上游限流/入口层挡量，而不是让服务内部默默排队等死。

---

# 二、Redis 分布式锁 + 幂等键 + 领券记录

（说明：项目里实际用的是 Bat2——公司自研的 Redis 风格 KV 存储，锁是 `Bat2DistLock`，语义等同 Redis SETNX + TTL，面试可直接说"Redis 原理的分布式锁"）

## 第一层：场景

**Q1：发券为什么会有重复领取问题？哪些情况触发？**

发券是**写操作、有资损**。重复领取的触发场景：
1. **用户侧并发**：狂点"领取"按钮、双击、客户端重试，同一 uid 的多个请求几乎同时到达；
2. **网络重试**：RPC 超时后框架/上游重试（Dubbo consumer 重试、网关重试）；
3. **时间差攻击**：查询时校验通过 ≠ 领取时还有资格——用户从看到页面到点击有时间差，期间缓存可能过期、实验可能切层、批次可能被风控下架；
4. **业务时序**：发券成功但响应丢失，客户端/上游再次发起。

**Q2：为什么需要用户级并发控制？**

因为"是否已领过"的判断（查领券记录/券状态）和"执行发券"之间存在**check-then-act 竞态窗口**：两个并发请求都查到"未领取"，然后都去发券 → 重复发放。锁的作用就是把同一用户的发券操作**串行化**，让 check-then-act 变成原子区间。注意粒度是**用户级**不是全局锁：不同用户互不阻塞，全局锁会把发券 QPS 压成 1，用户级锁只牺牲"同一用户的并发重复请求"——而这正是我们想拦的。

## 第二层：实现

**Q3：锁 key 怎么设计的？为什么粒度是 uid？**

```java
@UserLock(uid = "#request.uid", type = UserLockEnum.AGGREGATE_COUPON_SEND_COUPON, leaseTimeInMs = 5000L)
```
AOP 切面 `UserLocker` 用 **SpEL** 从方法入参解析 uid（`#request.uid`），拼 key：

```java
String lockKey = String.format("semporna:a:c:s:c:%d", uid);  // UserLockEnum
```

设计考量：
- **粒度 = uid**：并发重复领券只可能发生在同一用户身上，锁 uid 就够了；锁全局/锁批次会误伤无关用户，锁 batchSn 粒度太粗且不同坑位互斥没必要。
- **前缀带业务命名空间**（`semporna:a:c:s:c:`）：多业务共用一个 KV 集群时避免 key 冲突，也便于按前缀治理/排查。
- **注解 + AOP**：锁逻辑与业务代码解耦，业务方法保持纯粹；`UserLockEnum` 集中管理所有锁类型，将来加新锁场景只需加枚举 + 注解。
- **fail-fast 细节**：uid 解析失败（null 或 0）**直接抛 `SempornaBat2LockException`，不上锁绝不执行业务**——防止"锁失效后业务裸奔"导致重复发券。

**Q4：过期时间怎么设置？业务没执行完锁过期了怎么办？**

`bat2lock.run(lockKey, leaseTimeInMs=5000, waitTime=1000, callback)`：
- **leaseTime 5s**：锁自动过期时间（防死锁——万一持有锁的实例宕机，锁 5s 后自动释放，不会永久卡死其他请求）。5s 的依据：发券链路内部有实验校验 + 缓存查询 + 风控 RPC + 券详情查询 + 发券 RPC，正常几百 ms，5s 是留了 5~10 倍余量的保守值。
- **waitTime 1s**：抢锁等待上限，拿不到锁就失败返回，防止请求在锁上无限排队堆积。

锁提前过期的风险及应对（面试深挖点）：
1. **锁续期/看门狗**：标准解法是后台线程定期续期（如 Redisson watchdog），`Bat2DistLock` 内部有租约管理；
2. **即使锁真的提前过期**，还有两道兜底：**幂等键**（下游 Stockholm 按 bizId 判重，发不出第二张）+ **券状态校验**（发券前 `queryCouponGroupList` 查到已领记录则状态非 AVAILABLE，拒绝再发）；
3. 这就是设计哲学：**锁是性能/并发层面的第一道防线，不是正确性的唯一防线**——正确性由幂等最终兜底。

## 第三层：幂等

**Q5：已经有分布式锁了，为什么还需要业务幂等键？**

因为**两者防护的时间窗口不同**：
- 锁只管"**持锁期间**"的并发互斥，锁一释放，它的使命就结束了；
- 幂等键管"**任意时间**"的重复请求——锁释放后的重试、几秒后客户端重发、上游消息重投，这些请求相隔很远，锁早就不在了，但 bizId 相同 → 下游判重。

代码（`CouponSendAdapter.sendCoupon`）：

```java
String bizId = String.format("%s-%d-%d", SendCouponBizPrefEnum.AGGREGATE_PROMOTION_SLOT.getPref(),  // "semporna:a:p:s"
        targetBatch.getSlotId(), promotionCacheBO.getStartTime());
```

即 `semporna:a:p:s-{坑位ID}-{活动开始时间}`，传给 Stockholm 发券，下游按 **uid + scene + bizId** 联合判重。设计要点：
- **slotId**：同一活动内不同坑位是不同的券，各发各的，互不幂等干扰；
- **活动 startTime**：天然的活动周期标识——同一用户在同一活动周期内同一坑位，bizId 恒定 → 无论重试多少次，只发一张；下一个活动周期 startTime 变了，bizId 变了，可以再领（符合业务）；
- **幂等下沉到下游**：判重逻辑放在发券系统（数据强一致方），而不是本服务自己用缓存做判重（缓存不可靠，且多实例/多入口都会漏）。

**Q6：下游发券成功但 RPC 超时了，重试会不会重复发券？**

**不会，这正是幂等键存在的意义**。时序推演：
1. 第一次请求：Stockholm 已落库发券 → 响应还没回来，RPC 超时 → 本服务认为失败；
2. 重试（同 uid、同活动缓存 → 同 slotId、同 startTime → **同 bizId**）→ Stockholm 发现该 (uid, scene, bizId) 已发过 → 幂等返回已发结果/拒绝重复发放，**不会发第二张**；
3. 关键前提：**重试时活动缓存还在**（TTL 1 天，活动周期内基本必在），startTime 不变，bizId 稳定。如果缓存过期了，发券入口的活动资格校验会直接拒绝（"缓存中无活动资格记录"），同样发不出去。

可以再补一句完整链路的多层防线：锁拦并发 → 幂等键拦重试 → 券状态校验拦"已领再领" → 风控拦异常用户。

## 第四层：设计边界

**Q7：分布式锁、幂等键、领券记录缓存三者各自解决什么问题？**

| 机制 | 解决的问题 | 失效场景 | 角色 |
|---|---|---|---|
| **分布式锁**（`semporna:a:c:s:c:{uid}`，5s 租约） | **瞬时并发**：同一用户多个请求同时到达的竞态窗口，把 check-then-act 串行化 | 锁过期/锁服务故障 | 第一道防线（性能层） |
| **幂等键 bizId**（`semporna:a:p:s-{slotId}-{startTime}`） | **跨请求重复**：重试、重发、重投，无论间隔多久，同一业务语义只发一次 | 下游判重逻辑故障 | 最终兜底（正确性层） |
| **领券记录缓存**（`semporna:a:c:s:r:{uid}:{slotId}`，TTL 24h） | **业务状态**：给查询链路展示"已领取"状态、给发券链路提供"是否已领"的快速判断，且校验记录与活动周期/批次匹配（`CouponQueryAdapter` 里的三重过滤：时间在活动期内、batchSn 匹配） | 缓存丢失（可从 Stockholm 券状态反查） | 业务语义层（体验+前置拦截） |

一句话：**锁是时间维度的互斥，幂等是请求维度的去重，记录是业务维度的状态**。三者是纵深防御，不是互相替代。

**Q8：Redis（Bat2）挂了，发券链路怎么保证不出现严重重复发放？**

分两层回答——**先说设计取舍，再说兜底**：
1. **锁服务不可用时，发券链路选择不可用（fail-closed）而不是裸奔**：`UserLocker` 中 uid 解析失败直接抛异常；`bat2lock.run` 获取锁失败走 `onFailure` → 抛 `SempornaBat2LockException` → 请求失败。**宁可服务降级拒绝发券，也不冒重复发放的资损风险**——发券场景可用性 < 资损安全，这是主动的取舍。
2. **即使极端情况下锁完全失效**，重复发放仍被拦住：
   - **幂等键在 Stockholm 下游**，不依赖本地 Bat2——同一用户同活动同坑位的重复请求到下游仍被 bizId 判重；
   - **券状态实时校验**走 Stockholm（`queryCouponStatus`），已领过的券状态非 AVAILABLE，发券入口拒绝；
   - **批次风控**（`queryBatchSnByScene` 带 `checkRisk=true`）也在下游，异常流量会被拦。
3. 缓存挂了对**查询**链路的影响只是多穿透一次调算法（只读无资损），这也回答了"为什么查询不加锁"——**写操作有资损才配得上锁的成本，读操作并发最多浪费一次下游调用**。

---

# 三、Leo 动态配置

## 第一层：为什么

**Q1：为什么活动参数和任务超时放 Leo 而不写死在代码？**

营销活动的三个特性决定了必须热更新：
- **节奏快**：券批次（batchSn）、面额上限（maxAmount）、门槛（thresholdAmount）随大促/日常活动频繁更换，写死 = 每次调活动都要发版；
- **需要紧急止损**：发现某批次被羊毛党盯上、面额配错，**秒级改配置下线/修正**，发版流程（代码→测试→发布→扩容）根本来不及；
- **参数需要在线调优**：BatchUtils 超时时间（`AGGREGATE_PROMOTION_BATCH_UTIL_EXECUTE_TIME`，默认 200ms）这种参数，只有在线上真实流量下才知道最优值，放 Leo 可以边看监控边调。

**Q2：哪些参数需要频繁动态调整？**

看 `LeoConstants` + `SempornaCommonConfig`，分三类：
- **活动内容类**：`AGGREGATE_PROMOTION_COUPON_GROUP_CONFIG`（券组：坑位→批次列表→面额/门槛、Stockholm 发券场景）——**每次活动必改**；
- **运行参数类**：BatchUtils 并行超时时间、`METHOD_LOG_SAMPLE_RATE_CONFIG`（日志采样率）——**按线上表现调**；
- **映射/开关类**：`SECRET_CHANNEL_CONFIG`（加密渠道→真实渠道映射）、`URL_CONFIG`、`INTEGRATION_MOCK_RESULT_CONFIG`（下游 mock，测试用）——**按需随时改**。

## 第二层：实现

**Q3：配置变更后应用怎么实时感知并更新？**

注册-监听-回调三段式（`SempornaCommonConfig` + bronya 的 `LeoListenerManager`）：
1. **注册**：`@PostConstruct` 里 `LeoListenerManager.getInstance().register(key, listener)`。注意 register 内部会**立即用当前值调一次 `listener.onChange()`**——保证启动时就完成初始化，不依赖"等一次变更"；
2. **监听**：`LeoListenerManager` 是双重检查单例，构造时向 Leo 客户端的 `ConfigCache.addChange((key, value) -> ...)` 注册**全局变更回调**——Leo 客户端与配置平台保持长连接，推送到达即触发；
3. **分发**：回调里按 key 从 `leoListenersMap`（ConcurrentMap + CopyOnWriteArrayList）找到该 key 的所有 listener，逐个执行 `onChange(value)`。

单个配置的更新逻辑（以券组配置为例）：
```java
LeoListenerManager.getInstance().register(AGGREGATE_PROMOTION_COUPON_GROUP_CONFIG, value -> {
    AggregatePromotionCouponGroupConfig tempConfig =        // ① 反序列化到局部变量
        Optional.ofNullable(SempornaJsonUtils.fromSnakeJson(value, ...))
            .orElseThrow(...);                               // ② 解析失败抛异常
    if (!tempConfig.isValid()) throw ...;                   // ③ 业务合法性校验
    aggregatePromotionCouponGroupConfig =
        AggregatePromotionCouponGroupConfig.buildImmutableConfig(tempConfig); // ④ 构建不可变对象后一次性替换引用
});
```

**Q4：更新过程怎么保证线程安全？**

三重保障，缺一不可：
1. **不可变对象**：`buildImmutableConfig` 把内部 List 全部转成 `ImmutableList`（Guava），字段全 final——对象发布后**不可能被半改**；
2. **"先构建、后发布"的引用替换**：所有解析/校验都在局部变量 `tempConfig` 上进行，**校验失败抛异常，成员变量保持旧引用**（旧配置继续服务）；成功后一次原子引用赋值。读线程任何时刻拿到的引用，要么完整指向旧配置、要么完整指向新配置，**不存在中间态**；
3. **写端无并发**：配置变更回调由 Leo 客户端串行触发，同一个 key 的 listener 天然串行执行，不存在两个线程同时写同一配置字段的问题。

## 第三层：风险

**Q5：配置推错了会不会直接影响线上活动？**

分两类错误：
- **格式/结构错误**（JSON 解析失败、字段缺失、slotId 不在枚举里、batchSn 重复、maxAmount≤0）：被 `isValid()` 全量校验拦住，**抛异常、不生效、旧配置继续跑**。校验很细：slotId 必须是合法聚合券坑位枚举、slotId/batchSn 全局去重、面额门槛必须为正——把"半份配置"挡在门外；
- **语义错误**（比如面额本来想配 10 元配成 100 元）**：代码无法 100% 拦截**，靠管理手段：Leo 平台的审批/灰度发布机制、变更前 diff review、出问题秒级回滚（回滚也是一次配置推送）。另外代码里有限损设计——`maxAmount` 在 `CouponQueryAdapter` 中校验"算法给的面额 ≤ 配置上限"，配置错误的影响被限制在配置声明范围内。

**Q6：Leo 不可用时服务还能不能运行？**

**能**。关键在架构上**运行期读的是内存引用，不是 Leo**：
1. 启动时 `register` 内部已经用 `LeoUtils.getProperty(key)` 把配置**加载进内存**（本地快照），之后业务代码全部走 `sempornaCommonConfig.getXxx()` 读内存对象；
2. Leo 挂了只是**收不到变更推送**，服务继续用最后一次生效的配置运行——配置中心不可用 ≠ 服务不可用，这是配置中心客户端的标准设计（本地容灾快照）；
3. 唯一受影响的是"挂掉期间推送的变更"会在 Leo 恢复后补推（客户端重连对账）。

## 第四层：架构理解

**Q7：什么数据放配置中心，什么放数据库？**

判断维度是**变更主体、事务需求、数据量**：

| 维度 | 配置中心（Leo） | 数据库 |
|---|---|---|
| 谁改 | 运营/开发改**规则** | 用户行为产生**事实** |
| 典型数据 | 参数、开关、开关、映射表、券批次规则 | 领券流水、订单、用户资产 |
| 事务 | 不需要 | 强事务/持久化 |
| 变更频率 | 高、需要热生效 | 随业务写入 |
| 数据量 | KB~MB 级 | 无上限 |

本项目对照：券组配置（规则）→ Leo；用户的活动资格、领券记录（事实）→ 但这两个是**缓存性质的状态**，放 Bat2（KV），真正的券资产数据在 Stockholm 的数据库里。所以完整分层是：**规则 → 配置中心；状态缓存 → KV；事实 → 数据库**。

**Q8：为什么"活动策略热更新"对营销业务重要？**

四个字：**快、密、险、省**：
- **快**：营销节奏以小时计（大促预热、整点场、突发热点），策略调整必须分钟级生效，发版周期是天级；
- **密**：AB 实验迭代密集，实验参数、分流策略天天变；
- **险**：营销 = 直接发钱，是资损高危区。发现刷券、面额配错，**热更新是唯一的紧急刹车**（改配置下线批次/调面额，秒级止血），等发版止血，损失已经扩大几个数量级；
- **省**：一次活动调整如果走发版：开发→测试→发布→全量验证，人力成本高且发布本身有风险；改配置：改一行 JSON、审批、推送，全程无代码变更风险。

结合本项目收尾：聚合优惠券的券批次、面额上限、发券场景、并行超时全部在 Leo 上——**运营可以自己换活动，开发可以自己调参数，出事可以秒级回滚**，这就是这个设计对业务的全部价值。

---

# 附：面试防翻车提示

1. **别说错锁的实现**：项目用的是 Bat2（自研 Redis 风格 KV）+ `Bat2DistLock`，面试时说"原理等同 Redis SETNX+TTL 的分布式锁"最稳妥；
2. **强依赖失败的真实行为**：BatchUtils 对强依赖失败是**抛 `TaskException`**（请求异常终止），代码里 `BooleanUtils.isNotTrue(result)` 的 null 判断是防御式编程，主要处理"任务成功但返回 false"（命中对照组）的情况，别混为一谈；
3. **超时不中断任务**这个细节（`get(timeout)` 只是不等了，任务线程还在跑）是第 6 问的高分点，主动讲出来；
4. **线程池无队列**（SynchronousQueue）是第 8 问的题眼——"我们不是解决堆积，而是从设计上消灭堆积"；
5. 三个亮点的收束逻辑：**读链路快（并行编排）、写链路稳（锁+幂等纵深防御）、运营链路活（配置热更新）**——面试官问"这三个点怎么串起来"时用这句话。

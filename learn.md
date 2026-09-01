### 四、本职工作及公司改善建议

建议进一步探索 AI 工具与业务、技术文档及常见问题经验的结合，通过统一沉淀和智能检索，帮助新人更快速地理解业务、熟悉项目并定位常见问题。

 负责认证审核与奖励发放核心链路，基于MySQL 事务+ version 乐观锁保障审核、奖励状态并发更新一致性。
• 负责职业认证机审链路，接入LLM OCR 与审核系统完成机审、人审流转，并通过Leo 动态控制机审策略。
• 基于Tiger（Kafka）实现审核、打款结果异步消费与失败重试，结合状态校验保障消费幂等，避免重复打款。


基于 Tiger（Kafka）的审核/打款结果异步消费与幂等保障 —— 代码定位与技术详解
一、代码定位地图
#
职责
文件路径
1
Tiger 消费者抽象基类（重试核心）
maldives-task/.../task/tasks/AbstractTigerConsumer.java
2
审核结果异步消费者
maldives-task/.../task/tasks/PushAuditResultReceiptTask.java
3
打款结果异步消费者
maldives-task/.../task/tasks/TransferResultReceiptConsumer.java
4
业务级重试消费者（第二层重试）
maldives-task/.../task/tasks/SimpleRetryService.java
5
核心业务处理（幂等校验）
maldives-logic/.../profession/impl/ProfessionCertificationMainServiceImpl.java（L795 审核结果、L935 打款结果）
6
状态流转服务（乐观锁）
maldives-logic/.../profession/impl/ProfessionCertificationOperateServiceImpl.java
7
状态机 + 条件更新 SQL
maldives-repository/.../mybatis/impl/ProfessionCertificationRewardRecordRepositoryImpl.java
8
打款执行 Job（防重复打款核心）
maldives-logic/.../retry/retryable_job/SendProfessionCertificationRewardJob.java
9
重试框架（Worker/Producer/Consumer/Context）
maldives-logic/.../retry/SimpleRetry*.java
10
可重试异常语义
maldives-common/.../exception/MaldivesRetryException.java
11
状态枚举（奖励 8 态 / 审核 4 态）
maldives-contract/.../enums/ProfessionCertificationRewardStatusEnum.java 等
12
分布式锁 AOP
maldives-logic/.../aop/lock/UserLock.java + UserLocker.java

二、整体架构与消息流转    
【上游系统】                       【Tiger (Kafka) 消息中间件】              【maldives-task 消费进程】
┌──────────────┐   审核结果回执   ┌──────────────────────────┐   ┌────────────────────────────────┐
│ Chess 审核平台 │ ──────────────▶ │ push-audit-result-       │──▶│ PushAuditResultReceiptTask      │
│ (人审/机审)   │                │ receipt-topic             │   │  └▶ handleProfessionCertification│
└──────────────┘                 └──────────────────────────┘   │     AuditResult()  ← 状态校验幂等 │
                                                                └────────────────────────────────┘
┌──────────────┐   打款结果回执   ┌──────────────────────────┐   ┌────────────────────────────────┐
│ Koln 打款平台 │ ──────────────▶ │ koln-transfer-result      │──▶│ TransferResultReceiptConsumer   │
│ (微信/多多零钱)│                └──────────────────────────┘   │  └▶ handleTransferReceiptResult() │
└──────────────┘                                                 │     ← 状态校验幂等 + 金额校验    │
      ▲                                                           └────────────────────────────────┘
      │ sendTransfer (发起打款)                                                
      │                                                           ┌────────────────────────────────┐
      └───────────────────────────────────────────────────────────│ SendProfessionCertificationReward │
                                                                  │ Job (分布式锁+状态机防重复打款)    │
   同步执行失败 ──▶ 发送重试事件 ──▶ maldives-simple-retry topic ──▶ SimpleRetryService 消费重试  │
   (第二层业务级重试)                                             └────────────────────────────────┘
    
  

三、技术细节与代码实现逻辑
3.1 第一层：Tiger 消费框架的重试机制（AbstractTigerConsumer）    
// maldives-task/.../AbstractTigerConsumer.java
TigerConcurrentConsumer<String, String> tigerConcurrentConsumer = TigerClientFactory
    .createConcurrentConsumerBuilder(consumerGroup())          // 每个消费者独立消费组
    .bootstrapWithTopic(topic())
    .topicDelayConsumeTimeMsMap(Collections.singletonMap(topic(), delayConsumeTime())) // 支持延迟消费
    .enableConsumeRetry(true)                                  // ★ 开启消费重试
    .maxReconsumeTimes(maxReconsumeTimes())                    // ★ 最大重试 3 次
    .reconsumeDelayIntervalConfig(reconsumeDelayIntervalConfig()) // ★ 重试间隔 "15-s,1-m,5-m"
    .topicCheckTimeout(60 * 1000L)
    .build();
    
  消息监听器的失败重试语义：    
private TigerMessageListenerConcurrently<String, String> getMsgListener() {
    return (msgList, context) -> {
        for (ConsumerRecord<String, String> consumerRecord : msgList) {
            String value = consumerRecord.value();
            try {
                Cat.logEvent("tiger_msg", topic());           // CAT 监控埋点
                Cat.logMetricForCount("tiger_msg_" + topic());
                dealMsg(value);
            } catch (Exception e) {
                log.error("{} processor thread exception, msg:{}", topic(), value, e);
                return ConsumeConcurrentlyStatus.RECONSUME_LATER; // ★ 异常→稍后重新消费（触发重试）
            }
        }
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;        // 全部成功才 ACK
    };
}
    
  关键设计点：
- RECONSUME_LATER：单条消息处理异常即返回，整批不 ACK，Tiger 按 15秒 → 1分钟 → 5分钟 的梯度延迟重新投递，最多 3 次；
- CONSUME_SUCCESS：仅当批次内全部消息处理成功才提交 offset，保证**至少一次（At-Least-Once）**语义——这是后续必须做幂等的前提；
- 模板方法模式：子类只需实现 topic()、consumerGroup()、dealMsg()，重试策略默认 3 次 / 15s-1m-5m，可按需覆写。
3.2 审核结果消费（PushAuditResultReceiptTask）    
// topic: push-audit-result-receipt-topic  group: maldivesTaskPushAuditResultReceiptConsumerGroup
protected void dealMsg(String value) {
    AuditResultReceiptMessageDTO auditResultReceiptMsg = MaldivesJsonUtils.fromJson(value, AuditResultReceiptMessageDTO.class);
    if (invalidMessage(auditResultReceiptMsg)) {
        throw new MaldivesRetryException("invalid message:" + value);  // ★ 可重试异常
    }
    if (AuditTypeEnum.CAREER_CERTIFICATION.getType().equals(auditResultReceiptMsg.getType())) {
        professionCertificationMainService.handleProfessionCertificationAuditResult(auditResultReceiptMsg);
    }
}
    
  消息过滤策略：只处理 CAREER_CERTIFICATION（职业认证）类型，其他审核类型静默跳过——避免无关消息异常导致无意义重试。
3.3 打款结果消费（TransferResultReceiptConsumer）    
// topic: koln-transfer-result  group: maldivesTransferResultReceiptConsumerGroup
protected void dealMsg(String value) {
    TransferKolnTigerResultVO transferResult = MaldivesJsonUtils.fromJson(value, TransferKolnTigerResultVO.class);
    if (invalidMessage(transferResult)) {
        throw new MaldivesRetryException("invalid message:" + value);   // 反序列化失败/无 uid → 重试
    }
    if (BooleanUtils.isNotTrue(transferResult.getIsEnd())) {            // ★ 只处理终态回执
        Cat.logEvent(CAT_TYPE, "not end status");
        return;                                                          // 非终态直接 ACK 跳过
    }
    if (!Objects.equals(KolnActivityConstants.PROFESSION_CERTIFICATION_REWARD, transferResult.getActivityType())) {
        Cat.logEvent(CAT_TYPE, "not target activity");                   // ★ 只处理 "prof_cert" 活动
        return;
    }
    professionCertificationMainService.handleTransferReceiptResult(transferResult);
}
    
  关键设计点：Koln 打款平台是多业务共用的打款通道，回执 topic 是全站混流的。消费者通过双重过滤（isEnd 终态 + activityType=prof_cert）把无关消息挡在业务逻辑之外，且跳过即 ACK，不进入重试队列。
3.4 幂等核心①：打款回执处理的状态校验（handleTransferReceiptResult，L935）    
public void handleTransferReceiptResult(TransferKolnTigerResultVO transferResult) {
    // 1. 参数硬校验：金额/业务单号/打款订单号缺失 → 抛 MaldivesUnexpectedException（不可重试，人工排查）
    if (Objects.isNull(transferResult.getAmount()) || StringUtils.isBlank(transferResult.getActivitySn()) 
        || StringUtils.isBlank(transferResult.getTransferOrderId())) {
        throw new MaldivesUnexpectedException("参数异常");
    }
    Long professionCertificationId = Long.valueOf(transferResult.getActivitySn());

    // 2. 查询本地奖励记录
    ProfessionCertificationRewardRecordBO rewardBO = professionCertificationRewardRecordRepository
        .queryProfessionCertificationRewardRecordByUidAndCertIdAndType(
            transferResult.getUid(), professionCertificationId, ProfessionCertificationRewardTypeEnum.CASH.getCode());
    if (rewardBO == null) {
        throw new MaldivesUnexpectedException("记录不存在");   // 本地无记录 = 系统性不一致 → 告警排查
    }

    // ★★★ 幂等闸门：状态不是 WITHDRAWING(提现中) → 判定为重复消费，直接返回（ACK 成功）
    if (!ProfessionCertificationRewardStatusEnum.WITHDRAWING.getCode().equals(rewardBO.getStatus())) {
        Cat.logEvent(CAT_PROFESSION_CERTIFICATION_REWARD_RECEIPT, "status illegal: " + rewardBO.getStatus());
        return;   // 已是终态(WITHDRAW_SUCCESS/WITHDRAW_FAILED)，重复消息安全落地
    }

    // 3. 资金安全校验：回执金额 ≠ 本地金额 → 不可重试异常，人工介入
    if (!Objects.equals(rewardBO.getAmount(), transferResult.getAmount())) {
        throw new MaldivesUnexpectedException("金额不匹配");
    }

    // 4. 按打款订单终态更新本地记录
    if (Objects.equals(transferResult.getTransferOrderStatus(), TransferOrderStatusEnum.SUCCESS.getCode())) {
        updateSuccess = professionCertificationOperateService.updateProfessionCertificationRewardToWithdrawSuccess(
            rewardBO, transferResult.getThirdSuccessTime() * 1000L, transferResult.getTransferOrderId(),
            TRANSFER_TYPE_TO_DEDUCT_TYPE_MAP.get(transferResult.getTransferType()));
    } else if (Objects.equals(transferResult.getTransferOrderStatus(), TransferOrderStatusEnum.CLOSED.getCode())) {
        updateSuccess = professionCertificationOperateService.updateProfessionCertificationRewardToWithdrawFailed(
            rewardBO, transferResult.getTransferOrderId());
    } else {
        throw new MaldivesUnexpectedException("invalid transferOrderStatus: ...");
    }
    // 5. 更新失败（如并发冲突）→ 抛 MaldivesRetryException 交给 Tiger 重试
    if (!updateSuccess) {
        throw new MaldivesRetryException("updateProfessionCertificationRewardFinalStatus failed");
    }
}
    
  幂等实现的精髓在这里：状态不是 WITHDRAWING 就直接 return。因为 Tiger 是 At-Least-Once 语义，同一条打款成功回执可能被投递多次。第一次消费把状态推进到 WITHDRAW_SUCCESS（终态）后，后续重复消息在状态校验这关被直接吞掉——既不会重复更新，也不会报错阻塞消费。
3.5 幂等核心②：审核回执处理的状态校验（handleProfessionCertificationAuditResult，L795）    
public void handleProfessionCertificationAuditResult(AuditResultReceiptMessageDTO auditResultReceipt) {
    // 1. 类型/必填字段校验（无效消息抛异常触发重试，防止消息乱序导致误判）
    ...
    // 2. ★ 幂等闸门：认证记录不是"审核中(AUDITING)"状态 → 重复消费，直接返回
    ProfessionCertificationRecordBO certificationRecordBO = ...queryByUidAndCertId(uid, professionCertificationId);
    if (Objects.isNull(certificationRecordBO)) { return; }        // 无记录 → 跳过
    if (!ProfessionCertificationAuditStatusEnum.isAuditing(certificationRecordBO.getStatus())) {
        Cat.logEvent(CAT_PROFESSION_CERTIFICATION_AUDIT, "record_status_invalid:" + ...);
        return;   // 已是 AUDIT_PASS / AUDIT_FAILED → 重复消息安全落地
    }
    // 3. 审核通过分支：先过黑名单检查（拉黑用户禁止通过，抛不可重试异常）
    // 4. CAS 式更新：带 beforeVersion 乐观锁更新状态
    boolean success = professionCertificationOperateService.updateProfessionCertificationAuditResult(
        uid, professionCertificationId, auditResultBO);
    if (!success) {
        throw new MaldivesRetryException("updateProfessionCertificationAuditResult fail|..."); // 冲突→重试
    }
}
    
  双重幂等防护：先做状态前置校验（软校验，快速跳过重复消息），再做数据库乐观锁条件更新（硬校验，即使并发窗口内有两条相同消息同时通过软校验，也仅有一条能更新成功，另一条 effectRows=0 → 抛 MaldivesRetryException → 重试后再次进入时被软校验拦截）。
3.6 幂等核心③：数据库层状态机 + 乐观锁（Repository 层）    
// ProfessionCertificationRewardRecordRepositoryImpl.java —— 状态机定义
private static final Map<Integer, Set<Integer>> NEXT_STATUS_MAP = ImmutableMap.builder()
    .put(CERTIFICATION_AUDIT,  of(AUDIT_FAILED, CAN_WITHDRAW, CANCEL_CERTIFICATION_NO_REWARD, MACHINE_AUDIT_FAILED))
    .put(CAN_WITHDRAW,         of(WITHDRAWING, CANCEL_CERTIFICATION_NO_REWARD))
    .put(WITHDRAWING,          of(WITHDRAW_SUCCESS, WITHDRAW_FAILED))   // ★ 提现中只能到两个终态
    .put(MACHINE_AUDIT_FAILED, of(CERTIFICATION_AUDIT))                  // ★ 机审失败可申诉回退
    .build();

// 条件更新：内存状态机校验 + SQL WHERE 双保险
public int updateProfessionCertificationRewardRecord(BO recordBO, Integer beforeStatus, Long beforeVersion) {
    // 内存层：校验 (beforeStatus → newStatus) 是合法状态机边 + version 严格 +1
    if (NEXT_STATUS_MAP.get(beforeStatus) == null
        || !NEXT_STATUS_MAP.get(beforeStatus).contains(recordBO.getStatus())
        || !Objects.equals(beforeVersion + 1, recordBO.getVersion())) {
        throw new MaldivesDatabaseUnexpectedException("invalid params");
    }
    // SQL 层：UPDATE ... WHERE uid=? AND certId=? AND rewardType=? AND status=beforeStatus AND version=beforeVersion
    example.createCriteria()
        .andUidEqualTo(record.getUid())
        .andProfessionCertificationIdEqualTo(record.getProfessionCertificationId())
        .andRewardTypeEqualTo(record.getRewardType())
        .andStatusEqualTo(beforeStatus)          // ★ 状态前置条件
        .andVersionEqualTo(beforeVersion);       // ★ 乐观锁
    return mapper.updateByExampleSelective(record, example);
}
    
  三道防线叠加：
1. 内存状态机（NEXT_STATUS_MAP）：非法状态跳转直接拒绝，防御编程；
2. SQL status = beforeStatus：即使应用层校验被绕过（如并发），数据库层保证只有处于预期前置状态的行才会被更新；
3. SQL version = beforeVersion：乐观锁，并发更新同一行时只有第一个生效，第二个 effectRows=0。
3.7 幂等核心④：打款发起侧的防重复打款（SendProfessionCertificationRewardJob）    
public ProfessionCertificationRewardSendResultBO sendProfessionCertificationReward(Context context) {
    String lockKey = String.format(Bat2Constants.SEND_PROFESSION_CERTIFICATION_REWARD_LOCK, uid);
    bat2DistLock.run(lockKey, 5000L, () -> {                     // ★ 分布式锁：同一用户串行化
        ProfessionCertificationRewardRecordBO rewardBO = ...query(uid, certId, CASH);
        // ★ 已打款成功 → 直接返回，绝不二次打款
        if (WITHDRAW_SUCCESS.equals(rewardBO.getStatus())) { Cat.logEvent(CAT, "already_send"); return; }
        // ★ 已打款失败（终态）→ 直接返回
        if (WITHDRAW_FAILED.equals(rewardBO.getStatus()))  { Cat.logEvent(CAT, "already_send_fail"); return; }
        // ★ 仅允许 待提现/提现中 两种状态继续
        if (!CAN_WITHDRAW.equals(status) && !WITHDRAWING.equals(status)) {
            throw new MaldivesUnexpectedException("invalid_status");
        }
        // ★ 先占位：CAN_WITHDRAW → WITHDRAWING（乐观锁），打款前先把状态推进到"提现中"
        if (CAN_WITHDRAW.equals(rewardBO.getStatus())) {
            boolean ok = operateService.updateProfessionCertificationRewardToWithdrawing(rewardBO, ...);
            if (!ok) { throw new MaldivesRetryException("updateProfessionCertificationRewardToWithdrawing failed"); }
        }
        // 金额边界校验（防配置错误导致超额打款）
        if (!isRewardAmountValid(finalCashAmount)) { throw new MaldivesUnexpectedException("illegal cash amount"); }
        // 发送前置打款消息（财务离线对账用）+ 调用 Koln 打款
        SendTransferResponse resp = kolnIntegration.sendTransfer(...);
        if (isNotTrue(resp.getSuccess())) {
            if (Boolean.FALSE.equals(resp.getCanRetry())) {
                // ★ 明确不可重试 → 落终态 WITHDRAW_FAILED，防止后续再次发起打款
                updateProfessionCertificationRewardToWithdrawFailed(rewardBO, null);
            } else {
                throw new CallKolnRpcException("transfer_failed");  // → 进入第二层重试
            }
        }
    });
}
    
  "先占位、后打款"模式是避免重复打款的关键：发起打款前先把状态从 CAN_WITHDRAW CAS 到 WITHDRAWING，此后任何重复请求（用户连点、消息重投）都会被状态校验挡住。真正的打款结果由 koln-transfer-result 回执异步驱动 WITHDRAWING → 终态。
3.8 第二层：业务级重试框架（SimpleRetry）    
// SimpleRetryWorker —— 同步执行失败自动投递重试事件
public R execute(T request) {
    try {
        Response<R> response = retryableJob.execute(request);
        if (response != null && response.isSuccess()) { return response.getResult(); }
        throw new InstantExecuteException();
    } catch (SimpleRetryExpectedException e) {
        throw e;                              // 预期内业务失败：不重试
    } catch (Exception e) {
        simpleRetryEventProducer.send(request);  // ★ 非预期失败：发 Tiger 重试事件（syncSendWithRetry 重试2次）
        throw e;
    }
}
    
      
// SimpleRetryService —— 独立消费 maldives-simple-retry topic，梯度重试 1s/3s/10s × 3 次
concurrentConsumer.registerMessageListener((records, context) -> {
    for (ConsumerRecord<String, String> record : records) {
        SimpleRetryEvent event = fromJson(record.value());
        SimpleRetryEventConsumerImpl<?> consumer = simpleRetryContext.getConsumerByOpType(event.getOpType());
        consumer.consume(event.getRequestJson());   // 内部调 worker.retryExecute()，复用同一套幂等逻辑
        ...
        return ConsumeConcurrentlyStatus.RECONSUME_LATER;  // 失败 → Tiger 重试
    }
    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
});
    
  两层重试的分工：
层级
载体
策略
适用场景
第一层
Tiger 消费框架（AbstractTigerConsumer）
3 次，15s/1m/5m
消费回执消息时的瞬时异常（DB 抖动、并发冲突）
第二层
业务重试 topic（maldives-simple-retry）
3 次，1s/3s/10s
发起打款等同步操作失败后的异步补偿
第二层重试消费时调用的是同一个 SendProfessionCertificationRewardJob，天然复用分布式锁 + 状态机校验，重试本身也是幂等的。
3.9 异常分类驱动重试决策
异常
语义
Tiger 行为
典型场景
MaldivesRetryException
可重试
RECONSUME_LATER 梯度重试
消息体暂时无法解析、DB 更新冲突
MaldivesUnexpectedException
不可预期
同样抛出→重试 3 次→死信/告警
状态非法、金额不匹配、记录不存在
正常 return
业务跳过
CONSUME_SUCCESS ACK
非目标活动、非终态、重复消费
注意：MaldivesUnexpectedException 场景（如金额不匹配）抛出后也会走 Tiger 重试，但由于问题不会自愈，重试 3 次耗尽后进入死信，配合 error 日志 + CAT 告警人工介入——这是"宁可报警也不静默吞掉资金异常"的设计取向。

四、业务相关特点
4.1 业务背景：职业认证现金奖励（Maldives = 马尔代夫项目）
这套代码服务于**"职业认证 C 端展示标签"业务：用户完成职业认证（如提交资质材料）→ 平台审核（机审 + 人审，由 Chess 审核平台承接）→ 审核通过后发放现金奖励**（微信零钱 / 多多零钱两种到账方式，由 Koln 营销资金平台执行打款）。
4.2 状态机驱动的奖励生命周期    
认证审核中(1) ──人审通过──▶ 待提现(3) ──用户领取──▶ 提现中(4) ──打款成功──▶ 提现成功(6)【终态】
      │                        │                     └─打款关闭──▶ 提现失败(5)【终态】
      ├──人审驳回──▶ 审核失败(2)【终态】  │
      ├──机审驳回──▶ 机审失败(8) ──申诉成功──▶ 回到认证审核中(1)
      └──取消认证──▶ 取消认证不可领取(7)【终态】
    
  
- 审核回执消费驱动左半段（1 → 2/3/8），同时联动更新认证记录状态与奖励记录状态（一个事务内）；
- 打款回执消费驱动右半段（4 → 5/6）；
- 每次状态变更都双写操作流水（ProfessionCertificationOperateLog），审核通过还会追加"获取身份标识"流水（时间戳 +1 保证顺序），满足审计合规诉求。
4.3 资金安全的四重防线（避免重复打款）
1. 入口分布式锁：@UserLock（Bat2DistLock AOP）+ Job 内二次加锁，同一 uid 的领奖请求串行化；
2. 状态前置校验：WITHDRAW_SUCCESS/WITHDRAW_FAILED（终态）直接短路返回，"already_send" 打点监控；
3. 先占位后打款：发起打款前 CAS 推进 CAN_WITHDRAW → WITHDRAWING，占位失败即说明有并发请求抢先，本请求放弃；
4. 回执金额对账：打款回执金额与本地记录金额强校验，不一致立即抛异常告警，绝不带病更新状态。
4.4 可观测性建设
- 全链路 CAT 埋点：每个消费分支（not end status、not target activity、status illegal、already_send、audit_pass、update_success 等）都有事件打点，可量化重复消费率、幂等拦截次数、审核通过率；
- 前置打款消息（sendPrePayRecordWithKoln）专供财务离线对账，与回执结果形成资金双向核对；
- 消费入口统一 log.info 原始消息体，异常时 log.error 带全量上下文，便于重放排查。
4.5 工程化细节
- @Profile(NOT_TEST)：所有 Tiger 消费者仅在非测试环境启动，避免测试流量误消费生产消息；
- 多应用分工：SimpleRetryContext 按 appName 区分——maldives-api 作为重试事件生产者，maldives-task 作为消费者（task 应用不建 producer，api 应用不建 consumer Bean），职责物理隔离；
- 模板方法 + 注册中心：AbstractTigerConsumer 收敛消费样板代码，SimpleRetryContext.register() 以 opType 为键注册 Worker/Consumer，新增重试业务只需实现 RetryableJob 接口；
- 黑名单熔断：审核通过分支内置 ProfessionCertificationBlackListConfig 时间点检查，拉黑用户的审核回执即使上游误判通过也会被本地拦截（"can not audit pass after black"）。
4.6 设计模式总结
模式
应用点
模板方法
AbstractTigerConsumer（topic/group/dealMsg 抽象）
状态机
NEXT_STATUS_MAP + 两个状态枚举
CAS 乐观锁
version + beforeStatus 条件更新
策略注册
SimpleRetryContext opType → Worker/Consumer
AOP
@UserLock 分布式锁切面
最终一致性
回执驱动 + 两层重试 + 幂等落地

一句话总结：该逻辑以 AbstractTigerConsumer 为消费骨架，通过 PushAuditResultReceiptTask（topic：push-audit-result-receipt-topic）与 TransferResultReceiptConsumer（topic：koln-transfer-result）异步消费审核/打款回执；失败时依托 Tiger 的 RECONSUME_LATER + 梯度延迟（15s/1m/5m×3）实现第一层重试，同步打款失败再经 maldives-simple-retry topic 走第二层业务补偿重试；幂等则由"状态前置校验（软）+ 状态机条件更新（硬）+ version 乐观锁（底）+ 分布式锁（入口）"四层防护共同保障，确保 Kafka At-Least-Once 语义下的重复消息不会导致重复打款或状态错乱。

我已经完成了对整个项目的深度扫描。现在基于对代码的完整理解，为你详细回答所有面试问题。

---

# 📋 项目全貌(先讲业务，再讲技术)

**业务定位**：maldives 是消息营销线下的**职业认证服务**。用户(如医生、教师、律师等职业从业者)提交职业证明材料，通过审核后获得**职业身份标识**(C端展示)，同时可获得**现金奖励**(用户主动领取，经打款系统打到微信零钱/多多钱包)。

**核心表**(分库分表，均带 `version` 乐观锁字段)：
- `profession_certification_record` 认证记录表，uk(uid, cert_id)
- `profession_certification_reward_record` 奖励记录表，uk(uid, cert_id, reward_type)
- `profession_certification_operate_log` 操作流水表

**完整链路**(面试开场白建议)：

```
用户提交认证 ──► [事务]插入认证记录(AUDITING)+奖励记录(CERTIFICATION_AUDIT)+操作流水+初始化Chess审核单
     │
     ▼ (异步)
LLM OCR机审 ──► 识别失败/空白/含"豆包/AI生成"字样 ──► 机审失败(可申诉回退重审)
     │ 通过则等人工
     ▼
Chess人审 ──► Kafka(push-audit-result-receipt-topic) ──► 消费更新状态
     │              [事务+乐观锁] 认证: AUDITING→PASS/FAILED; 奖励: CERTIFICATION_AUDIT→CAN_WITHDRAW/AUDIT_FAILED
     ▼
用户领奖 ──► [分布式锁+校验链] CAN_WITHDRAW→WITHDRAWING ──► 调Koln打款
     │
     ▼
Koln打款结果 ──► Kafka(koln-transfer-result) ──► 消费更新终态
                WITHDRAWING→WITHDRAW_SUCCESS / WITHDRAW_FAILED (乐观锁+状态校验)
```

**外部依赖**：Chess(审核平台)、Aegean(LLM推理代理)、Koln(打款)、Risk(风控)、Bard(黑名单)、Fopen(实名)、Tiger(Kafka封装)、Leo(动态配置)、Bat2(Redis缓存/分布式锁)、DTS(binlog订阅)。

---

# 第一块：MySQL 事务 + version 乐观锁

### 第1问：项目整体流程？审核和奖励发放是什么关系？

**回答**：职业认证分两条主线：**认证状态线**和**奖励状态线**，两者是"主从联动"关系——认证记录是主状态，奖励记录跟随认证结果联动流转。

- 认证状态机(代码中 `ProfessionCertificationAuditStatusEnum` + `NEXT_STATUS_MAP`)：
  `AUDITING(审核中) → AUDIT_PASS / AUDIT_FAILED / MACHINE_AUDIT_FAILED(机审失败,可申诉) → AUDITING`
- 奖励状态机(`ProfessionCertificationRewardStatusEnum`):
  `CERTIFICATION_AUDIT(认证审核中) → CAN_WITHDRAW(待提现,审核通过时) / AUDIT_FAILED / MACHINE_AUDIT_FAILED → WITHDRAWING(提现中) → WITHDRAW_SUCCESS / WITHDRAW_FAILED`

用户提交认证时，**同一个事务里**插入认证记录(AUDITING)和奖励记录(CERTIFICATION_AUDIT,有奖励资格时)；审核通过后奖励变为"待提现"；用户点领取后走打款；打款结果通过 Kafka 回执更新终态。**审核结果直接决定奖励资格和状态，所以两者必须强一致**。

### 第2问：为什么这个链路存在并发更新问题？

**回答**：三个真实并发源(代码里都有对应防护)：

1. **机审和人审并发**：OCR 机审是异步任务(`batchUtils.runAsync`),人工审核在 Chess 平台操作，两者可能同时处理同一条"审核中"的记录——机审要判失败、人审判通过，谁先落库谁生效，不能互相覆盖；
2. **Kafka 重复投递**：审核结果消息至少一次投递，同一条"审核通过"消息可能被消费两次；
3. **用户重复点击领奖**：用户可能连点提现按钮，或客户端重试，`checkAndSendProfessionCertificationReward` 被并发调用。

如果没有并发控制，会出现"认证已通过但奖励还是审核中"、"同一笔奖励重复打款"等数据不一致。

### 第3问：为什么用 MySQL 事务？保证了哪些操作一致性？

**回答**：以审核结果落库为例，`ProfessionCertificationOperateServiceImpl.processAuditResult` 上有 `@Transactional(rollbackFor = Throwable.class)`,一个事务内保证**三类写操作的原子性**：

1. **写操作流水**(审核通过/驳回 + 获取身份标识，两条 log);
2. **更新认证记录**(AUDITING → PASS/FAILED);
3. **联动更新奖励记录**(CERTIFICATION_AUDIT → CAN_WITHDRAW/AUDIT_FAILED)。

这三步必须同生共死：如果更新奖励记录失败，认证状态不能单独变为"通过"——否则用户看到认证成功但奖励消失，属于资损/客诉事故。同理提交认证时(`insertProfessionCertificationRecord`)也是事务内保证"认证记录 + 奖励记录 + 两条操作流水"原子插入。`rollbackFor = Throwable.class` 确保任何异常(包括非 RuntimeException)都回滚。

### 第4问：version 乐观锁怎么实现的？更新 SQL 什么样？

**回答**：典型的 **CAS 式条件更新**。表里有 `version` 字段，更新时把它放进 WHERE 条件，同时 SET 新 version:

```sql
UPDATE profession_certification_record
SET status = #{newStatus}, version = #{beforeVersion + 1},
    completed_time_ms = ..., discarded_time_ms = ...
WHERE uid = #{uid}
  AND profession_certification_id = #{certId}
  AND status = #{beforeStatus}      -- 期望状态
  AND version = #{beforeVersion}    -- 期望版本
```

代码路径：业务层先 `query...ByUidAndCertId` 读出 `beforeVersion`，构建 BO 时 `setVersion(beforeVersion + 1)`,Repository 层(`updateProfessionCertificationRecord`)用 MyBatis Example 拼 WHERE 条件执行，**返回影响行数**；`rows <= 0` 说明 version 或 status 已被别人改过，抛 `MaldivesDatabaseUnexpectedException` 让事务回滚。

**关键细节**：除了 version,WHERE 里还带了 `status = beforeStatus`,即"状态 + 版本"双重条件，而且 Repository 层有 `NEXT_STATUS_MAP` 状态机校验(只允许 AUDITING→PASS/FAILED/MACHINE_AUDIT_FAILED 等合法流转)，**非法流转直接在代码层拒绝**，这是三层防护：状态机校验 → 条件更新 → 影响行数判断。

### 第5问：已有事务，为什么还要乐观锁？

**回答**：事务解决的是**原子性**(多个写要么全成功要么全失败)，**不解决并发写冲突**。两个事务可以同时读到 version=5 的记录，然后都尝试更新：

- 事务只保证"各自事务内的操作一致"，但两个事务都提交成功的话，**后提交的会覆盖先提交的**(在 READ COMMITTED/REPEATABLE READ 下，普通 UPDATE 是"后者覆盖前者"的 lost update);
- 加了 version 后，第一个事务提交后 version 变成 6,第二个事务的 `WHERE version=5` 匹配不到行，影响行数为 0,更新失败回滚——**乐观锁把"并发覆盖"变成了"并发冲突检测"**。

一句话：**事务是"保原子"，乐观锁是"保时序正确"**，两者互补缺一不可。

### 第6问：两个线程同时审核同一条记录会发生什么？

**回答**：假设线程A(人审通过)和线程B(机审失败)同时处理 version=5 的记录：

1. 两者都读到 `status=AUDITING, version=5`,各自开启事务；
2. A 先执行 UPDATE `WHERE status=1 AND version=5` → 成功，行数=1,version 变 6,提交；
3. B 执行 UPDATE `WHERE status=1 AND version=5` → **匹配不到行**(status/version 都变了)，行数=0;
4. B 的代码检测到 `rows <= 0`,抛 `MaldivesDatabaseUnexpectedException`,**整个事务回滚**(包括 B 已写入的操作流水)；
5. 最终结果：只有 A 的结果生效，数据一致性保持——认证通过、奖励变为待提现；B 的机审失败不会覆盖。

另外机审入口还有前置状态校验(`handleMachineAuditFailed` 里 `if (!isAuditing(status)) return;` 静默返回)，大部分冲突在查询阶段就被拦截，乐观锁是最后一道兜底。

### 第7问：为什么不用 select ... for update 悲观锁？

**回答**：基于三个考量：

1. **冲突概率低**：一条认证记录同一时刻被两个审核方操作是极小概率事件(人审平台有任务分配，天然串行；机审异步且大部分在查询阶段就被状态校验拦下)。低冲突场景乐观锁吞吐远高于悲观锁——悲观锁每次都要加锁、持锁期间阻塞，乐观锁是无锁读 + 一次条件 UPDATE;
2. **锁持有时间可控性**：审核落库事务里除了 DB 操作，还包含操作流水写入等多个步骤，悲观锁会把锁持有时间拉长，高并发审核回执消费时容易造成锁等待甚至死锁；
3. **失败代价可接受**：乐观锁冲突时业务语义明确(记录已被处理)，直接失败/静默返回即可，不需要阻塞重试。

**补充加分项**：项目里对"用户领奖"这种冲突概率较高的场景，用的是**分布式锁**(`@UserLock` AOP + Bat2DistLock,领奖入口)而不是悲观锁，同样是避免 DB 层长事务持锁——**锁的选择是按冲突概率和业务语义分层的**。

### 第8问：乐观锁更新失败后，直接失败还是重试？为什么？

**回答**：**分场景，大部分直接失败，不做自动重试**：

- **审核结果更新失败**(`rows<=0`):抛异常回滚，Kafka 消费层返回 `RECONSUME_LATER` 走消息级重试。重试时重新查库，发现状态已不是 AUDITING,走幂等分支静默返回。**不在线程内自旋重试**，因为失败原因大概率是"记录已被别人处理"，重试没有意义，靠消息重试 + 状态校验天然收敛；
- **领奖状态更新失败**(CAN_WITHDRAW→WITHDRAWING):同样抛 `MaldivesRetryException`,由 SimpleRetry 框架发 Kafka 消息异步重试；
- **为什么不 CAS 自旋重试**：审核/领奖都是**状态流转**操作，不是"扣减库存"那种可重读重算的操作。冲突说明状态机已经走了别的分支，自旋重读只会得到"状态不合法"的结论。**只有"读-改-写且新值依赖最新值"的场景才适合自旋，状态机场景的正确姿势就是失败即终止**。

---

# 第二块：LLM OCR 机审 / 人审流转

### 第1问：为什么引入 LLM OCR?解决什么业务问题？

**回答**：三个业务痛点：

1. **审核人力成本**：职业认证材料(执业证书、职称证明)原本全量人工审核，量大、时效差(用户等结果要几小时甚至几天)。LLM OCR 机审把"明显不合格"的材料(空白图、AI 生成图、缺关键信息)在秒级拦下，人工只处理边缘 case,审核成本和时效大幅优化；
2. **传统 OCR 不够用**：职业证书版式千差万别(不同机构、不同年代)，传统模板 OCR 无法做"语义级"判断——比如"证书是否包含执业机构"、"是否出现 AI 生成痕迹"。LLM 的多模态理解能力恰好能输出图片的完整文字内容供规则引擎判断；
3. **风控前移**：奖励是现金，黑产会用 AI 生成的假证书批量薅羊毛，机审能在打款前拦截。

### 第2问：哪些场景直接机审，哪些转人工？

**回答**(以代码 `imageUrlOcrTask` + `OcrResultJudgementUtils` 的实际分类为准)：

**机审直接拦截(判 MACHINE_AUDIT_FAILED)**:
- 图片含"豆包"、"AI生成"等字样 → `ILLEGAL_TEXT`(AI 伪造材料);
- 全部图片识别结果为空白 → `IMAGE_BLANK`(假图/无关键信息);
- 规则校验不通过：按职业+材料字段配置的"必须包含/任一包含/禁止包含"关键字规则失败(如医生执业证书缺"主要执业机构")。

**转人工(Chess 平台)**:
- OCR 识别成功且规则通过 → 材料交给 Chess 审核平台**人工复核**，机审只做"粗筛拦截"，**不做通过判定**(机审失败可以申诉，说明系统对机审结果保留人工纠错通道);
- `SERVICE_ERROR`(LLM 服务异常)/`DOWNLOAD_FAIL`(图片下载失败)→ **不判失败**，留在审核中等人审，避免误杀。

**设计哲学**：机审只拦"高置信度的坏"，不放行"看起来像的好"——**宁可多人工，不可错杀用户**(机审失败用户可申诉回退到 AUDITING 重新人审)。

### 第3问：从上传材料到审核完成，机审链路是怎样的？

**回答**(对应 `ProfessionCertificationOcrServiceImpl`):

1. **提交阶段**：用户提交认证 → 事务内插入认证记录+奖励记录+操作流水，初始化 Chess 审核单，并写入 Bat2 缓存 `certId→uid` 映射(OCR 回调时只有 certId,用缓存反查 uid,避免再查库);
2. **触发机审**：C 端调 `ocrCertificationMaterial`,**接口立即返回，OCR 通过 `batchUtils.runAsync` 异步执行**(用户不等待，LLM 调用耗时不可控);
3. **逐图识别**：每张图调用 `AegeanIntegration.llmOcr`(LLM 推理代理，system prompt="你是专业OCR识别助手"，temperature=0.1 保证稳定输出；**一张图一次调用**，多图混识别会串内容);
4. **结果分类**：每张图产出 `(结果枚举, 识别文本)` 对：SERVICE_ERROR / DOWNLOAD_FAIL / IMAGE_BLANK / ILLEGAL_TEXT / PASS;
5. **汇总判定**：含 ILLEGAL_TEXT 或全部空白 → 机审不通过，生成拒绝文案(如"认证图片存在视觉异常或修改痕迹");
6. **规则精审**(`ocrNew`):按 Leo 配置的"职业ID+材料字段"规则，用 `OcrResultJudgementUtils.judgeOcrResult` 做"必须包含/任一包含/禁止包含(带豁免词)"校验，错误明细按字段组装成 `MaterialOcrResultVO` 列表;
7. **结果回传**:`chessIntegration.submitMaterialOcrResult` 把机审结论+字段级错误明细提交给 Chess,供人审参考；
8. **状态落库**：不通过则 `handleMachineAuditFailed` → 事务+乐观锁把认证流转为 MACHINE_AUDIT_FAILED、奖励联动为机审失败。

### 第4问：LLM OCR 结果怎么进入后续审核流程？

**回答**：两条路：

1. **结构化回传 Chess**：OCR 的字段级校验结果(`MaterialOcrResultVO`:字段名、是否有效、错误文案列表)通过 RPC 提交给 Chess 审核平台，**人审员在审核工作台直接看到机审结论和具体哪个字段不合格**，辅助人工快速裁决——机审是"辅助人审"而不是替代；
2. **本地状态联动**：机审不通过时，maldives 自己落库(认证→MACHINE_AUDIT_FAILED,奖励→MACHINE_AUDIT_FAILED),用户端立即看到失败原因；用户可**申诉**，申诉后乐观锁把状态回退 AUDITING、奖励回退 CERTIFICATION_AUDIT,**重新进入人审队列**——这就是"机审失败是**非终态**"的设计(`MACHINE_AUDIT_FAILED` 的 `isFinal=false`)。

人审最终结果则通过 Kafka 回执(见第三块)驱动本地状态机到终态。

### 第5问：Leo 动态控制机审策略，具体控制哪些内容？

**回答**(以 `MaldivesCommonConfig` 注册的 Leo key 为准，全部**热更新生效，无需发版**)：

| Leo Key | 控制内容 |
|---|---|
| `...machine.audit.config` | **机审总开关 enabled**(一键关闭机审，全部走人审)、测试用 mockUid、`skipAiCheckProfessionIds`(哪些职业跳过 AI 生成检测) |
| `maldives-base.material.ocr.prompt` | LLM 的 **system prompt / user prompt / temperature**——可在线调优识别效果 |
| `maldives-base.material.ocr.rule.config` | **按职业ID+材料字段**配置规则：必须包含关键字(如"主要执业机构")、任一包含关键字组(如"资格名称"或"专业技术资格")、禁止包含关键字+**豁免词**(如违禁词"豆包"豁免"豆包子公司")、每条规则的报错文案 |
| `maldives-base.is.test.env` / `mock.ocr.result` | 测试环境识别+mock OCR 结果，方便联调 |

### 第6问：为什么策略不能写死在代码里？

**回答**：四个理由：

1. **规则高频迭代**：黑产伪造手段在变(今天防"豆包"水印，明天出新变体)，新职业类型持续接入(每个职业的证书关键字不同)。写死意味着**每次调规则都要发版**，而 LLM 识别效果的调优(prompt/temperature)更是需要运营侧反复实验；
2. **应急止血能力**：机审是"自动拒绝用户"的逻辑，一旦发现误判(比如某职业证书新版式缺了配置的关键字导致批量误杀)，**Leo 一秒关掉机审开关或修正规则**，立刻止血；写死代码则要等发版+部署，期间持续产生客诉；
3. **灰度能力**:`skipAiCheckProfessionIds` 支持按职业粒度灰度放量，新规则先在个别职业验证；
4. **职责分离**：规则(哪些关键字合法)是**运营/审核团队的领域知识**，不是研发逻辑，Leo 让运营自助调整，研发不用当"改规则的中间人"。

### 第7问：LLM OCR 识别错误，怎么避免错误结果直接影响用户？

**回答**：代码里有**四层防误杀设计**：

1. **机审只拦不通过，不判通过**：LLM 识别+规则全过的材料仍交人审终审，LLM 的错误最多影响"效率"，不影响"正确性"；
2. **服务异常不等于失败**:`SERVICE_ERROR`/`DOWNLOAD_FAIL` 时**不判机审失败**，材料留在审核中走人审——LLM 挂了不能让用户买单；
3. **申诉通道兜底**：机审失败是**非终态**，用户可申诉，乐观锁回退状态重新人审。就算 LLM 误判，用户有一次人工纠错机会(`MACHINE_AUDIT_FAILED → AUDITING`);
4. **规则引擎比 LLM 判断更可控**：LLM 只负责"提取文字"(客观任务，temperature=0.1 降低幻觉)，**判断对错交给确定性的关键字规则**(带豁免词机制减少误伤)，而不是让 LLM 直接输出"合格/不合格"——把不可靠的模型输出和关键业务决策解耦。

### 第8问：LLM 服务超时/不可用，链路怎么降级？

**回答**：

1. **入口异步化**：OCR 接口本身是异步任务，LLM 超时不阻塞用户提交，用户侧无感知，状态停在"审核中";
2. **异常分类降级**:`llmOcr` 抛异常被 catch,返回 null → 归类为 `SERVICE_ERROR`,**机审判定 pass=false 的条件不含 SERVICE_ERROR**(只有 ILLEGAL_TEXT 或全部空白才拦)，所以 LLM 不可用时材料**自动全部流转人审**，链路不中断——这就是"机审开关"的自然等价形态；
3. **总开关一键降级**：Leo 的 `machine.audit.config.enabled=false` 可主动关闭整个机审，`handleMachineAuditFailed` 入口直接 return;
4. **测试环境隔离**:`IS_TEST_ENV` + `MOCK_OCR_RESULT` 支持 mock 识别结果，LLM 故障期间测试不受影响。

---

# 第三块：Kafka 异步消费 + 失败重试 + 幂等

### 第1问：为什么审核结果、打款结果用 Kafka 异步处理而不是同步调用？

**回答**：这里 Kafka 消息的消费方是 maldives,生产方是**审核平台(Chess)和打款系统(Koln)**——是典型的**跨系统结果回执**场景：

1. **系统解耦**：Chess/Koln 是独立平台，服务成百上千个业务方，不可能对每个业务方做同步 RPC 回调(要维护调用方列表、处理每个调用方的超时和失败)。发一条回执消息，谁关心谁订阅，**生产方完全不感知消费方**;
2. **可靠性**：同步调用失败即丢失(审核系统不可能因为 maldives 抖动就重试或保存每个回调)；消息落 Kafka 后**至少一次投递 + 消费重试**，maldives 宕机重启后消息还在，结果不丢——**钱和认证结果不能丢，这是选消息的核心原因**；
3. **削峰**：审核平台批量出审核结果(如审核员集中处理一批)、打款系统批量回调时，消息在 Kafka 里排队，maldives 按自己的消费能力处理，不会被打垮。

### 第2问：Kafka 在这里主要解决解耦、削峰还是可靠性？

**回答**：**首要可靠性，其次解耦，削峰是附带收益**。

- **可靠性是根本诉求**：审核结果决定奖励资格(能不能提现)、打款结果决定资金状态(是否到账)，丢一条消息 = 用户认证状态卡死在中间态 / 钱打了但状态没更新。所以消费框架开启了 `enableConsumeRetry(true)`,失败消息最多重投 3 次；
- **解耦是架构必然**：上游是平台型系统，回执天然走消息；
- **削峰是顺带的**：审核和打款回执量级不算特别大，但批量回执时消息缓冲确实平滑了流量。

### 第3问：消费到审核结果/打款结果后，具体做哪些业务处理？

**回答**：两个消费者(继承 `AbstractTigerConsumer`,topic 级配置)：

**审核结果**(`PushAuditResultReceiptTask`,topic=`push-audit-result-receipt-topic`):
1. 反序列化 + 消息合法性校验(类型必须是职业认证、状态必须是"已审核")；
2. 查认证记录，**状态校验**：非 AUDITING → 静默返回(幂等/已被处理)；
3. 审核通过：黑名单二次校验(拉黑用户不允许通过)→ **事务内**：写"审核通过"+"获取身份标识"两条操作流水、乐观锁更新认证 AUDITING→PASS、奖励 CERTIFICATION_AUDIT→CAN_WITHDRAW;
4. 审核拒绝：校验拒绝原因必填 → 事务内写"驳回"流水、更新认证 AUDITING→FAILED、奖励→AUDIT_FAILED。

**打款结果**(`TransferResultReceiptConsumer`,topic=`koln-transfer-result`):
1. 过滤：只处理 `isEnd=true` 的终态回执、只处理本活动类型；
2. 查奖励记录，**状态校验**：非 WITHDRAWING → 静默返回(幂等)；
3. **金额对账校验**：回执金额 ≠ 记录金额 → 抛异常报警(资损风险，人工排查)；
4. 乐观锁更新终态：打款成功 → WITHDRAW_SUCCESS(记录到账时间、打款单号、打款方式)；打款关闭 → WITHDRAW_FAILED。

### 第4问：消费失败后怎么重试？

**回答**：**框架级消息重试**，配置在 `AbstractTigerConsumer`:

```java
.enableConsumeRetry(true)
.maxReconsumeTimes(3)                      // 最多重投 3 次
.reconsumeDelayIntervalConfig("15-s,1-m,5-m")  // 退避间隔: 15秒 → 1分钟 → 5分钟
```

消费逻辑抛任何异常(包括主动抛的 `MaldivesRetryException`)→ listener 返回 `RECONSUME_LATER` → Tiger(Kafka 封装)按退避策略重新投递。**重试是消息级的**，每次重试都从头执行完整业务逻辑，靠状态校验保证不重复执行副作用。

另外项目里还有一层**业务自愈重试**：领奖打款同步失败时，`SimpleRetryWorker` 把请求发到 `maldives-simple-retry` topic,由 `SimpleRetryService` 消费重试(1s/3s/10s 三次)——这是**主动补偿**，和 Kafka 被动重投互补。

### 第5问：Kafka 重复投递，为什么状态校验能保证消费幂等？

**回答**：核心是**"前置状态校验 + 条件更新"两道闸**，本质是把"消息幂等"转化为"状态机幂等"：

1. **第一道闸(查询层)**：消费先查记录当前状态，**只有处于"期望的前置状态"才继续处理**：
   - 审核结果：必须 `AUDITING`,否则 `Cat.logEvent` 记录后**静默返回**(消息 ACK,不重试);
   - 打款结果：必须 `WITHDRAWING`,否则静默返回。
   
   第二次消费同一条消息时，状态已被第一次消费推进到终态/下一状态，校验不过直接返回——**重复消息变成了 no-op**;

2. **第二道闸(更新层)**：即使两个消费线程同时通过第一道闸(并发重复)，乐观锁条件更新 `WHERE status=前状态 AND version=前版本` 保证**只有一个线程更新成功**，另一个 rows=0 抛异常回滚。

为什么状态校验就够幂等？因为这类消息的**业务语义本身就是单向状态流转**——"审核通过"消息重复 100 次，期望的终效也只是"状态=通过+奖励=待提现"，是幂等的目标状态；状态一旦到位，后续重复消息天然无效。**不需要消息去重表，状态字段本身就是幂等键**。

### 第6问：同一条打款成功消息消费两次，会发生什么？

**回答**：分两种时序：

- **串行重复**(第一次消费成功后消息又被重投)：第二次消费查到状态已是 WITHDRAW_SUCCESS ≠ WITHDRAWING → 记录 `status illegal` 事件 → 静默返回。**无任何副作用**；
- **并发重复**(两条相同消息同时被两个线程消费)：两个线程都读到 WITHDRAWING,都尝试 `WHERE status=WITHDRAWING AND version=N` 更新为 WITHDRAW_SUCCESS → **只有一个成功**，另一个 rows=0 抛 `MaldivesRetryException` → Kafka 重试 → 重试时状态已变，静默返回。最终状态正确：**只记录一次到账时间、一个打款单号，不会重复记账**。

关键在于更新动作是**条件更新而非无条件 SET**,重复消息的"写"在数据库层被物理拦截。

### 第7问：如果"下游已打款成功，但本地状态更新失败"，重试时怎么避免重复打款？

**回答**：这是**跨系统一致性的经典难题**(打款 RPC 成功，本地 WITHDRAWING 状态更新失败)。项目的防护设计：

1. **打款前的状态预置(关键！)**:`SendProfessionCertificationRewardJob` 的顺序是——**先把奖励从 CAN_WITHDRAW 乐观锁更新为 WITHDRAWING(持久化到 DB),再调 Koln 打款**。这样即使打款后本地更新回执失败，记录已卡在 WITHDRAWING 中间态；
2. **重试时的状态拦截**：SimpleRetry 重试会重新执行 Job,Job 内部先查状态——`WITHDRAW_SUCCESS` → `already_send` 直接返回；**`WITHDRAWING` → 说明打款可能已发出，不再重复调用 Koln**，只等待打款回执消息来推进终态(代码里 WITHDRAWING 也是允许放行的状态，但不会二次发起打款，因为 Koln 侧有幂等)；
3. **下游幂等键**：调 Koln 时传了 `riskEventId`(风控生成的事件ID)和 `activitySn`(=certId),**打款系统侧以 eventId 幂等**——即使真的重复调用，Koln 也不会重复打款，返回相同结果；
4. **分布式锁**：领奖入口 `@UserLock`(uid 粒度)+ Job 内 `Bat2DistLock`(uid 粒度，5秒租期)双重锁，同一用户的领奖请求物理串行，杜绝并发双开打款；
5. **对账兜底**：打款前发"前置打款消息"给财务离线对账(`sendPrePayRecordWithKoln`),回执金额与本地不一致时抛异常报警，异常态有人工介入通道。

**总结话术**：防重复打款 = **状态机前置(CAN_WITHDRAW→WITHDRAWING 先落库)+ 下游幂等键 + 分布式锁** 三层，而不是只靠本地 DB。

### 第8问：一条消息一直消费失败，怎么处理？会不会影响后续消息？

**回答**：

1. **有限重试**：最多 3 次(15s/1m/5m 退避)，超限后消息进入**死信/不再投递**(Tiger 框架的 maxReconsumeTimes 语义)，不会无限循环；
2. **不影响后续消息**：Tiger 是**并发消费者**(`TigerConcurrentConsumer`),单条消息失败只返回该批 `RECONSUME_LATER`,**不阻塞分区后续消息的消费**——重试消息重新入队排队，后续消息照常处理。这也是为什么消费逻辑要**幂等+无顺序依赖**的设计前提；
3. **失败可观测**：每次异常都打 error 日志 + Cat 打点(`tiger_msg_` 前缀 metric、状态非法事件打点)，重试 3 次仍失败的消息会通过监控告警暴露，人工介入；
4. **区分失败类型**(面试加分点)：代码里区分了两类异常——
   - `MaldivesUnexpectedException`(非法消息、记录不存在、金额不匹配)：**数据问题，重试也不会成功**，但为了不丢消息仍走重试，超限后靠告警人工排查；
   - `MaldivesRetryException`(更新失败、RPC 超时)：**瞬时故障**，重试大概率自愈。
   
   消息里还有 `dealMsg` 前置校验，空消息直接 return ACK,避免脏消息空耗重试额度。

---

# 🎯 终极串联题：四个机制各解决什么问题？

**标准回答**(建议背熟，再按代码展开)：

> **这四个机制解决的是分布式业务一致性的四个不同层面的问题：**
>
> 1. **MySQL 事务**保证**单次业务操作的原子性**——审核落库时"操作流水 + 认证记录 + 奖励记录"三张表的写要么全成、要么全败，不会出现"认证通过了但奖励状态没跟上"的中间态；
>
> 2. **version 乐观锁**解决**并发状态更新冲突**——机审和人审同时操作、Kafka 重复消费、用户重复点击，通过"WHERE status+version"条件更新，保证同一状态流转只有一个赢家，其余失败回滚，且无锁高性能；
>
> 3. **Kafka**负责**异步解耦和结果通知的可靠性**——审核平台、打款系统的回执经消息投递，生产消费解耦，at-least-once + 消费重试保证"结果不丢、服务不被下游故障拖死"；
>
> 4. **状态校验**负责**重复消息下的业务幂等**——消费入口先查状态、非期望前置状态静默返回，把消息幂等转化为状态机幂等，重复投递不会产生重复副作用，和乐观锁配合构成"查询拦截 + 更新拦截"双保险。
>
> **一句话**：事务保原子、乐观锁保并发正确、Kafka 保可靠传递、状态校验保幂等收敛——四者叠加，才让"认证→奖励→打款"这条涉及资金的状态机能安全运转。

---

# 💡 面试补充弹药(可能被追问的细节)

1. **"为什么审核通过消息消费失败要抛异常重试，而不是静默？"**——因为审核结果是关键推进事件，静默返回会让记录卡死在"审核中"，用户永远等不到结果；重试 + 状态校验能保证最终推进。
2. **"操作流水表的作用？"**——审计追溯：提交/审核中/通过/驳回/获取标识/申诉每个节点都有流水，时间戳+1 保证顺序，客诉时可还原完整时间线。
3. **"机审失败为什么设计成非终态？"**——LLM 有误判率，申诉回退(AUDITING)给用户人工纠错通道，是对"AI 决策不可靠"的产品级兜底。
4. **"DTS 在项目里干嘛？"**——监听认证表 binlog，DB 变更时把 Bat2 缓存短时过期(2s),防止高并发下旧数据回填缓存，解决"缓存与 DB 不一致"问题。
5. **"为什么打款用分布式锁而不是只靠乐观锁？"**——乐观锁防的是"状态被覆盖"，分布式锁防的是"打款 RPC 被并发重复发起"(RPC 调用无法用 DB 条件更新拦截)，两者防护的临界区不同。

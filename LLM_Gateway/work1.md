aegean = LLM 网关（Agent）。上游业务方通过 Dubbo RPC 调用 LlmInferProxyService（llmChat / llmAsyncGenerate / llmGenerateAudio），网关完成 鉴权（Leo 白名单 + 场景授权）→ 双层 Sentinel 限流 → 多媒体下载与内存治理 → ticket→物理集群路由 → 鹊桥 HTTP 转发底层大模型。核心类：LlmInferProxyServiceImpl（入口+第一层限流）、AuthManagerImpl（鉴权）、HttpManagerImpl（第二层限流+转发）、ProxyConfig/LlmProviderConfig（Leo 动态配置）。

八个问题回答
1. 为什么需要统一 LLM 网关？业务方直连大模型集群有什么问题？
直连模式下有四个核心痛点：① 无鉴权——任何知道集群地址的服务都能调用，无法防非法调用和做审计；② 无限流保护——某个业务方流量暴涨会直接打垮集群，故障扩散到所有调用方；③ 物理细节暴露——调用方必须硬编码 providerName、真实 modelName、path、PNS 租户等，集群迁移、换模型版本要所有业务方改代码发版，且调用方可乱传参数；④ 重复建设——超时、重试、监控、大文件内存治理每个业务方各做一遍。所以我们做了网关：收敛流量统一入口，统一做白名单鉴权、双层限流、ticket→模型的路由映射（屏蔽物理细节）、CAT 统一监控和大对象内存治理，底层变更对调用方完全透明。

2. 一次完整的大模型请求经过网关的链路？
以 llmChat 为例，六步：① Dubbo 入口——调用方传 appName + proxyScene（ticket）+ 业务参数，Provider 端 AegeanDubboFilter 统一处理异常封装；② 鉴权——AuthManager.auth() 校验调用方白名单（appName 缺省时从 Dubbo attachment 取 consumer 名）→ 校验 proxyScene 是否授权 → 校验目标 provider 是否 valid，返回该场景的路由配置 BO；③ 第一层限流——SphU.entry("aegean_场景资源名")，consumer×scene 维度；④ 多媒体预处理——从 OSS 下载图片/视频，流式 base64 编码，文件大小打点，支持 fastFail；⑤ 模型映射与转发——用配置里的逻辑模型名覆盖请求的 model 字段，序列化后进入 HttpManager，先做第二层限流 SphU.entry("aegean_providerName")，再通过鹊桥 HttpTemplate（PNS 服务发现）POST 到底层集群真实 path；⑥ 后置处理——如字幕场景解析 ASS 内容并上传 OSS，最后封装 BaseResponse 返回，finally 中释放 Sentinel Entry。

3. Leo 具体管理哪些配置？为什么需要动态化？
Leo 管理两个 key：① 消费者授权配置——调用方白名单集合，以及每个 proxyScene 的授权记录（consumer、authorized、sentinelResourceName、路由到哪个 llmProvider、llmRequestPath、llmModelName），本质是"谁 + 哪个场景 → 哪个物理集群"的映射；② 物理集群连接配置——每个 provider 的 PNS 租户/分组、端口、超时、clientNamespace、valid 标记、sbd 环境隔离标记。动态化的原因：权限变更是高频运营动作（新业务接入、临时授权、违规紧急摘权、集群故障一键摘流），不可能靠发版解决；模型路由也需要随集群扩容、迁移、版本切换、环境灰度实时调整。动态化后权限和路由与代码彻底解耦，秒级生效、可快速回滚。

4. Leo 配置变化时，怎么保证运行中请求不会读到"更新到一半"的配置？
核心是整体替换 + 不可变对象，而非原地修改：Leo 推送触发 load() 回调，把整份 JSON 一次性反序列化，构建全新的不可变 Map/Set（unmodifiableMap/toUnmodifiableSet），然后一次性替换成员引用。Java 引用赋值是原子的，读线程要么拿到旧引用（完整的旧配置），要么拿到新引用（完整的新配置），不存在中间态；配置 BO 全部是 final 字段的不可变对象，构造后不会变。另外两点保障：请求内一致性——一次请求在 auth 阶段拿到 configBO 引用后，整条链路（限流、路由、转发）都用这一个对象，中途配置更新不影响本次请求按旧配置执行完毕；失败保护——rawData 为空或解析异常时抛异常、不替换引用，继续沿用旧配置，坏配置不会覆盖好配置。HttpTemplate 的懒加载则用 tryLock + double-check 保证并发下只初始化一次。

5. 为什么需要两层 Sentinel 限流？只在 Dubbo 入口限流不行吗？
不行，因为两层解决的是两个不同维度的问题。只在入口（调用方维度）限流：多个业务方的配额之和可能超过集群真实容量——比如 10 个业务方各给 100 QPS，但集群只能扛 500，入口全放行了底层照样被打垮；而且新增业务方或调大某家配额都可能意外突破集群容量。反过来只在集群维度限流：无法做业务方之间的隔离，A 涨流量会挤占 B 的请求，先到先得导致 B 被误伤。所以第一层的语义是配额管理与公平隔离（每家业务方用多少），第二层的语义是容量兜底保护（集群总共能扛多少），两者配合才能既隔离业务方又保护底层。

6. 两个维度分别以什么作为 Sentinel Resource？阈值分别解决什么问题？
调用方维度：resource 为 aegean_ + sentinelResourceName（未配置时降级用 proxyScene），即每个 consumer × 场景一条规则，在 Dubbo 服务实现的入口处 SphU.entry，阈值解决"单个业务方的最大用量"——保证 A 的流量最多消耗 A 自己的配额，不侵占他人。模型集群维度：resource 为 aegean_ + llmProviderName，在 HttpManager 发起 HTTP 转发前 SphU.entry，阈值对应该物理集群的全局最大 QPS，解决"无论多少上游调用，集群总流量不超容量上限"。另外压测流量通过 TS_ 前缀的 resource 名与线上流量天然隔离，互不影响。

7. A、B 调同一集群，A 流量暴涨，怎么既不影响 B 又不打垮底层？
第一道防线就是第一层限流：A 的超量请求在自己 consumer 维度的阈值处被直接拒绝，根本到不了集群维度，B 的流量不受任何影响——这正是配额规划时保证"所有业务方对同一集群的配额之和 ≤ 集群容量"的原因，让隔离在第一层就完成。第二层是兜底：如果配额规划有疏漏或整体水位异常，集群维度限流保证底层不被打垮。动态应急：如果 A 是合理增长，通过 Leo 动态上调 A 的场景配置和 Sentinel 阈值，无需发版；如果 A 是异常调用，直接在 Leo 把它的授权摘掉；如果底层集群本身出问题，把 provider 的 valid 置为 false 一键摘流整个集群。整个链路上鉴权、限流、路由全部可动态调整，具备完整的应急手段。

8. Sentinel 拒绝的请求怎么处理？直接失败、重试还是降级？为什么？
直接快速失败，不重试。代码上 SphU.entry 抛出 BlockException 后，由 Provider 端的 AegeanDubboFilter 统一捕获，封装成错误码 RATE_LIMIT_EXCEED_EXCEPTION 的 BaseResponse 返回调用方。理由有三：① 重试会放大流量——LLM 推理是秒级长耗时、资源密集型请求，被限流说明容量已满，重试只会雪上加霜，违背限流初衷，所以 Dubbo provider 也配置了 retries=0；② 快速失败保护网关自身——避免被拒请求在线程池里堆积，拖垮网关影响其他业务方；③ 降级策略应该由调用方决定——网关不感知各业务方的业务语义，返回明确的限流错误码后，调用方可自行选择降级路径（换小模型、读缓存、提示用户稍后再试）。这是典型的 fail-fast 设计：网关负责"拒绝得快、错误码明确"，降级决策权交还业务方。

以上回答均基于项目实际代码（LlmInferProxyServiceImpl、AuthManagerImpl、HttpManagerImpl、ProxyConfig/LlmProviderConfig、AegeanDubboFilter），细节可经得起追问。

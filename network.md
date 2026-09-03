Http初始化配置
sequenceDiagram
    participant Biz as 业务层
    participant PC as ProxyConfig
    participant LPC as LlmProviderConfig
    participant HT as HttpTemplate
    participant RG as RegistryConfig
    participant PNS as PNS服务
    participant LB as 负载均衡
    participant CPP as 连接池
    participant Target as 目标实例(10.0.0.2:8080)

    Biz->>PC: doPost(request, proxyScene)
    
    rect rgb(240, 248, 255)
    Note over PC: ① 查询代理配置
    PC->>PC: 根据proxyScene查到LlmProxyConfigBO
    PC-->>Biz: 返回llmProviderName(如"gpt-4-provider")
    end

    rect rgb(255, 248, 220)
    Note over LPC: ② 获取/创建HttpTemplate
    Biz->>LPC: getOrCreateHttpTemplate("gpt-4-provider")
    LPC->>LPC: 从httpTemplateMap获取或懒加载
    LPC-->>Biz: 返回HttpTemplate实例
    end

    rect rgb(245, 255, 240)
    Note over HT: ③ 发起HTTP请求
    Biz->>HT: post()
    HT->>HT: .setPath("/v1/chat/completions")
    HT->>HT: .setBody(request)
    HT->>HT: .execute()
    end

    rect rgb(255, 240, 245)
    Note over HT,Target: ④ HttpTemplate内部请求流程
    HT->>RG: 查询可用实例
    RG->>PNS: 请求实例列表(pnsTenant, pnsGroup)
    PNS-->>RG: 返回实例列表
    RG->>LB: 负载均衡选择实例
    LB-->>HT: 选中 10.0.0.2:8080
    
    HT->>CPP: 获取/创建TCP连接
    CPP-->>HT: 返回连接
    HT->>Target: 发送HTTP请求
    Target-->>HT: 返回HttpResponse
    end
    
    HT-->>Biz: 返回最终响应

业务层调用 doPost(request, proxyScene)
    │
    ▼
① ProxyConfig 根据 proxyScene 查到 LlmProxyConfigBO
   → 获取 llmProviderName（如 "gpt-4-provider"）
    │
    ▼
② LlmProviderConfig.getOrCreateHttpTemplate("gpt-4-provider")
   → 从 httpTemplateMap 获取/懒加载 HttpTemplate
    │
    ▼
③ HttpTemplate.post()
   .setPath(configBO.getLlmRequestPath())   // 设置请求路径，如 "/v1/chat/completions"
   .setBody(request)                       // 设置请求体
   .execute()                               // 发起请求
    │
    ▼
④ HttpTemplate 内部：
   ├── 通过 RegistryConfig(pnsTenant, pnsGroup) 向 PNS 查询可用实例
   ├── 负载均衡选择一个实例（如 10.0.0.2:8080）
   ├── 从连接池获取/创建到 10.0.0.2:8080 的 TCP 连接
   ├── 发送 HTTP 请求
   └── 返回 HttpResponse
总结：服务配置与连接池机制

Q1：动态配置热更新的机制是什么？
A： 通过配置中心监听机制实现。服务启动时向配置中心注册监听器，当配置变更时，配置中心回调通知，服务端解析配置并重建内存中的配置缓存和客户端实例缓存。整个过程无需重启，通过引用整体替换保证可见性。

Q2：配置中心推送的数据格式是什么？
A： JSON 数组字符串，每个元素是一个配置对象。字段命名采用 snake_case，反序列化为配置对象列表。反序列化器容许未知字段（静默忽略）和缺失字段（置为 null）。

Q3：服务发现参数的作用是什么？
A： 服务发现参数（租户 + 分组）唯一标识注册中心中的一组服务实例。客户端通过这两个参数向注册中心查询可用实例列表（IP:Port），获得列表后通过负载均衡选择一个实例发起请求。注册中心负责维护实例的存活状态，实例上下线时自动更新。

Q4：服务发现系统与容器编排系统的关系？
A： 职责不同，协同工作。容器编排系统（如 K8s）负责部署、调度、扩缩容；服务发现系统（如 PNS）只负责记录"服务在哪里、是否存活"。容器启动后向服务发现系统注册，消费方通过服务发现系统查询目标地址。两者是上下游关系，不是同一层。

Q5：服务发现系统与通用协调系统（如 Zookeeper）的区别？
A： 通用协调系统是"瑞士军刀"，能做服务发现、分布式锁、配置管理、选举等；服务发现系统是"专用通讯录"，只做服务注册与发现，但更轻量、性能更好。两者可在同一项目中并存，各司其职。

Q6：共享连接池时，不同客户端模板之间有什么区别？
A： 共享的是连接池基础设施（连接数上限、复用策略、回收策略），不共享的是各自的服务发现目标（连谁）、超时策略（等多久）和身份标识（叫什么）。连接池内部按目标地址（host:port）分组管理，不同模板连不同后端，互不干扰。超时配置是请求级的，不是连接池级的。

Q7：初始化时建立的是连接吗？
A： 不是。初始化只创建配置模板对象，包含寻址配置（去哪连）和请求策略（怎么连），此时零条 TCP 连接。真正的 TCP 连接和 HTTP 请求在首次调用 execute() 时才按需创建。连接建立后归还连接池，后续请求复用。

Q8：连接池如何管理多个目标地址的连接？
A： 连接池按目标地址（host:port）分 route 管理，每个 route 维护一组独立连接。两个核心限制：MaxPerRoute（单个目标地址的最大连接数）和 MaxTotal（整个池的最大连接数）。少了按需新建（受 MaxPerRoute 限制），多了排队等待（受 MaxTotal 限制），空闲/过期的连接自动关闭回收。

Q9：MaxPerRoute 管的是池总量还是单 route？
A： MaxPerRoute 是**单个 route（单个 host:port）**的最大连接数。MaxTotal 才是整个池的总量上限。两者是分层限制关系：单 route 不超 MaxPerRoute，所有 route 之和不超 MaxTotal。

Q10：连接断开时为什么不能直接新建连接？为什么启动时连不上会导致启动失败？
A： 客户端模板的创建不只是 TCP 连接，还包含服务发现订阅、连接池初始化等步骤。旧设计在启动时强制初始化所有客户端模板，任一步骤失败且异常未捕获，就会导致整个服务启动失败。新设计通过容错跳过 + 懒加载重试解决：启动时初始化失败的不阻塞（异常被捕获、跳过），请求到来时再按需重试创建，此时目标服务可能已恢复。

Q11：目标服务故障时，服务发现订阅这一步会失败吗？
A： 不一定。服务发现系统（注册中心）与目标服务是两个独立系统。目标服务故障时，注册中心仍正常运行，只是返回空实例列表，订阅本身成功。订阅失败主要发生在注册中心本身不可达、网络不通、或框架内部初始化异常时。

Q12：旧设计在启动时强制初始化，哪些场景会导致启动失败？
A： 核心矛盾是启动时序不匹配：消费方启动快（秒级），目标服务启动慢（分钟级）。典型场景包括：容器编排平台滚动发布/实例迁移时的窗口期无可用实例、目标服务启动慢尚未注册、注册中心不可达、网络分区、配置错误、依赖初始化顺序不当等。最常见的是容器编排平台的滚动发布和实例迁移场景。

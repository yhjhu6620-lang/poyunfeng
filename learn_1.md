这个是技术的优化，是LLM网关aegean项目，我做了如下的流程：
1.熟悉整个网关项目的代码逻辑，了解鉴权、限流、大模型http请求等；
2.对网关项目中的proxyscene进行Cat.logEvent打点；重新整合同步请求和流式请求，通过回调函数的形式进行打点Cat.newTransaction。
<img width="976" height="408" alt="image" src="https://github.com/user-attachments/assets/dfa90879-3cc6-4061-8e5c-967d3cd10f2e" />


图片是项目背景和需求，我做了如下的流程：
1.在Leo配置中心的职业资格value增加getSkipAiCheckProfessionIds字段，加入跳过机审中有AI的职业ID；
2.我修改职业认证maldives的业务逻辑代码，同时，增加单测代码的逻辑，配置泳道进行自测；
3.上传代码到htj之后，我在手机htj环境下进行自测增加的功能是否有效，成功地完成了测试。
<img width="993" height="361" alt="image" src="https://github.com/user-attachments/assets/74aced75-4fad-4e4a-bc1c-26775de85615" />

这是项目背景，我是做的主要流程如下：
1. 查看constantine-api的订单列表实际逻辑的代码，写出对“【多件折扣】订单列表单独灰度重开“的版本控制和ab实验的代码。
2. 完成constantine-api中的多件折扣分流实验的代码和单测；并且遇到了不同代码的风格，组合与继承，重构了代码逻辑。
3. 现在htj上进行测试，完成之后再预发上进行测试直至最后的代码提交。
4.  <img width="1408" height="456" alt="image" src="https://github.com/user-attachments/assets/17309245-8ef4-4c94-9245-5625bbf1b8fd" />



视频审核 sam3 模型部署
- 配合其他上游调用大模型网关的开发，增加字段和更新发版号。
- 同时，增加Leo配置中心的调用方与大模型的配置。
<img width="963" height="294" alt="image" src="https://github.com/user-attachments/assets/6a543947-cf9c-449d-bdcb-a6051861548a" />


aegean兼容大模型上下线优化
- 将Leo配置热加载直接报错的逻辑优化为抛异常继续处理，解决k8s在实际迁移中触发告警；
- 利用ReentrantLock双重锁+快速失败，实现安全懒加载，http Client二次初始化逻辑并且，也避免线程推积。
<img width="994" height="462" alt="image" src="https://github.com/user-attachments/assets/2b793cd7-cf8a-4e1d-8df9-651bdb45eefc" />


- 了解foreign代码的逻辑，采用设计模式中的模板方法和策略模式重构查询多件折扣与省钱助手的代码；
- 重新配置Leo的一二级资源，使配置能够清晰明确；同时，也增加resource_render配置，在业务代码中动态加载Leo已有banner资源的配置；
- 在预发上进行自测，从monitor查看并分析报错逻辑，解决代码逻辑错误、风控与ab实验等白名单，最终自测成功；
<img width="966" height="417" alt="image" src="https://github.com/user-attachments/assets/da06ae84-d876-47b3-a7f2-7a66364ec55f" />



- 在dubbo rpc中增加一个hostgroup，针对评价返现与业务逻辑进行隔离；
- 并在金丝雀上增加hostgroup的配置，也在预发配置进行测试，没有问题发线上。
<img width="990" height="468" alt="image" src="https://github.com/user-attachments/assets/ff9656fc-21e1-4b20-82e8-ee056348c524" />


- 增加聚合优惠券的活动子渠道实验的代码，写完单测并上htj与预发；
- 开a/b实验，聚合优惠券渠道A大盐、D大盐以及团弹窗渠道大盐；
- 针对聚合优惠券渠道A大盐实验不平，开5组aa进行分桶验证。
<img width="858" height="559" alt="image" src="https://github.com/user-attachments/assets/3ce5f944-0b92-43e8-a673-b003e8539135" />



- 了解职业认证代码，采用模板方法写"裂变邀约"资格（引导已认证用户邀请好友认证）业务代码逻辑；
- 配置Leo中裂变邀约推送跳转链接，并进行自测直至最后的上线。
<img width="1021" height="749" alt="image" src="https://github.com/user-attachments/assets/60cb265b-bd5a-47f1-ac0a-cc19d66155dd" />


- 这个业务需要在Leo配置中增加职业认证即可；
- 在htj上自测成功；
<img width="1017" height="899" alt="image" src="https://github.com/user-attachments/assets/db8efb56-560a-4de5-8b69-fce54ffdbeef" />


- 在Leo配置中增加团弹窗迭代的字段与实验开关配置；
- 在a/b test中增加一个实验开关；
- 把7.31团弹窗迭代增加的实验，字段、配置白名单、埋点、监控地址以及上下游配合注意事项写成文档；
- 在预发上自测成功了，灰度发布发线上，推全量、进冷启，开实验90%，同时观察监控是否正常。
<img width="993" height="850" alt="image" src="https://github.com/user-attachments/assets/5b2a026a-8e41-4a87-a779-e345b1e2e540" />


- 在Leo配置中增加团弹窗迭代的字段与实验开关配置，并在a/b test中开实验；
- 把把8.7团弹窗迭代增加的实验，字段、配置白名单、埋点、监控地址以及上下游配合注意事项写成文档；
- 继续按照之前的要求，进行自测、推全量、进冷启、开实验90%，观察监控。
<img width="1000" height="770" alt="image" src="https://github.com/user-attachments/assets/bb1366a3-8b64-4fdd-bd9e-d78e69ceb345" />


- 排查Trace链路，精准定位到“无活动资格”的根因为Leo配置错误，并协助完成配置修复，在htj上自测成功。
<img width="1213" height="844" alt="image" src="https://github.com/user-attachments/assets/45fd1a6c-668b-42f9-bc24-31c5c2f44509" />


- 修改代码中返回的失败原因，同时也更新Leo配置中内部后台ai标识的内容；
<img width="1002" height="768" alt="image" src="https://github.com/user-attachments/assets/b886984f-aff7-40f9-a12d-a8f846f3ad1f" />

-增加算法服务，重构并实现职业标签与商品类目的精准匹配逻辑；
- 主动协同上下游团队，对齐并透传算法所需的数据字段，保障数据链路完整；
- 依托Leo配置中心实现新旧逻辑的开关控制与灰度发布，确保线上变更平稳无故障。
<img width="942" height="454" alt="image" src="https://github.com/user-attachments/assets/2c4a00b1-79a7-4e5b-9802-b0d70f49ac5a" />

- 分析判断markov客户端发起请求，到网关这服务端处理请求的差时是5秒钟，导致请求超时失败率较高。
- 查看监控与代码逻辑中，在网关的provider没有限流设置，第二点是，线程池也没有打满，都是正常的现状。第三点，就是Tcp. DelayedACKLost出现了异常，其他监控指标都是征程的；第四点，这个问题通过扩容可以极大缓解；
- 网关服务，传输的对象都是图像这样的大对象，分析出是网络传输问题，同时也可能是Dubbo 2.x 的解码/反序列化跑在 Netty EventLoop（IO 线程）上。


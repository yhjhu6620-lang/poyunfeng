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



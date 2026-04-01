# 为什么我想把多 Agent 系统做成一个“运行时”：Form_zero 的设计笔记

项目地址：<https://github.com/sheydz/agents_token_os>

大多数人第一次做多 Agent 系统，起点都很自然：先写几个 prompt，给它们起上 `planner`、`writer`、`reviewer` 这样的名字，再用几条规则决定谁先说、谁后说。只要 demo 能跑起来，系统看起来就已经“有协作感”了。

但我很快发现，这种做法有一个很根本的问题：你其实并没有得到一个真正的多进程协作系统，你只是得到了几段彼此嵌套的 prompt。它们一开始很灵活，后面却越来越难维护。你不知道哪段上下文是真正重要的，不知道旧版本 prompt 是怎么被新版本替换掉的，也不知道一条消息为什么会走这条路而不是那条路。更麻烦的是，一旦你想做优化器、评分器、回退版本、异步消息、动态分流，这种“把一切都藏进大字符串”的方式就会迅速失控。

`Form_zero` 就是在这个背景下出现的。它想解决的不是“怎么再包一层 agent prompt”，而是“怎么把多 Agent 协作做成一个真正有运行时、可追踪、可优化、可重建的系统”。

## 从“对话历史”转向“运行时状态”

我对这个项目最核心的一句话总结是：**把多 Agent 系统的真相，从 prompt 文本转移到结构化运行时状态里。**

在 `Form_zero` 里，系统不是围绕一段 conversation history 组织的，而是围绕四层对象组织的：

- `workflow`
- `program`
- `process`
- `segment`

这四层的划分非常重要。

`workflow` 是模板层。它只回答“系统初始化时应该有哪些 slot，每个 slot 应该引用哪条角色定义”。  
`program` 是运行时实例层。它持有当前 slot 状态，是真正的运行期真相。  
`process` 是执行实体。它不是抽象名字，而是可以被绑定、解绑、轮换、继承上下文的真实实例。  
`segment` 是唯一的上下文物化单位。所有 prefix、suffix、发送痕迹、接收痕迹、历史内容，最终都要变成段落。

这个拆法看起来“重”，但恰恰因为它重，所以很多以前说不清的事情 suddenly 变清楚了。比如：

- 一个 slot 当前到底绑定了哪个 process
- 这个 process 的 prompt 是哪一版
- 它的父版是谁
- 它最近 3 段历史是什么
- 当前 open live bus 里还有什么没 seal
- 某次全局更新到底是 promote 了还是 rollback 了

这些问题如果还停留在“我们把一堆字符串拼起来再发给模型”的阶段，几乎是没法系统回答的。

## 为什么要把角色定义单独抽成 prefix/suffix

很多实现会把角色身份直接写在 prompt 开头。但在 `Form_zero` 里，角色定义被抽成了 `prefix_suffix_definite`。这不是为了形式整洁，而是为了把“角色的静态身份”和“运行中的动态上下文”强行拆开。

静态身份是什么？是这个角色长期不变的工作边界、风格、职责和语气。  
动态上下文是什么？是它此刻正在经历的任务、收到的消息、留下的痕迹和最近的优化结果。

一旦这两者混在一起，系统就会很快变得模糊：你不知道某段文字到底是“这个角色天生该有的”，还是“这次任务临时加进去的”。而把 prefix/suffix 抽出来之后，静态角色定义就有了自己的生命周期，动态上下文则由 process 自己累计。

这也是为什么我后来坚持把 provider 输入从过去的 `full_text` 进一步改造成 `full_context`。模型真正看到的，不应该只是一个大字符串，而应该是带角色结构的上下文数组：

- prefix -> `system`
- suffix -> `assistant`
- `process_prompt` -> `user`
- process stream -> `assistant`

这一步很关键，因为它意味着运行时不再只是“为了拼 prompt 方便”，而是真正开始维护 provider 视角下的结构化上下文。

## program 不是 workflow 的影子，而是运行中的真相

这是 `Form_zero` 里我很喜欢的另一个决定：**workflow 只是模板，program 才是实例真相。**

很多系统在运行期还不停回看原始模板，好像模板永远比实例更“正统”。但我觉得这会让系统越来越僵。一个真正运行起来的程序，本来就应该允许自己长出新的 slot、换掉旧 process、做 branch、做 async clone，甚至演化出和初始模板不完全一样的状态。

所以在 `Form_zero` 里，`program.plan_state_json` 被赋予了很高的地位。它持有：

- 当前 slot 集合
- 每个 slot 对应的 `prefix_suffix_definite_id`
- 上下游 group
- monitor trigger
- `global_phase`
- `process_context_lengths`

这意味着系统不再是“模板在上，实例在下”的服从关系，而是“模板负责初始化，实例负责继续活下去”的关系。这个转变非常重要，因为只有这样，slot 的演化、rebinding、reprompt rotation 才有一致的真相来源。

## process 是真正的一等公民

在很多多 Agent 框架里，slot 就像 process。你说有一个 `planner`，其实就是一条固定 prompt 加一段对话历史。但在 `Form_zero` 里，slot 和 process 被明确拆开：

- slot 是 program 里的位置
- process 是当前占据这个位置的实例

这样一来，很多高级能力才变得自然：

- free process 可以先被创建，之后再绑定
- 某个 slot 可以切换到 successor process
- 旧 process 不需要删除，可以保留为 lineage 节点
- prompt 的版本演化不靠覆盖文本，而靠 process rotation

这个设计的含义很深。它意味着系统优化的对象不是“某段 prompt”，而是“一个有历史、有绑定关系、有上下文的运行实例”。

也正因为 process 是一等公民，`process_prompt` 才值得被单独建表，而不是混进普通历史里。当前占位 prompt 是一个特殊层，它不是普通 assistant 消息，也不是普通 send/receive 痕迹。它就是当前这版 process 的“正式工作说明书”。一旦你接受这一点，后面的 lineage、score、rollback 逻辑都会顺很多。

## 消息为什么不能只是“send 过去就完了”

如果把 `Form_zero` 看成一个运行时，那么消息系统就是它的血液循环。这里最关键的一点是：**消息并不是简单的 A 发给 B，而是一条要经过判断、排队、封段、释放、统计和可能触发全局优化的主链。**

在这个系统里，一条消息至少会经历这些步骤：

1. 构造 `SendMessageRequest`
2. 解析 sender 和 target 的真实 process
3. 判断它们是不是 direct downstream
4. 如果不是，就走 delivery judge
5. 让 judge 在 7 个 `DeliveryAction` 中选一个
6. 进入全局队列
7. 由 dispatcher 路由到 mailbox、clone、bounce 或 drop
8. sender 侧写入 started/completed trace
9. 触发 seal 判断
10. release target mailbox
11. 更新 program 级长度索引
12. 必要时触发 global reprompt

这套设计表面上更复杂，但它换来了一个非常宝贵的东西：**消息行为终于可以被解释。**

你不再需要靠“模型大概就是这样想的”去猜为什么某条消息被 clone 了、为什么某个 slot 忽然开始卡住、为什么这次全局更新之后 planner 换了版本。所有这些事情，理论上都能在 process、segment、binding、plan_state 中找到对应痕迹。

## hidden process：把优化器真正收进系统内部

我一直不太喜欢那种“业务 agent 是系统的一部分，但优化器只是外面一层 wrapper”的做法。因为这样一来，优化器虽然看起来在“管理系统”，却不真正生活在系统里。它没有自己的上下文，没有自己的历史，也没有和其他角色一样的进程身份。

`Form_zero` 在这点上做了一个我很认同的决定：把 `__optimizer__` 和 `__score_judge__` 都做成真实 process。

这两个隐藏进程和普通进程一样，拥有：

- process id
- slot binding
- `process_prompt`
- process stream
- full context

不同只是职责。

`__score_judge__` 更像一个周期性统计器。它不是每条消息都同步跑，而是按周期拿当前累积上下文做评分，输出 `mean_score`。  
`__optimizer__` 则像一个长期观察者。它会收到异常投递事件、prompt snapshot、score update，然后在需要时给出局部 hint 或全局 reprompt 方案。

一旦你把优化器做成系统内的真实角色，很多以前只能靠“外挂脚本”完成的行为，就终于有了统一的消息路径和状态归属。这是我觉得 `Form_zero` 很有意思的一点：它不是在系统外面再套一层“智能控制器”，而是让优化本身也成为系统内部的一条进程链。

## Global reprompt 最值得看的地方：不是改 prompt，而是换 process

这个项目里我最喜欢的设计，也许就是 global reprompt 的处理方式。

很多系统遇到上下文膨胀或任务边界变化时，做法是直接把当前 prompt 改掉。短期看这很方便，但长期代价很大：你会失去版本关系，失去旧 prompt 的引用路径，也没法清楚回答“这版为什么被替换掉”。

`Form_zero` 的思路完全不同。它做的是 successor rotation：

- 新建一个 successor process
- 给 successor 写入新的 `process_prompt`
- 复制最近历史和 open live
- 解绑旧 process
- 将 slot 重新绑定到 successor
- 旧 process 保留在库里，作为 lineage 节点

如果新 prompt 的分数更高，就 promote 到正式占位层。  
如果新 prompt 的分数不够高，就 rollback：让父版 prompt 回到占位层，同时把这次失败的新 prompt 降级成普通 stream 内容。

我觉得这套设计非常成熟，因为它不是把 prompt 当成一段可以随手覆盖的字符串，而是把 prompt 演化视为运行实例的状态转移。这样做虽然实现复杂度更高，但换来的是系统可解释性和版本可追踪性。

## 为什么要维护 program 级的长度索引

上下文压缩和 reprompt 的触发条件，很多人第一反应都是“看当前这个 agent 的上下文是不是超了”。但 `Form_zero` 把问题提得更系统：真正需要看的不是某一个 process，而是整个 program 当前占用了多少上下文预算。

于是长度索引被放进了 `program.plan_state_json.process_context_lengths`。这意味着：

- 长度统计是 program 级的，而不是单点局部判断
- reprompt 可以基于“全局负载”触发
- hidden process 也能一起计入
- rotation 后可以删旧 process id、加新 process id，保持索引同步

这个想法很像操作系统里的资源统计。不是某个线程说“我有点大了”，而是整个进程组的资源使用逼近阈值后，系统开始做全局性的整理动作。

## 我觉得这套架构真正有价值的地方

如果只看 demo 效果，`Form_zero` 可能并不会比那些 prompt 拼得很巧的多 Agent 系统更“花哨”。但它真正有价值的地方不是花哨，而是它开始把多 Agent 协作当成一个工程系统来处理。

这个系统里，重要的不是“下一句 prompt 怎么写”，而是下面这些工程问题终于有了清晰答案：

- 什么是角色定义，什么是实例状态
- 什么是当前绑定，什么是历史 lineage
- 什么是占位 prompt，什么是普通上下文
- 什么是临时任务消息，什么是可复用上下文
- 什么情况下应该全局更新，更新后如何保留旧版本

这些问题在小 demo 里可能都不是问题，但只要系统想继续长大，它们迟早会变成问题。`Form_zero` 的意义就在于，它试图在问题真正爆炸之前，先给出一套可落地的架构答案。

## 结语

我越来越相信，多 Agent 系统如果想从“会跑的 demo”走向“能长期演化的系统”，就必须放弃那种把一切都藏进 prompt 文本里的做法。你需要真实的 process、真实的 binding、真实的 segment、真实的优化链路，以及一套能把这些状态串起来的 runtime。

`Form_zero` 还远没有到终点，它仍然有很多可以继续清理、继续统一、继续验证的地方。但至少它已经很明确地站在了一个我认为正确的方向上：不再把多 Agent 当成几段 prompt，而是把它们当成一个有内核、有调度、有状态机的系统来看待。

如果你也在做多 Agent 架构，也许最值得先问自己的，不是“我还差哪条 prompt”，而是“我的系统里，到底有没有真正的运行时”。

如果你想直接看代码、数据模型和当前实现，可以访问项目仓库：<https://github.com/sheydz/agents_token_os>

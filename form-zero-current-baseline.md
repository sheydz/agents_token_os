# Form_zero 当前版本基线

这份文档只描述当前仓库实际保留的行为，不引用旧专项结论。

## 1. RuntimeHead

- `RuntimeHead.full_text` 是当前 process 的主上下文文本。
- sealed 历史会进入 `full_text`。
- 当前运行中的 open live 也可能进入运行时读取到的 `full_text`。
- 数据库侧的 sealed-only 快照和运行中的 resident head，不要混为一谈。

## 2. Open Live 与 Seal

- `append_segment_delta`
  - 会更新当前运行中的 live 视图
  - 会返回本次请求级的 `live_frame`
- `seal_live_segment`
  - 会把当前 live 段落成 sealed `segment`
  - seal 之后，这段内容就成为持久历史

## 3. Boundary

- `boundary` 是 runtime 认可“这一段到此为止”的事件。
- 它不是模型 token 规则，不靠模型输出某个特殊字符触发。
- 最常见的 boundary 触发方式是：
  - 先 `append_segment_delta`
  - 再 `seal_live_segment`

## 4. History Query

- `history_query` 读取的是：
  - sealed 历史
  - 当前 live bus 里的 open live
- 所以它可以同时看到：
  - 已 seal 的 `work_result`
  - 还没 seal 的 `token_stream`

## 5. Mailbox 结果释放

- 远程工具结果会先变成 `tool_executor_result` 消息。
- 这个结果是否进入目标 `mailbox_message`，取决于：
  - 最终投递动作
  - 目标 process 是否发生 boundary
- 对 `segment_boundary_deliver` 来说，boundary 不是可选项。

## 6. Transport

- `stdin` 和 `/ws` 都是 transport 输入面。
- `/ws` 当前返回的是：
  - 本次 `handle_packet(...)` 直接产生的 `direct_events`
- `/ws` 目前不等价于“全局后台事件广播总线”。
- 如果某个后台异步动作更新了 runtime head，但没有经过当前请求的 `direct_events`，客户端不应假设它一定会被自动推到 socket。

## 7. 当前基线文件

- 测试：
  - [current_runtime_flow.rs](/Users/b2heping/project/project/hb_rob/agents_token_os/Form_zero/tests/current_runtime_flow.rs)
- example：
  - [tool_audit.rs](/Users/b2heping/project/project/hb_rob/agents_token_os/Form_zero/examples/tool_audit.rs)
  - [program_bootstrap_template.rs](/Users/b2heping/project/project/hb_rob/agents_token_os/Form_zero/examples/program_bootstrap_template.rs)
  - [three_slot_bootstrap.rs](/Users/b2heping/project/project/hb_rob/agents_token_os/Form_zero/examples/three_slot_bootstrap.rs)

这几处内容，加上当前代码实现，就是本仓库现在的事实来源。

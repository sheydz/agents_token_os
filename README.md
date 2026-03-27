# Form_zero

Form_zero 是一个以真实 `process`、`segment`、`send_message` 为核心的运行时。

当前仓库只保留一套“当前版本基线”：

- 当前代码
- 当前基线测试
- 当前 example

历史专项观察文档和旧测试已移除，不再作为契约来源。

## 当前版本的 6 个事实

1. `RuntimeHead.full_text` 是运行中上下文的主视图。
2. open live 和 sealed 历史是两层状态，不是同一个持久快照。
3. `boundary` 是 runtime 侧事件，不是模型 token 或特殊字符。
4. `history_query` 读取的是 sealed 历史加当前 live 视图。
5. 结果是否进入目标 `mailbox_message`，取决于 `delivery_action` 和目标侧 boundary。
6. `/ws` 当前只保证返回本次请求触发的 `direct_events`，不把后台异步事件自动广播成通用实时流。

更详细的当前版本说明见 [form-zero-current-baseline.md](/Users/b2heping/project/project/hb_rob/agents_token_os/form-zero-current-baseline.md)。

## 运行环境

测试库：

- `FORM_ZERO_TEST_DATABASE_URL`

example / CLI：

- `FORM_ZERO_DATABASE_URL`

provider 兼容 OpenAI-style `chat/completions`：

- `FORM_ZERO_PROVIDER_BASE_URL`
- `FORM_ZERO_PROVIDER_API_KEY`
- `FORM_ZERO_PROVIDER_MODEL`
- 可选：`FORM_ZERO_PROVIDER_TEMPERATURE`

## 当前基线入口

测试：

```bash
cargo test --test current_runtime_flow -- --test-threads=1
```

example：

```bash
cargo run --example tool_audit
cargo run --example program_bootstrap_template
cargo run --example three_slot_bootstrap
```

## Example 说明

- `tool_audit`
  - 输出当前工具注册表快照，并跑一组当前版本可复现的工具样例
- `program_bootstrap_template`
  - 通用三槽 `planner / writer / reviewer` 初始化模板
- `three_slot_bootstrap`
  - 固定主题的三槽协作演示

## 推荐环境变量

```bash
export FORM_ZERO_TEST_DATABASE_URL='postgres://postgres@127.0.0.1:55432/form_zero_current_test'
export FORM_ZERO_DATABASE_URL='postgres://postgres@127.0.0.1:55432/form_zero_current_example'
export FORM_ZERO_PROVIDER_BASE_URL='https://dashscope.aliyuncs.com/api/v2/apps/protocols/compatible-mode/v1'
export FORM_ZERO_PROVIDER_API_KEY='YOUR_PROVIDER_KEY'
export FORM_ZERO_PROVIDER_MODEL='tongyi-xiaomi-analysis-flash'
export FORM_ZERO_PROVIDER_TEMPERATURE='0'
```

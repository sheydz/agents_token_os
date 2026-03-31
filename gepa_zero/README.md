# GEPA Zero Async Multi-Agent

`gepa_zero` 现在是一个常驻式异步多 agent 原型机：

- 普通 agent 消息先进入全局消息队列
- 再经过 `Form_zero` 风格的优先级/投递判断器
- 然后进入目标 agent 的 mailbox，等待释放
- `score-judge` 每 10 秒慢速更新一次平均默契分
- 非默认分流动作会触发系统级局部优化消息
- 当上下文压力达到阈值时，会触发一次全体 agent 的统一 reprompt

## 主要文件

- `agents.json`
- `tasks.json`
- `menu_repository.json`
- `ingredient_repository.json`
- `prompts/master_system_prompt.txt`
- `prompts/apprentice_system_prompt.txt`
- `prompts/delivery_judge_system_prompt.txt`
- `prompts/judge_system_prompt.txt`
- `prompts/optimizer_meta_system_prompt.txt`
- `prompts/reflection_suffix.txt`
- `prompts/reprompt_suffix.txt`
- `src/main.rs`
- `src/provider.rs`

## 环境变量

- `GEPA_ZERO_PROVIDER_API_KEY`
- `GEPA_ZERO_PROVIDER_BASE_URL`
  - 默认 `https://dashscope.aliyuncs.com/compatible-mode/v1`
- `GEPA_ZERO_PROVIDER_MODEL`
  - 默认 `qwen3-max-2026-01-23`
- `GEPA_ZERO_TEMPERATURE`
  - 默认 `0.1`
- `GEPA_ZERO_RUN_DIR`
  - 默认 `./runs/latest`

## 运行

```bash
cd gepa_zero
cargo test
cargo run
```

## 运行中 artifacts

每个任务目录会持续刷新这些文件：

- `thread.json`
- `agent_turns.json`
- `judge_views.json`
- `reflections.json`
- `reprompts.json`
- `run_state.json`

任务结束后还会补：

- `final_prompts_end_of_task.json`
- `accepted_prompts_after_task.json`

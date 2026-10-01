# 泄洪闸门操作协调服务

这是一个无外部依赖的 Go 后端，用本地内存仓库协调闸门登记、开度调整命令、安全联锁复核和执行结果。服务强调单闸门活动命令互斥、原子状态转换、执行令牌生命周期和稳定错误传播。

## 运行

```sh
go build ./...
go run ./cmd/server --addr 127.0.0.1:18119
```

健康入口为 `GET http://127.0.0.1:18119/health`。

## API

```text
POST /gates
GET  /gates/{id}
POST /commands
GET  /commands/{id}
POST /commands/{id}/interlock
POST /commands/{id}/execute
```

登记闸门示例：

```json
{
  "id": "gate-east",
  "name": "东侧泄洪闸",
  "max_aperture": 4.5,
  "initial_aperture": 0.5
}
```

提交命令示例：

```json
{
  "gate_id": "gate-east",
  "target_aperture": 2.25,
  "reason": "上游水位调整",
  "requested_by": "dispatcher-lin"
}
```

联锁复核示例：

```json
{
  "verdict": "clear",
  "verifier": "safety-wu",
  "note": "上下游条件满足"
}
```

执行示例：

```json
{
  "operator": "operator-chen",
  "token": "复核响应中的 execution_token",
  "request_id": "操作员为本次动作生成的唯一标识，重试保持不变",
  "simulate_fault": false
}
```

### 令牌一次性与同请求重试

- `token` 只授权一次执行边界跨越（approved → executing）。边界跨越的一刻令牌即作废，命令结束后不能用它再次启动任何移动。
- `request_id` 标识操作员的同一次动作。响应丢失后，操作员用**相同的 `request_id` 和相同的 `token`** 重试，服务不会重新执行，而是返回原终态（命令、闸门快照原样），响应中 `idempotent_replay` 为 `true`。
- 未生效的失败（空操作员、令牌不匹配、缺少 `request_id` 等）不消耗令牌：修正后可用同一 `request_id` 正常执行。
- 不同的 `request_id` 复用已结束命令的旧令牌，会被当作一次新动作拒绝（409 `command_already_completed` / `command_already_aborted`）。
- 同一请求近乎同时到达的多个重试，只有一个跨越执行边界，其余全部确认同一终态。

## 目录

- `cmd/server`：服务进程。
- `cmd/workflowcheck`：四条工作流的真实 HTTP 检查入口。
- `internal/bootstrap`：依赖装配。
- `internal/contracts`：公开数据契约。
- `internal/coordination`：业务工作流。
- `internal/domain`：实体、不变量和状态机。
- `internal/httpapi`：路由、解码和错误映射。
- `internal/storage`：互斥保护的内存仓库。

## 工作流检查

```sh
go run ./cmd/workflowcheck --workflow register-gate
go run ./cmd/workflowcheck --workflow request-gate-command
go run ./cmd/workflowcheck --workflow review-safety-interlock
go run ./cmd/workflowcheck --workflow execute-gate-command
```

这些检查会创建临时 HTTP 服务并调用真实路由。它们不调用外部网络，也不依赖数据库。

## 测试状态

测试有意延后。当前交付仅包含初始化基线和生产级 smoke 检查；后续任务阶段会添加自动化测试与红绿验证，不应把当前没有测试文件视为项目未完成。

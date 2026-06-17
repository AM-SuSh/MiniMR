# MiniMR — 基于 Go 的轻量级分布式 MapReduce 框架

## 一、项目简介

MiniMR 是一个用 Go 实现的分布式 MapReduce 框架，采用经典 **Master / Worker** 架构：

| 角色 | 职责 |
|------|------|
| **Master（JobTracker）** | 接收作业、切分输入、调度 Map/Reduce 任务、跟踪进度、容错与 checkpoint |
| **Worker（TaskTracker）** | 通过 RPC 拉取任务，执行 Map/Reduce UDF，读写共享工作目录中的中间文件 |
| **Client** | 通过 HTTP 向 Master 提交作业、轮询状态、获取结果；支持恢复中断作业 |
| **Web Dashboard** | 浏览器可视化调度进度、Worker 状态、决策日志与历史作业 |

核心能力包括：UDF 注册、Combine 预聚合、Reduce 提前调度、Shuffle 优化（二进制编码 / gzip / 有序归并）、任务超时重试、Worker 心跳与黑名单、输入故障与 Worker 故障分离、Master checkpoint 与作业恢复。

## 二、环境要求

### 必需

| 依赖 | 版本 | 说明 |
|------|------|------|
| **Go** | 1.21+ | 编译与运行全部组件 |
| **操作系统** | Windows / Linux / macOS | Windows 与 WSL 均可原生编译运行分布式模式 |

### 可选

| 依赖 | 版本 | 用途 |
|------|------|------|
| **Python** | 3.8+ | `bridge/` 脚本提交任务、爬虫流水线 |
| **pip 包 requests** | — | Python 桥接调用 HTTP API |
| **WSL / bash** | — | 运行 `scripts/*.sh` 一键演示 |
| **Linux** | — | Go `-buildmode=plugin` 动态加载 UDF（Windows 不支持） |

### 端口与目录

默认占用端口：

- **8080**：Master RPC（Worker 连接）
- **8081**：Master HTTP（Client、Dashboard、REST API）

运行后会在项目根目录自动创建（或需手动创建）：

| 目录 | 说明 |
|------|------|
| `mr-work/`（或自定义 `workdir`） | 作业工作目录：中间文件 `mr-*`、最终输出 `mr-out-*` |
| `logs/` | 每作业调度审计日志 `{job_id}.log`（JSON Lines） |
| `checkpoints/` | 每作业 checkpoint `{job_id}.json`，用于 Master 崩溃后恢复 |
| `bin/` | `scripts/` 编译产物（可选） |

## 三、目录架构

```
MiniMR/
├── main.go                 # 单机模式入口（本地 MapReduce，无需 Master/Worker）
├── go.mod
├── README.md
│
├── cmd/                    # 可执行程序入口
│   ├── master/             # Master 进程：RPC + HTTP + Dashboard 静态资源
│   ├── worker/             # Worker 进程：连接 Master、执行任务
│   ├── client/             # CLI 客户端：提交 / 恢复 / 等待作业完成
│   └── shuffle_bench/      # Shuffle 性能基准工具
│
├── mr/                     # 核心框架
│   ├── master.go           # Master 调度、HTTP API、作业生命周期
│   ├── worker.go           # Worker 主循环、任务执行
│   ├── task.go             # 任务状态机、超时重试、checkpoint 周期保存
│   ├── rpc.go              # Master ↔ Worker RPC 协议
│   ├── splitter.go         # 输入文件切分
│   ├── mapper.go / reducer.go / combiner.go
│   ├── local.go            # 单机 RunLocal 实现
│   ├── checkpoint.go       # checkpoint 读写、WorkDir 对齐、作业恢复
│   ├── joblog.go           # logs/ 持久化调度决策
│   ├── dashboard.go        # Dashboard API 数据聚合
│   ├── failure.go          # 故障分类（输入 / Worker / 中间文件）
│   ├── events.go           # 决策事件类型定义
│   ├── plugin_linux.go     # Linux plugin 加载
│   └── *_test.go           # 单元与集成测试
│
├── udf/                    # 用户定义函数（Map / Reduce / Combine）
│   ├── registry.go         # init() 注册表
│   ├── wordcount.go        # 词频统计 UDF
│   ├── crawl_clean.go      # 爬虫数据清洗 UDF
│   └── plugins/            # Linux .so 插件源码（可选）
│
├── web/                    # Dashboard 前端（由 Master HTTP 托管）
│   ├── index.html
│   ├── app.js              # 轮询、历史作业、断连/恢复会话日志
│   └── styles.css
│
├── bridge/                 # Python HTTP 桥接
│   └── submit_job.py       # 提交并等待作业完成
│
├── scripts/                # Shell 辅助脚本
│   ├── run_master.sh
│   ├── run_workers.sh
│   └── run_wordcount.sh    # 一键编译 + 启 Master/Worker + 提交 WordCount
│
├── testdata/               # 测试与演示输入数据
├── docs/                   # 补充文档（使用说明、检查报告等）
├── logs/                   # 运行时生成：作业调度日志
└── checkpoints/            # 运行时生成：作业 checkpoint
```

### 运行时数据流

```
输入文件 ──split──▶ Map 任务 ──▶ mr-{map}-{reduce}（WorkDir）
                                      │
                                      ▼
                              Reduce 任务 ──▶ mr-out-{reduce}
```

Master 将调度决策写入 `logs/{job_id}.log`；作业状态快照写入 `checkpoints/{job_id}.json`。

## 四、编译与部署

### 4.1 获取代码

```bash
git clone git@github.com:AM-SuSh/MiniMR.git
cd MiniMR
```

### 4.2 编译

```bash
# 分布式三件套
go build -o bin/master ./cmd/master
go build -o bin/worker ./cmd/worker
go build -o bin/client ./cmd/client

# 单机模式无需单独二进制，直接 go run . 即可
```

Windows PowerShell 下可将 `bin/master` 换为 `bin\master.exe` 等。

### 4.3 单机模式

适合本地调试 UDF 与分片逻辑：

```bash
go run . -input testdata/input.txt -output mr-out-standalone -nreduce 3
```

python集成：

```
python bridge/submit_job.py --input testdata/input.txt
```

### 4.4 分布式部署

在**项目根目录**执行（Master 需能访问 `web/`、`logs/`、`checkpoints/` 相对路径）。

**终端 1 — Master**

```bash
go run ./cmd/master -port :8080 -http :8081
# 或./bin/master -port :8080 -http :8081
```

启动后访问 Dashboard：**http://localhost:8081/**

**终端 2 — Worker**

```bash
go run ./cmd/worker -master localhost:8080 -id worker-1
go run ./cmd/worker -master localhost:8080 -id worker-2
```

`-id` 省略时自动使用 `主机名-PID`。

**终端 3 — 提交作业**

```bash
go run ./cmd/client \
  -master-http http://localhost:8081 \
  -input testdata/input.txt \
  -nreduce 3
```

### 4.5 WSL / 脚本一键部署

```bash
# 仅启动 Master
bash scripts/run_master.sh

# 启动 N 个 Worker（默认 3 个）
bash scripts/run_workers.sh 3 localhost:8080

# WordCount 完整演示（编译 + Master + Worker + Client）
bash scripts/run_wordcount.sh
```

### 4.6 Plugin测试

```bash
export CGO_ENABLED=1
go build -buildmode=plugin -o wordcount_mapper.so ./udf/plugins/wordcount_mapper.go
go build -buildmode=plugin -o wordcount_reducer.so ./udf/plugins/wordcount_reducer.go
```

```bash
# 终端 1 — Master
go run ./cmd/master -port :8080 -http :8081

# 终端 2 — Worker（加载 plugin）
go run ./cmd/worker -master localhost:8080 -id worker-1 \
  -plugin wordcount_mapper.so -plugin-name wordcount

# 终端 3 — 提交任务
go run ./cmd/client \
  -master-http http://localhost:8081 \
  -input testdata/input.txt \
  -nreduce 3 \
  -map wordcount_map \
  -reduce wordcount_reduce \
  -combine wordcount_combine
```

### 4.7 测试

```bash
# Master
go run ./cmd/master -port :8080 -http :8081

# Worker
go run ./cmd/worker -master localhost:8080 -id worker-1

# Client
go run ./cmd/client `
  -master-http http://localhost:8081 `
  -input testdata/pd.train.part1 `
  -split 67108864 `
  -nreduce 3 `
  -map wordcount_map `
  -reduce wordcount_reduce `
  -combine wordcount_combine `
  -workdir mr-work-pd `
  -slowstart 0.6
```



## 五、使用指南

### 5.1 Client 命令行参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `-master-http` | `http://localhost:8081` | Master HTTP 基址 |
| `-input` | — | 逗号分隔的输入文件列表（提交新作业时必填） |
| `-recover` | — | 恢复指定 Job ID，而非提交新作业 |
| `-nreduce` | `3` | Reduce 分区数 |
| `-map` / `-reduce` / `-combine` | wordcount 系列 | UDF 名称 |
| `-split` | `0` | 分片大小（字节），`0` 为默认（约 32 MiB） |
| `-workdir` | `mr-work` | 工作目录 |
| `-slowstart` | `0` | Reduce 慢启动阈值，`0` 表示默认 0.8 |

**示例：大文件分片 WordCount**

```powershell
go run ./cmd/client `
  -master-http http://localhost:8081 `
  -input testdata/pd.train.part1 `
  -split 67108864 `
  -nreduce 3 `
  -map wordcount_map `
  -reduce wordcount_reduce `
  -combine wordcount_combine `
  -workdir mr-work-pd `
  -slowstart 0.6
```

Client 在作业 `failed` 时以 **退出码 1** 结束；`completed` 时打印 JSON 结果。

### 5.2 Web Dashboard

1. 启动 Master 后浏览器打开 `http://localhost:8081/`。
2. 在顶部填写 **Master HTTP** 地址，点击「连接」。
3. **Job ID** 留空则跟随最新作业；填入 ID 或点击「历史作业」可查看已完成/失败记录。
4. 面板包含：作业进度、Map/Reduce 任务表、Worker 列表、调度决策日志。
5. Master 断开时，当前作业会显示「Master 已退出 / 连接断开」；重连后显示恢复横幅（按作业隔离，不影响其他历史作业日志）。

### 5.3 作业恢复

当 Master 异常退出时，运行中作业的状态保存在 `checkpoints/{job_id}.json`。

**恢复步骤：**

1. 重新启动 Master（会自动扫描 checkpoint，日志中列出可恢复作业）。
2. 确保原 Worker 仍在运行（或重新启动 Worker）。
3. 任选一种方式触发恢复：

```bash
# 方式 A：Client 专用恢复模式
go run ./cmd/client -master-http http://localhost:8081 -recover <job_id>

# 方式 B：HTTP API
curl -X POST http://localhost:8081/api/recover \
  -H "Content-Type: application/json" \
  -d "{\"job_id\":\"<job_id>\"}"

# 方式 C：查询可恢复列表
curl http://localhost:8081/api/recoverable
```

Master 在 `/api/status` 与 `/api/dashboard` 轮询时也会 **自动尝试恢复** `recoverable` 状态的作业。

恢复时会根据 WorkDir 中已有中间文件对齐已完成任务，重置 `InProgress` 任务后继续调度。

### 5.4 Python 提交作业

```bash
pip install requests
python bridge/submit_job.py --master http://localhost:8081 --input testdata/input.txt
```

### 5.5 自定义 UDF

1. 在 `udf/` 下实现 Map、Reduce（及可选 Combine）函数。
2. 在 `udf/registry.go` 的 `init()` 中注册名称。
3. Worker 启动时通过 `_ "mapreduce/udf"` 自动加载。
4. 提交作业时在 `-map` / `-reduce` / `-combine` 或 API JSON 中指定函数名。

## 六、HTTP API 一览

| 方法 | 路径 | 说明 |
|------|------|------|
| `POST` | `/api/job` | 提交新作业，返回 `job_id` |
| `GET` | `/api/status?job=<id>` | 作业状态与进度（空 job 为最新） |
| `GET` | `/api/result?job=<id>` | 作业完成后的结果摘要 |
| `GET` | `/api/dashboard?job=<id>` | Dashboard 全量数据（含决策、Worker 快照） |
| `GET` | `/api/recoverable` | 列出可恢复作业 |
| `POST` | `/api/recover` | Body: `{"job_id":"..."}` 恢复作业 |
| `GET` | `/` | Web Dashboard 静态页面 |

**提交作业请求体示例：**

```json
{
  "input_files": ["testdata/input.txt"],
  "n_reduce": 3,
  "map_func": "wordcount_map",
  "reduce_func": "wordcount_reduce",
  "combine_func": "wordcount_combine",
  "split_size": 0,
  "work_dir": "mr-work",
  "reduce_slow_start": 0.8
}
```

**作业状态 `state` 取值：** `running` · `completed` · `failed` · `recoverable`

## 七、测试

### 7.1 测试

- 集成测试

```bash
go test ./... -v -count=1
```

- shuffle对比测试

```bash
go run ./cmd/shuffle_bench -nreduce 5
```

### 7.2 测试覆盖范围

| 包 / 文件 | 主要内容 |
|-----------|----------|
| `mr_test.go` | 分片、UDF、分布式 WordCount、超时重试、黑名单、输入故障、作业失败中止 |
| `mr/checkpoint_test.go` | checkpoint 存取、WorkDir 对齐、恢复调度 |
| `mr/checkpoint_history_test.go` | 历史作业从 checkpoint 加载 |
| `mr/master_recover_test.go` | 状态轮询自动恢复 |
| `mr/master_status_test.go` | failed 状态不被覆盖 |
| `mr/failure_test.go` | 故障原因分类 |
| `mr/dashboard_test.go` | 失败作业 Dashboard 横幅 |

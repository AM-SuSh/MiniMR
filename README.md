# MiniMR — Go 分布式 MapReduce 框架

基于 Master/Worker 架构的分布式 MapReduce 实现，支持 UDF 注册、Combine 优化、Reduce 提前调度、容错与 Python 桥接。

## 架构

```
Client/Python ──HTTP──▶ Master (JobTracker)
                           │
                     RPC  │  AssignTask / Report / Heartbeat
                           ▼
                    Worker 集群 (TaskTracker)
                           │
                     共享文件系统 (中间数据 mr-{map}-{reduce})
```

## 快速开始

### 单机模式（向后兼容）

```bash
go run . -input testdata/input.txt -output mr-out-standalone
```

### 分布式模式

**Windows 原生或 WSL 均可编译运行：**

```bash
# 终端 1 — 启动 Master
go run ./cmd/master -port :8080 -http :8081

# 终端 2 — 启动 Worker（可启动多个）
go run ./cmd/worker -master localhost:8080 -id worker-1

# 终端 3 — 提交任务
go run ./cmd/client \
  -master-http http://localhost:8081 \
  -input testdata/input.txt \
  -nreduce 3
```

### WSL 一键演示

```bash
wsl bash scripts/run_wordcount.sh
wsl bash scripts/run_crawl_clean.sh
```

## 目录结构

| 路径 | 说明 |
|------|------|
| `mr/` | 核心框架：Master、Worker、RPC、分片、任务状态机 |
| `udf/` | UDF 注册表与实现（WordCount、爬虫清洗） |
| `cmd/master` | Master 进程入口 |
| `cmd/worker` | Worker 进程入口 |
| `cmd/client` | CLI 任务提交客户端 |
| `bridge/` | Python HTTP 桥接脚本 |
| `scripts/` | 运行与测试脚本 |
| `testdata/` | 测试数据 |
| `main.go` | 单机模式入口 |

## UDF

| 名称 | Map | Reduce | Combine |
|------|-----|--------|---------|
| WordCount | `wordcount_map` | `wordcount_reduce` | `wordcount_combine` |
| 爬虫清洗 | `crawl_clean_map` | `crawl_clean_reduce` | — |

新增 UDF：在 `udf/` 下实现函数并在 `registry.go` 的 `init()` 中注册。

## Python 集成

```bash
pip install requests
python bridge/submit_job.py --input testdata/input.txt
python bridge/crawler_pipeline.py
```

## Plugin 模式（仅 Linux/WSL）

```bash
GOOS=linux go build -buildmode=plugin -o wordcount_mapper.so udf/plugins/wordcount_mapper.go
GOOS=linux go build -buildmode=plugin -o wordcount_reducer.so udf/plugins/wordcount_reducer.go
```

## 测试

```bash
go test ./... -v -count=1
```

## 优化特性

- **Reduce 提前调度**：某 reduce 分区所有 Map 输出就绪即可开始 Reduce
- **Combine**：Map 端本地预聚合，减少 Shuffle 数据量
- **Shuffle 优化**：Map 端按 key 排序写入，Reduce 端归并
- **容错**：任务超时重分配、Worker 心跳检测、中间文件原子写入

## 环境要求

- Go 1.21+
- Python 3.8+（可选，用于 bridge 脚本）
- WSL（可选，用于 bash 脚本和 plugin 模式）

# sharded-checkpoint-store-go

一个纯本地文件系统的分片检查点（checkpoint）存储层，仅依赖 Go 标准库。
周期运行的作业把状态分片写盘；进程崩溃或重启后，从最近一个**完整提交**
的检查点继续，半成品永远不会被当成可用数据。

## 存储布局

```
<root>/
  <jobID>/
    checkpoints/
      <checkpointID>/
        shard-000000          # 已提交的分片文件
        shard-000001
        manifest.json         # 提交记录，最后原子落盘
      <checkpointID>/         # 写入中的检查点（对读侧不可见）
        shard-000000.tmp
        manifest.json.tmp
```

`manifest.json` 内容：

```json
{
  "format_version": 1,
  "job_id": "job-a",
  "checkpoint_id": "ckpt-2",
  "base_checkpoint": "ckpt-1",
  "shards": [
    {"index": 0, "file": "shard-000000", "size": 128, "sha256": "...", "offset": 4096}
  ],
  "created_at": "..."
}
```

每个分片记录 `size` 与 `sha256` 用于完整性校验；`offset` 是该分片逻辑流
的结束位点，作为增量续写 / 断点续传的游标。`base_checkpoint` 指向前一个
检查点，构成增量链。

## 一致性语义

- **原子提交**：分片先写 `*.tmp` 并 fsync，提交时先 rename 成分片最终名，
  最后把 `manifest.json.tmp` 原子 rename 为 `manifest.json` 并 fsync 目录。
  manifest 的存在即"已提交"的唯一判据。
- **全有或全无**：提交在任何一步被中断（崩溃、取消、失败），该检查点目录
  没有 manifest，对 `Latest`/`List`/`Open` 均不可见；`Open` 显式返回
  `ErrIncomplete`，绝不回退到部分数据。
- **读侧隔离**：读只能看到已提交的完整状态。写入中的 `*.tmp` 与无 manifest
  的目录对读完全透明。
- **写侧互斥**：同一作业同一时刻只允许一个活跃 `Writer`，第二个 `Begin`
  返回 `ErrWriterConflict`。
- **幂等重试**：同一 checkpoint ID 以完全相同的内容重复提交是 no-op，
  结果与只提交一次一致；内容不同则返回 `ErrCommitConflict`。
- **回退拒绝**：分片 `offset` 低于基线中同分片位点，或 `BeginFrom` 指定的
  基线落后于最新已提交检查点，都返回 `ErrRegression`，不会悄悄从头重传。

## 恢复流程

1. `NewStore(dir)` 重新打开存储（进程重启后）。
2. `Latest(jobID)` 找到最近一个含 manifest 的检查点；无则返回 `ErrNotFound`。
3. 用 `ReadShard` 逐个读取分片（每次读取都做存在性、大小、校验和验证），
   取各分片 `offset` 作为续写游标。
4. `Cleanup(jobID)` 清理中断写入留下的无 manifest 残骸目录。
5. `Begin`/`BeginFrom` 基于最新检查点开始下一轮增量写入。

## 错误分类

所有错误为 `*checkpoint.Error`，携带 `Kind`、`JobID`、`CheckpointID` 及
`Shard`（分片相关时），可用 `errors.Is` 判定：

| Kind | 哨兵 | 含义 |
|---|---|---|
| `not_found` | `ErrNotFound` | 作业 / 检查点 / 分片不存在 |
| `incomplete` | `ErrIncomplete` | 目录存在但从未提交（中断的写入） |
| `shard_missing` | `ErrShardMissing` | 已提交检查点引用的分片文件丢失 |
| `shard_truncated` | `ErrShardTruncated` | 分片文件比 manifest 记录短 |
| `shard_corrupted` | `ErrShardCorrupted` | 校验和 / 大小不符（内容被改动） |
| `regression` | `ErrRegression` | 位点或基线回退 |
| `commit_conflict` | `ErrCommitConflict` | 同 ID 不同内容的重复提交 |
| `writer_conflict` | `ErrWriterConflict` | 同一作业已有活跃写入者 |

损坏类错误都定位到具体分片号，且不会静默回退到旧版本。

## API 速览

```go
s, _ := checkpoint.NewStore("/var/lib/ckpt")

w, _ := s.Begin("job-a", "ckpt-3")        // 或 BeginFrom 指定基线
w.WriteShard(0, offset0, data0)           // 多分片可并行写
w.WriteShard(1, offset1, data1)
err := w.Commit()                          // 原子提交；或 w.Abort() 放弃

r, _ := s.Latest("job-a")                  // 恢复：最近完整检查点
data, meta, _ := r.ReadShard(0)            // 读取即校验
```

## 测试

```sh
go test -race -v ./...
```

覆盖：多分片并行写入、提交中断（通过 commit 阶段 failpoint 注入，确定性
复现，无 sleep）、主动放弃、幂等重复提交与冲突、位点/基线回退、增量续写
与重启恢复、并发读写互斥、分片丢失/截断/篡改/manifest 丢失的定位。测试
日志以 "scenario:" / "case ok:" 标注各场景与用例结果。

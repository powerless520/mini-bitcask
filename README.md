# mini-bitcask

> 用纯 Rust（零依赖，仅 std）实现的 Bitcask 风格 KV 存储引擎，核心不到 400 行。
> 这是一个**学习型开源项目**：目标不是和 RocksDB 竞争，而是把「造存储引擎」的全过程开源出来。

[![Rust](https://img.shields.io/badge/rust-1.70%2B-orange)](https://www.rust-lang.org/)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue)](LICENSE)

## 为什么做这个

Bitcask 是 Basho（riak 数据库）2009 年提出的存储模型，设计极其简洁：

- **写路径**：所有写入 append-only 追加到日志文件 → 顺序写，吞吐极高
- **内存索引**：`HashMap<key, offset>`，offset 指向最新值的磁盘位置
- **读路径**：查内存索引 → 按偏移量 seek 读取，一次 IO 搞定
- **启动恢复**：重放日志重建索引

```
set("name", "cyrus")                    data.log
    │                                  ┌─────────────────────────────┐
    ▼                                  │ [name] PUT "cyrus"          │
 index:                                │ [name] PUT "v2"   ← 垃圾    │
   name ──► offset 32 ──────────────►  │ [name] DEL        ← 垃圾    │
                                       │ [name] PUT "cyrus2" ← 最新  │
get("name")                            └─────────────────────────────┘
    查索引 → seek → 读到 "cyrus2"
```

代价是日志里会积累垃圾数据（旧版本值、墓碑），需要定期 **merge/compaction** ——
这恰好是本项目 Roadmap 的第一个研究课题。

## 快速开始

```bash
# 运行测试
cargo test

# 交互式体验
cargo run -- ./mydata
```

```
> set name cyrus
OK (key=name)
> get name
cyrus
> stats
live_keys=1 log_bytes=30 (~0.0 KiB)
> exit
```

## 代码导读

| 文件 | 内容 | 学习点 |
|------|------|--------|
| `src/lib.rs` | 引擎核心 | append-only 文件设计、二进制协议编码、日志重放、`O_APPEND` 偏移量陷阱 |
| `src/main.rs` | REPL CLI | `std::io` 交互、错误处理 |

刻意保留的教学性设计：

1. **零依赖**：序列化用裸的字节序编码（大端），让你看清“协议”的本质
2. **脏尾容忍**：`read_entry_at` 读到不完整条目时静默停止 —— 崩溃后最后一半写入会被丢弃（这正是 append-only 的崩溃安全性）
3. **防御性长度校验**：`MAX_KEY` / `MAX_VALUE` 上限，防止损坏日志触发疯狂内存分配

## Roadmap（每个课题都是一个独立 PR）

- [x] **v0.2 CRC 校验**（已实现）：每条 entry 前置 4 字节 CRC-32（手写查表法，零依赖），
      重放时逐条校验；损坏尾部直接截断丢弃，保证索引永远指向完整记录。
      实测教训：截断是必须的 —— 否则 O_APPEND 会把新记录写到损坏数据之后，下次重放仍会在损坏处停止，新数据永远不可见
- [ ] **v0.3 Merge/Compaction**：新开只读文件 + active 文件，把存活数据重写进新文件，删除垃圾 —— 学习 Bitcask 论文 §3
- [ ] **v0.4 多文件 + hint file**：日志切分、merge 时生成 hint file 加速重启
- [ ] **v0.5 并发句柄**：`Arc<RwLock<Bitcask>>` 或多 reader 单 writer（Bitcask 原文的设计）
- [ ] **v0.6 索引优化**：keydir 换成 `BTreeMap` 支持范围查询 / 尝试内嵌布隆过滤器
- [ ] **v0.7 基准测试**：用 `criterion` 跑 set/get 压测，对比纯 HashMap 基线

## 相关阅读

- Bitcask 论文：*Bitcask — A Log-Structured Hash Table for Fast Key/Value Data* (Basho, 2010)
- [mini-lsm](https://github.com/skyzh/mini-lsm)：本项目灵感来源之一，进阶可去做 LSM-Tree
- [build-your-own-x](https://github.com/codecrafters-io/build-your-own-x)：更多"造轮子学习"路线

## License

MIT

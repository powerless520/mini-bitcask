# mini-bitcask

> 用纯 Rust（零依赖，仅 std）实现的 Bitcask 风格 KV 存储引擎，核心约 1010 行（含 24 个测试共 1659 行）。
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
这正是 v0.3 做的事。

## 文件布局

```
mydata/
├── active.log            唯一可写文件，append-only 追加新写入
├── segment-000001.log    只读分段（merge 产物，写完后永不修改）
├── hint-000001.log       分段的索引备份（key + 位置，不含 value），加速重启
├── bloom-000001.filter   分段的布隆过滤器，用于负查询（可能不存在 -> 一定不存在）
└── merge-000002.tmp      merge 中的临时文件，rename 后成为正式分段
```

内存索引保存 `key -> (file_id, offset)`（BTreeMap，有序），读操作按位置直接 seek，一次 IO 拿到 value。
重启时：有完好的 hint 就直接用它建索引（不碰数据文件）；否则重放数据。

## 范围查询与前缀扫描

```rust
// [start, end) 左闭右开，结果按 key 升序
for (k, v) in db.range("user:1", "user:99")? { /* ... */ }

// 前缀扫描：常用于按命名空间枚举
let users = db.scan_prefix("user:")?;
```

```
> prefix user:
user:1 = alice
user:10 = carol
user:2 = bob
```

注意：字符串字典序下 `user:10` 排在 `user:2` 之前——想要数值序请用零填充 key。

## 快速开始

```bash
# 运行测试
cargo test

# 交互式体验
cargo run -- ./mydata
```

```
> set user alice
OK (key=user)
> set user bob
OK (key=user)
> set lang rust
OK (key=lang)
> stats
live_keys=2 total_bytes=104 (~0.1 KiB) segments=0 active=104 B
> merge
merge 完成: 104 B -> 41 B (压缩到 39.4%)
> get user
bob
> exit
```

## Merger 的崩溃安全性

merge 不是"原地覆写"，而是经典的三步走：

1. **写临时文件** `merge-<id>.tmp`，逐条重写存活数据 + `fsync`
2. **原子 rename** 成 `segment-<id>.log`（同一文件系统内 rename 是原子的）
3. **删除旧分段 + 截断 active.log**

任何时刻崩溃都安全：

| 崩溃时机 | 结果 |
|---------|------|
| rename 之前 | 旧文件完好，临时文件下次启动时被自动清理 |
| rename 之后、删除之前 | 新旧文件并存，重放按分段 id 升序 + active 最后，新分段数据覆盖旧数据，结果正确 |

## 代码导读

| 文件 | 内容 | 学习点 |
|------|------|--------|
| `src/lib.rs` | 引擎核心 | append-only 文件设计、二进制协议编码、CRC 校验、多文件 merge、`O_APPEND` 偏移量陷阱 |
| `src/main.rs` | REPL CLI | `std::io` 交互、错误处理 |

刻意保留的教学性设计：

1. **零依赖**：序列化用裸的字节序编码（大端），让你看清“协议”的本质
2. **脏尾容忍**：`read_entry_at` 读到不完整条目时静默停止 —— 崩溃后最后一半写入会被丢弃（这正是 append-only 的崩溃安全性）
3. **防御性长度校验**：`MAX_KEY` / `MAX_VALUE` 上限，防止损坏日志触发疯狂内存分配

## Roadmap（每个课题都是一个独立 PR）

- [x] **v0.2 CRC 校验**：每条 entry 前置 4 字节 CRC-32（手写查表法，零依赖），
      重放时逐条校验；损坏尾部直接截断丢弃，保证索引永远指向完整记录。
      实测教训：截断是必须的 —— 否则 O_APPEND 会把新记录写到损坏数据之后，下次重放仍会在损坏处停止，新数据永远不可见
- [x] **v0.3 Merge/Compaction**：active.log + 只读分段的多文件布局；
      存活数据重写进新分段 → 原子 rename → 删除旧文件（Bitcask 论文 §3）。
      实测：10 轮覆盖写后 merge，104 B → 41 B，压缩到 39.4%
- [x] **v0.4 Hint file**：merge 时顺便生成 `hint-<id>.log`（key + 位置 + 长度，不含 value）。
      重启时若 hint 结构完整则直接建索引，完全跳过数据扫描；
      任一校验失败（残缺/越界）则整体作废，回退全量重放 —— 宁可不加速也不能读错。
      实测教训：残缺 hint 必须回退而不是报错 —— 容错是 hint 信任策略的一部分
- [x] **v0.5 并发句柄**：`Bitcask` 变成 `Clone + Send + Sync` 共享句柄（内部 `Arc`），多读单写。
      锁顺序全局统一（active → index → segments）；get 校验读出的 key 并在位置过期时重试。
      实测教训 ×2：① `stats()` 反序锁（先 index 后 active）会与写路径形成死锁环；
      ② 并发 merge 下索引位置会过期，读出的 entry 必须校验 key，否则会把别的 key 的值读给调用方
- [x] **v0.6 有序索引**：keydir 从 `HashMap` 换成 `BTreeMap`。
      单点读写 O(log n) 的代价，换来 key 天然有序、范围查询 `[a, b)`、前缀扫描；
      `range`/`scan_prefix` 先在索引读锁内收集 key（短持锁）再逐个取值，
      不阻塞写入且自动继承并发 merge 的重试逻辑
- [x] **v0.7 布隆过滤器**：每分段在 merge 时生成 `bloom-<id>.filter`（位图 + 双哈希，目标误判率 1%），
      重启加载、随分段删除；提供 `may_contain()` 负查询 API。
      **诚实说明**：keydir 全量驻留内存时 bloom 对 `get` 没有提速（索引本身就是精确过滤器），
      它的价值在 merge 期判断与未来 keydir 分区/落盘场景 —— 与其假装是性能优化，不如把能力做完整
- [ ] **v0.8 基准测试**：用 `criterion` 跑 set/get/range 压测，对比纯 BTreeMap 基线

## 相关阅读

- Bitcask 论文：*Bitcask — A Log-Structured Hash Table for Fast Key/Value Data* (Basho, 2010)
- [mini-lsm](https://github.com/skyzh/mini-lsm)：本项目灵感来源之一，进阶可去做 LSM-Tree
- [build-your-own-x](https://github.com/codecrafters-io/build-your-own-x)：更多"造轮子学习"路线

## License

MIT

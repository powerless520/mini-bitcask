# mini-bitcask

> 用纯 Rust（零依赖，仅 std）实现的 Bitcask 风格 KV 存储引擎，核心约 1420 行（含 38 个测试共 2427 行）。
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

## TTL 过期

```rust
// 60 秒后过期
db.set_with_ttl("token", "abc", Duration::from_secs(60))?;
db.get("token")?;  // Some("abc")

// 61 秒后
db.get("token")?;  // None —— 读路径按索引里的 expire_at 过滤，一次磁盘 IO 都不产生
```

语义（三个容易踩的边界）：

- `ttl == 0` 等价于**永不过期**，而不是"立即过期"——后者会让 `set_with_ttl(k, v, 0)` 变成一个语义诡异的删除 API
- `expire_at` 放在**内存索引里**，所以过期判断是纯内存操作；磁盘 entry 里也存一份，作为重启/hint 恢复时的权威来源
- 过期**不是墓碑**：过期的 key 可以正常被重新 `set`，不会阻塞后续写入

**惰性删除的取舍**：读视图（`get` / `keys` / `range` / `prefix` / `len` / `contains_key` / `may_contain`）
立刻看不到过期 key，但磁盘空间要等 `merge` 才真正回收。为什么不在 `get` 时顺手写墓碑？

1. 那会让读路径产生写操作 —— 读放大变写放大，读线程还得抢 active 写锁，直接破坏「多读单写」模型
2. 空间回收本来就该批量做：一次 merge 一次 `fsync`，比每条过期数据各写一次便宜得多

代价是过期数据在 merge 前持续占盘。`stats().expired_keys` 就是这个「待清算量」的可观测信号：

```
> setex token 60 abc
OK (key=token, ttl=60s)
> ttl token
剩余 60.0s（到期时间戳 1794813504013）
> ttl plain
永不过期
> ttl nope
(nil)
```

```
# 1 秒 TTL 的 key 过期后
> stats
live_keys=2 expired=1 total_bytes=88 (~0.1 KiB) segments=0 active=88 B blooms=0 [有已过期 key 待 merge 回收空间]
> merge
merge 完成: 88 B -> 62 B (压缩到 70.5%)
> stats
live_keys=2 expired=0 total_bytes=62 segments=1 active=0 B blooms=1
```

## 批量写

```rust
// 一批里可以混着写、删、带 TTL 的写
db.write_batch(vec![
    Op::put("user:1", "alice"),
    Op::put_with_ttl("session:abc", "token", Duration::from_secs(3600)),
    Op::delete("user:2"),
])?;

// 只写不删的便捷写法
db.set_many(items)?;   // items: IntoIterator<Item = (K, V)>
```

批量写把 N 条记录编码进一个连续缓冲区，因此：**一次 `write_all`、一次 active 锁、一次 index 锁**。

**但这个 API 不保证原子性**，必须说清楚：这批操作在磁盘上就是 N 条独立记录。
写到一半崩溃，结果是「前缀可见、后缀丢失」——即前 k 条生效、后面的没写进去。
索引只在 `write_all` 成功后更新，所以**索引与数据始终自洽**，不会出现"索引指向不存在的记录"。

想要「这批要么全生效、要么全不生效」需要两阶段提交（先写整批 + commit record，
读时只认已提交的部分）。那是另一个量级的复杂度，本版明确不做，并把边界写在 API 文档里。

批内同一个 key 出现多次时，按顺序生效（last-write-wins）。

```
> batch 5000 k
批量写入 5000 条，198890 字节，耗时 2.42ms（2063628 ops/s）
```



## 格式演进：v0.9 不向下兼容

给 entry 加 `expire_at` 改变了二进制布局（entry header 13 → 21 字节，hint header 20 → 28 字节）。
**v0.8 及更早版本写的数据目录，在 v0.9 下会从第一条记录起 CRC 校验失败，被当作损坏尾部整体截断。**

这不是 bug，而是真实存储引擎的日常。所以生产级实现都在每个文件头放 `magic + version`，
启动时识别格式代际，再决定迁移还是拒绝服务。本项目故意不做兼容层 —— 用一次真实的数据不兼容，
把「为什么需要文件格式版本号」这件事从抽象说教变成亲身经历。

## 快速开始

```bash
# 运行测试
cargo test

# 交互式体验
cargo run -- ./mydata
```

```
mini-bitcask v1.1 (dir: ./mydata)
命令: set <k> <v> | setex <k> <秒> <v> | get <k> | del <k> | ttl <k>
      batch <n> <前缀> | list | stats | merge
      range <a> <b> | prefix <p> | bloom <k> | exit
共 0 个存活 key 从日志中恢复
> set user alice
OK (key=user)
> set user bob
OK (key=user)
> set lang rust
OK (key=lang)
> stats
live_keys=2 expired=0 total_bytes=87 (~0.1 KiB) segments=0 active=87 B blooms=0
> merge
merge 完成: 87 B -> 57 B (压缩到 65.5%)
> get user
bob
> prefix l
lang = rust
> exit
bye
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
| `src/lib.rs` | 引擎核心 | append-only 文件设计、二进制协议编码、CRC 校验、多文件 merge、`O_APPEND` 偏移量陷阱、惰性删除、批量写与成本归因 |
| `src/main.rs` | REPL CLI | `std::io` 交互、错误处理 |
| `examples/bench.rs` | 零依赖基准 | 手写计时、`black_box` 防优化、和内存基线对打的方法论 |

刻意保留的教学性设计：

1. **零依赖**：序列化用裸的字节序编码（大端），让你看清“协议”的本质
2. **脏尾容忍**：`read_entry_at` 读到不完整条目时静默停止 —— 崩溃后最后一半写入会被丢弃（这正是 append-only 的崩溃安全性）
3. **防御性长度校验**：`MAX_KEY` / `MAX_VALUE` 上限，防止损坏日志触发疯狂内存分配
4. **惰性删除**：TTL 过期只在读视图过滤，物理回收交给 merge —— 用一次真实取舍讲清"读路径不该写"这条原则

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
- [x] **v0.8 基准测试**：`examples/bench.rs` 手写零依赖基准，量化 set/get/range 吞吐并对比纯 BTreeMap 基线；
      merge 压缩比与 hint 重启耗时同场测量。
      **诚实说明**：`criterion` 会引入约 50 个传递依赖，与「零依赖」的立身之本冲突，因此选择手写基准 ——
      代价是没有置信区间，这个代价在 README 里明确写出来而不是藏起来
- [x] **v0.9 TTL 过期**：entry 增加 `expire_at`（unix 毫秒，0 = 永不过期），
      索引里也存一份 → 过期判断纯内存、零 IO；提供 `set_with_ttl()` / `expire_at()`，
      merge 顺手物理清除过期数据。
      **取舍**：惰性删除而非"读到就写墓碑"，避免读路径产生写操作、破坏多读单写模型；
      代价是过期数据在 merge 前持续占盘，用 `stats().expired_keys` 把这份"待清算量"暴露出来。
      **真实教训**：加字段 = 格式不兼容，v0.8 的数据目录会被当损坏尾部整体截断 ——
      这就是为什么生产引擎的文件头必须有 magic + version
- [x] **v1.0 批量写 + 成本归因**：`write_batch(Vec<Op>)` / `set_many()` 把一批操作编码进
      一个连续缓冲区，一次 `write_all`、一次 active 锁、一次 index 锁；`encode_entry_into`
      直接写进目标 buffer（省掉每条一次临时 Vec）。
      **结果出乎预期**：只快 1.2–1.4x，不是想象中的数倍。于是把 CRC-32 单独计时 ——
      155 字节/条要 200–215 ns，占单条 `set` 的 **36–41%**，而它是逐字节纯 CPU 工作、批量摊薄不了。
      之前 README 里"4x 差距主要来自 write 系统调用"的判断是错的，已按实测数据修正。
      **教训**：先测量再优化；锁和系统调用在无竞争时根本不贵
      **边界**：批量写不保证原子性，崩溃时是"前缀可见"而非"全有或全无"（要原子性得加 commit record）
- [x] **v1.1 slice-by-8 CRC**：把 v1.0 归因出的最大成本项优化掉。8 张表一次折叠 8 轮逐字节迭代，
      155 B/条从 200–215 ns 降到 **41–46 ns**（721 → 3300–3800 MB/s，约 4.7x）。
      单条 `set` 从 520 → 322–334 ns，与内存基线的差距从 3.8x 缩到 **2.2–2.4x**；
      批量写跟着受益：427 → 233–248 ns。
      正确性不靠"我记得算法对"：保留朴素实现作参照（仅测试构建编译），
      差分测试覆盖 8 边界两侧的全部长度 + 已知向量（`123456789` → `0xCBF43926`）。
      **这一版闭环了完整的方法论**：测量（v0.8）→ 归因（v1.0）→ 优化 → 再测量（v1.1），
      预测收益 520-215+46≈350 ns，实测 322–334 ns —— 预算和决算对上了

## 后续可以做的

- **TTL 主动清理**：后台线程定期扫描（目前只能靠 merge 回收）
- **keydir 分区落盘**：索引不再全量驻留内存，让 v0.7 的布隆过滤器真正开始产生价值
- **硬件 CRC**：SSE4.2 的 `crc32` 指令能再快一个量级，但会失去"纯 std 零依赖"的属性，需要 feature gate —— 是否值得，取决于你对"零依赖"的忠诚度
- **发布到 crates.io**：目前只能 path 依赖

## 基准测试

```bash
cargo run --release --example bench
```

零依赖的手写基准（不是 `criterion`）——理由见下文「为什么不用 criterion」。
10 万 key × 128 B value（约 14.8 MB），每组都和纯 `BTreeMap<String, Vec<u8>>` 基线对打：

```
顺序写入 set                 326 ns/op     3063859 ops/s   | BTreeMap 基线     144 ns/op（2.3x）
随机读 get                 2024 ns/op      494129 ops/s   | BTreeMap 基线     292 ns/op（6.9x）
范围扫描 range              1098 ns/op      910598 ops/s   | BTreeMap 基线      20 ns/op（54.6x）
批量写 set_many             248 ns/op     4032631 ops/s   | 对比单条 set     326 ns/op（快 1.3x，每批 5000 条）
CRC-32（155B/条）            46 ns/op        3342 MB/s   | 占单条 set 的 14%

merge 压缩
  耗时 150.67ms，18.4 MB -> 3.3 MB（压缩到 18.1%）
  存活 key 100000，分段数 0 -> 1，布隆过滤器 1 个

重启加载（hint 快速路径）
  10.92ms（100000 keys，3.34 MB 分段）
```

怎么读这些数字（倍数给区间，因为单次运行本身就有抖动）：

| 指标 | 典型差距 | 差距来源 |
|------|---------|---------|
| `set` | 2.2–2.4x | v1.0 归因出的 CRC 大头（当时占 36–41%）在 v1.1 被 slice-by-8 压掉后，剩下的差距就是：String 分配 + value 拷贝 + 树插入（基线那部分）+ 两次锁 + 两次 memcpy + 索引插入。**已经没有单项占比 >20% 的"大头"了** —— 这通常意味着该换思路而不是继续抠 |
| `get` | 7–13x | 主体是一次 `pread`；读路径也要**校验 CRC**（v1.1 后约 45 ns，占比从 11% 降到 ~2%）——「每读一次都验证数据完整」是有价格的 |
| `range` | 倍数不稳 | 绝对值更可靠：约 1.1–1.2 μs/op。倍数波动大是因为内存基线只有 10–40 ns，已经接近计时噪声下限。根因是设计取舍：`range` 先在索引读锁内收集 key（很便宜），再对**每一行**调用 `get()` 取值（索引查找 + `pread`）。换来的是不长时间持锁阻塞写入，以及自动继承并发 merge 的 key 校验重试逻辑。想要快就得顺序读整段文件并跳过失效条目 —— 那是另一种权衡 |

诚实提示：

- **单次运行、无置信区间**。同一份代码连跑三次，随机读在 1.5–2.5 μs 之间波动
  （对应 7–13x），这是调度与页缓存状态的噪声，不是实现不稳定。想要统计显著性请多跑几轮取中位数。
- **全是页缓存热态**。冷启动（首次读盘）会慢一到两个数量级，这个基准测不出来。
- `merge` 的 18.1% 不是「压缩率」，是**存活数据占比**——本例每个 key 只写过 2 版左右，
  如果反复覆盖写同一批 key，这个比例会低到个位数。
- 批量写的对比对批量**略不利**：`Op` 持有 owned `Vec<u8>`，所以每条多一次 128 B 的 clone，
  而单条 `set` 传的是切片。即便如此它仍快 1.3–1.4x。

### 一次被数据推翻的假设（及它的闭环）

v1.0 的原始假设是：「`set` 比内存基线慢 4x，主要是每条一次 `write` 系统调用 + 锁开销，
做成批量写就能把差距抹掉大半。」

实测结果：批量写只快 **1.2–1.4x**。于是不再猜，把 CRC-32 单独计时 —— 155 字节要 200–215 ns，
单这一项就吃掉单条 `set` 的 36–41%。而 CRC 是逐字节查表，**批量写摊薄不了它**。

三条结论，都是被数字逼出来的：

1. 无竞争的 `Mutex` 不贵（约 15–20 ns/次），2 次锁省下来也就 30–40 ns
2. `BufWriter` 早就在帮你合并系统调用，所谓「每条一次 write」其实是「每条一次 memcpy」
3. 真正贵的是**逐字节的校验和计算** —— 这也解释了为什么生产引擎都在 CRC 上用
   `slice-by-8` 查表或硬件指令（SSE4.2 `crc32`）

v1.1 顺着这条线索把方法论走完了闭环：**测量 → 归因 → 优化 → 再测量**。

- 预算：slice-by-8 应省 ~170 ns，`set` 预期 520 − 170 ≈ 350 ns
- 决算：CRC 215 → 46 ns（4.7x），`set` 实测 322–334 ns，批量写 427 → 233–248 ns
- 预算和决算对上了，且没有引入新的"神秘大头"：现在单项占比最大的是基线本来就要做的
  String 分配与树插入，没有单项 >20% 的可优化项了

这比任何性能技巧都值钱：**先测量再优化，预算与决算互相校验**。
至于硬件 CRC 指令（SSE4.2），它能再快一个量级，但会打破纯 std 零依赖 ——
值不值这个代价，取决于你对"零依赖"的忠诚度，这是取舍题不是技术题。


### 为什么不用 criterion

`criterion` 是 Rust 基准测试的事实标准，但它会拉进约 50 个传递依赖，
而本项目的立身之本是「零依赖，`cargo build` 不需要网络」。为了一个学习项目的性能数字
破坏这个约束不划算。手写基准的代价是少了统计严谨性 —— 我选择把这个代价明确写出来，
而不是假装它不存在。

## 相关阅读

- Bitcask 论文：*Bitcask — A Log-Structured Hash Table for Fast Key/Value Data* (Basho, 2010)
- [mini-lsm](https://github.com/skyzh/mini-lsm)：本项目灵感来源之一，进阶可去做 LSM-Tree
- [build-your-own-x](https://github.com/codecrafters-io/build-your-own-x)：更多"造轮子学习"路线

## License

MIT

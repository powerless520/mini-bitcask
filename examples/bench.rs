//! 零依赖基准测试（刻意不引入 criterion）。
//!
//! 为什么不用 criterion：
//!   - 它会拉进 ~50 个传递依赖、编译数分钟，破坏本项目"零依赖、秒级编译"的基调
//!   - 代价是测量不如 criterion 严谨（单次运行、无置信区间、无自动剔除离群值）
//!   - 因此本基准的定位是 **量级对比**（本引擎 vs 纯 BTreeMap 基线），
//!     不是学术级性能报告。需要严谨统计请自行接 criterion 作为 dev-dependency。
//!
//! 运行：
//!   cargo run --release --example bench
//!
//! 说明：
//!   - 所有测量在 release 优化下进行
//!   - 值长度 128 字节，10 万个 key（约 13 MB 数据），属于小规模缓存友好型负载
//!   - 磁盘为 macOS APFS，数据落在系统临时目录（受页缓存影响，非冷启动数字）

use mini_bitcask::Bitcask;
use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::hint::black_box;
use std::time::{Duration, Instant};

const N_KEYS: u32 = 100_000;
const VALUE_LEN: usize = 128;
/// 单条 entry 的额外开销：21 字节 header + 6 字节 key（"key000000"）
const ENTRY_OVERHEAD: usize = 21 + 6;
/// 范围扫描一次覆盖的 key 数
const SCAN_KEYS: u32 = 10_000;

fn main() {
    let dir = env::temp_dir().join(format!("mini-bitcask-bench-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);

    println!("mini-bitcask 基准测试");
    println!(
        "  数据规模: {N_KEYS} keys x {VALUE_LEN}B values (~{:.1} MB)",
        (N_KEYS as f64 * (VALUE_LEN + ENTRY_OVERHEAD) as f64) / 1024.0 / 1024.0
    );
    println!("  注意: 单次运行、无置信区间，仅用于量级对比\n");

    let db = Bitcask::open(&dir).expect("open");
    let value = vec![0xABu8; VALUE_LEN];

    // ---------- 1) 顺序写入 ----------
    let t = Instant::now();
    for i in 0..N_KEYS {
        db.set(format!("key{i:06}"), &value[..]).expect("set");
    }
    let t_write = t.elapsed();

    let mut map: BTreeMap<String, Vec<u8>> = BTreeMap::new();
    let t = Instant::now();
    for i in 0..N_KEYS {
        black_box(map.insert(format!("key{i:06}"), value.clone()));
    }
    let t_write_base = t.elapsed();

    // ---------- 2) 随机读（命中） ----------
    // 用固定步长的伪随机序，避免排序带来的预取优势
    let order: Vec<u32> = (0..N_KEYS).map(|i| (i * 7919) % N_KEYS).collect();

    let t = Instant::now();
    for &i in &order {
        black_box(db.get(format!("key{i:06}")).expect("get"));
    }
    let t_read = t.elapsed();

    let t = Instant::now();
    for &i in &order {
        black_box(map.get(&format!("key{i:06}")));
    }
    let t_read_base = t.elapsed();

    // ---------- 3) 范围扫描 ----------
    let t = Instant::now();
    let rows = db
        .range("key010000", &format!("key{:06}", 10_000 + SCAN_KEYS))
        .expect("range");
    let t_range = t.elapsed();
    black_box(&rows);
    assert_eq!(rows.len(), SCAN_KEYS as usize);

    let t = Instant::now();
    let rows_base: Vec<_> = map
        .range("key010000".to_string()..format!("key{:06}", 10_000 + SCAN_KEYS))
        .collect();
    let t_range_base = t.elapsed();
    black_box(&rows_base);

    // ---------- 4) merge 压缩 ----------
    // 再覆盖写一遍，制造 50% 垃圾
    for i in 0..N_KEYS {
        db.set(format!("key{i:06}"), b"small".to_vec())
            .expect("set");
    }
    let stats_before = db.stats();
    let t = Instant::now();
    let (bytes_before, bytes_after) = db.merge().expect("merge");
    let t_merge = t.elapsed();
    let stats_after = db.stats();

    // ---------- 5) 重启加载 ----------
    db.sync().expect("sync");
    drop(db);
    let t = Instant::now();
    let db2 = Bitcask::open(&dir).expect("reopen");
    let t_reopen = t.elapsed();
    assert_eq!(db2.len(), N_KEYS as usize);
    drop(db2);

    // ---------- 报告 ----------
    print_row("顺序写入 set", N_KEYS as u64, t_write, Some(t_write_base));
    print_row("随机读 get", N_KEYS as u64, t_read, Some(t_read_base));
    print_row(
        "范围扫描 range",
        SCAN_KEYS as u64,
        t_range,
        Some(t_range_base),
    );

    println!("\nmerge 压缩");
    println!(
        "  耗时 {:.2?}，{:.1} MB -> {:.1} MB（压缩到 {:.1}%）",
        t_merge,
        bytes_before as f64 / 1024.0 / 1024.0,
        bytes_after as f64 / 1024.0 / 1024.0,
        bytes_after as f64 / bytes_before as f64 * 100.0
    );
    println!(
        "  存活 key {}，分段数 {} -> {}，布隆过滤器 {} 个",
        stats_after.live_keys,
        stats_before.segment_count,
        stats_after.segment_count,
        stats_after.bloom_count
    );

    println!("\n重启加载（hint 快速路径）");
    println!(
        "  {:.2?}（{} keys，{:.2} MB 分段）",
        t_reopen,
        N_KEYS,
        bytes_after as f64 / 1024.0 / 1024.0
    );

    let _ = fs::remove_dir_all(&dir);
    println!("\n注：以上为内存页缓存热态数字；冷启动（首次读盘）会更慢。");
}

fn print_row(label: &str, ops: u64, elapsed: Duration, baseline: Option<Duration>) {
    let per_op_ns = elapsed.as_nanos() as f64 / ops as f64;
    let ops_per_sec = ops as f64 / elapsed.as_secs_f64();
    match baseline {
        Some(base) => {
            let base_ns = base.as_nanos() as f64 / ops as f64;
            let ratio = per_op_ns / base_ns;
            println!(
                "{label:<18} {:>9.0} ns/op  {:>10.0} ops/s   | BTreeMap 基线 {:>7.0} ns/op（{:.1}x）",
                per_op_ns, ops_per_sec, base_ns, ratio
            );
        }
        None => println!(
            "{label:<18} {:>9.0} ns/op  {:>10.0} ops/s",
            per_op_ns, ops_per_sec
        ),
    }
}

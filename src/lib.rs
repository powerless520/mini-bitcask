//! mini-bitcask —— 一个 ~1010 行的 Bitcask 风格 KV 存储引擎（教学/开源项目）
//!
//! 设计模型（来自 Basho/riak 的 Bitcask 论文）：
//!
//!   写路径：所有写操作（set/delete）以 append-only 方式追加到 active.log
//!   索引：  内存中维护有序索引 BTreeMap<key, (file_id, offset)>，指向最新值的磁盘位置
//!   读路径：查内存索引 -> 按 (file, offset) 直接 seek 读取 value
//!   启动：  重放全部分段 + active.log，重建内存索引
//!   合并：  merge 把存活数据重写进新分段文件，随后删除旧文件（见 merge()）
//!
//! 磁盘条目（entry）二进制布局（大端序）：
//!
//!   ┌────────┬──────────┬────────┬──────────────┬─────────────┬─────────────┐
//!   │  crc   │ key_len  │  flag  │  value_len   │  key bytes  │ value bytes │
//!   │  u32   │  u32     │  u8    │  u32         │  key_len B  │ value_len B │
//!   └────────┴──────────┴────────┴──────────────┴─────────────┴─────────────┘
//!
//!   crc  = CRC-32（IEEE，多项式 0xEDB88320），覆盖 key_len 起至 value 结尾的全部字节
//!   flag = 0 表示 PUT，1 表示 DELETE（墓碑，value 为空）
//!
//! v0.3 文件布局（多文件 / merge，对应 Bitcask 论文 §3）：
//!
//!   <dir>/active.log            唯一可写文件，append-only 追加新写入
//!   <dir>/segment-000001.log    只读分段（merge 产物，写完后永不修改）
//!   <dir>/hint-000001.log       分段的索引备份（key + 位置，不含 value），加速重启
//!   <dir>/merge-<id>.tmp        merge 过程中的临时文件，rename 后成为正式分段
//!
//! v0.4 hint file：merge 时顺便写「不含 value 的索引备份」，重启时直接建索引。
//! 信任策略：hint 必须逐字节精确解析到文件尾、所有位置不越界，否则整体作废回退全量重放。
//!
//! v0.5 并发模型（对应 Bitcask 论文 §4，多读单写）：
//!
//!   `Bitcask` 是 Clone + Send + Sync 的共享句柄（内部 Arc），可交给任意线程。
//!   - 写（set/delete/merge）串行：都经过 active 的互斥锁，天然单写者
//!   - 读（get）并发：索引用 RwLock 读锁，分段文件句柄各自独立加锁，
//!     不同分段的读互不阻塞
//!   - 锁顺序（防死锁，全局统一）：active -> index -> segments
//!     get 先读索引并立即释放，再取文件句柄，不形成环
//!   - merge 竞态：读索引和读数据之间旧分段可能被删除，get 内置
//!     "重读索引 + 重试"，merge 期间读操作依然正确
//!
//! 崩溃安全性：
//!   1. 重放时逐条校验 CRC，损坏尾部直接截断丢弃（v0.2）
//!   2. merge 用 "写临时文件 -> fsync -> 原子 rename" 三步，任何时刻崩溃都不会丢数据；
//!      rename 顺序先数据后 hint，中间崩溃只损失加速路径
//!
//! v0.6 索引优化：keydir 从 HashMap 换成 BTreeMap（`KeyDir` 别名）。
//!
//!   - 单点读写从 O(1) 变成 O(log n)（可接受的代价）
//!   - 换来：key 天然有序（keys/range/prefix 无需排序）、范围查询 [a, b)、前缀扫描
//!   - range()/scan_prefix() 先在索引读锁内收集 key 列表（短持锁），
//!     再逐个 get 取 value —— 不阻塞写入，并自动继承 get 的并发 merge 重试逻辑
//!
//! v0.7 布隆过滤器：每个分段在 merge 时生成 bloom-<id>.filter（位图 + 双哈希，目标误判率 1%），
//!   重启时加载、随分段删除。
//!
//!   诚实说明：keydir 全量驻留内存时，布隆过滤器对 get **没有** 提速作用（索引本身是精确过滤器）。
//!   它的价值在于 merge 期判断 key 是否可能在某分段、以及为未来 keydir 分区/落盘预留负查询能力。
//!   因此本版把能力做完整（生成/持久化/恢复/负查询 API），而不是假装它是性能优化。
//!
//! 这是刻意留白的项目：基准测试留作后续演进路线，见 README.md 的 Roadmap。

use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock};

const FLAG_PUT: u8 = 0;
const FLAG_DEL: u8 = 1;

/// 当前可写文件名
const ACTIVE_FILE: &str = "active.log";
/// 只读分段文件名前缀
const SEGMENT_PREFIX: &str = "segment-";
/// 分段对应的 hint 文件名前缀
const HINT_PREFIX: &str = "hint-";
/// 分段对应的布隆过滤器文件名前缀
const BLOOM_PREFIX: &str = "bloom-";
/// merge 临时文件名前缀（数据与 hint 共用，靠扩展名 .tmp 识别）
const MERGE_TMP_PREFIX: &str = "merge-";
/// active.log 在索引中使用的 file_id
const ACTIVE_FILE_ID: u64 = 0;

/// 一条 entry 的解析结果：(key, flag, value, entry 总字节数)
type Entry = (String, u8, Vec<u8>, u64);

/// 内存索引（keydir）：key 有序存储，支持范围查询与前缀扫描。
type KeyDir = BTreeMap<String, ValuePos>;

/// 一次写入在磁盘上的位置。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ValuePos {
    /// 0 = active.log，其他 = segment-<id>.log
    file_id: u64,
    /// entry 起始偏移量
    offset: u64,
}

// ---------------- 编码与 CRC ----------------

/// CRC-32（IEEE 802.3，多项式 0xEDB88320），查表法。
/// 故意手写而非引入 crc crate —— 本项目坚持零依赖，顺便看清校验和的本质。
fn crc32(data: &[u8]) -> u32 {
    static TABLE: OnceLock<[u32; 256]> = OnceLock::new();
    let table = TABLE.get_or_init(|| {
        let mut t = [0u32; 256];
        for (i, slot) in t.iter_mut().enumerate() {
            let mut c = i as u32;
            for _ in 0..8 {
                c = if c & 1 != 0 {
                    0xEDB8_8320 ^ (c >> 1)
                } else {
                    c >> 1
                };
            }
            *slot = c;
        }
        t
    });
    let mut crc = 0xFFFF_FFFFu32;
    for &b in data {
        crc = table[((crc ^ b as u32) & 0xFF) as usize] ^ (crc >> 8);
    }
    !crc
}

fn invalid_data(msg: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.into())
}

/// 把一条记录编码成完整的磁盘字节流（crc + payload）。
fn encode_entry(key: &str, flag: u8, value: &[u8]) -> Vec<u8> {
    let mut payload = Vec::with_capacity(9 + key.len() + value.len());
    payload.extend_from_slice(&(key.len() as u32).to_be_bytes());
    payload.push(flag);
    payload.extend_from_slice(&(value.len() as u32).to_be_bytes());
    payload.extend_from_slice(key.as_bytes());
    payload.extend_from_slice(value);

    let mut out = Vec::with_capacity(4 + payload.len());
    out.extend_from_slice(&crc32(&payload).to_be_bytes());
    out.extend_from_slice(&payload);
    out
}

/// 把一条 hint 记录编码成磁盘字节流（不含 value，这是 hint 省空间的关键）：
///
///   ┌──────────┬────────────┬─────────────┬──────────┬────────────┐
///   │  key_len │  value_len │  entry_len  │  offset  │  key bytes │
///   │   u32    │    u32     │    u32      │   u64    │  key_len B │
///   └──────────┴────────────┴─────────────┴──────────┴────────────┘
fn encode_hint_entry(key: &str, value_len: usize, entry_len: u64, offset: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(20 + key.len());
    out.extend_from_slice(&(key.len() as u32).to_be_bytes());
    out.extend_from_slice(&(value_len as u32).to_be_bytes());
    out.extend_from_slice(&(entry_len as u32).to_be_bytes());
    out.extend_from_slice(&offset.to_be_bytes());
    out.extend_from_slice(key.as_bytes());
    out
}

fn segment_file_name(id: u64) -> String {
    format!("{SEGMENT_PREFIX}{id:06}.log")
}

fn hint_file_name(id: u64) -> String {
    format!("{HINT_PREFIX}{id:06}.log")
}

fn bloom_file_name(id: u64) -> String {
    format!("{BLOOM_PREFIX}{id:06}.filter")
}

fn parse_segment_id(path: &Path) -> Option<u64> {
    let name = path.file_name()?.to_str()?;
    let digits = name.strip_prefix(SEGMENT_PREFIX)?.strip_suffix(".log")?;
    digits.parse::<u64>().ok()
}

/// 尽力读满 buf。
///   Ok(true)  -> 读满
///   Ok(false) -> 在起始位置遇到干净 EOF（读完了）
/// 读到一半 EOF 视为数据损坏，返回 Err（由调用方决定回退策略）。
fn fill_exact(reader: &mut impl Read, buf: &mut [u8]) -> io::Result<bool> {
    if buf.is_empty() {
        return Ok(true);
    }
    let mut got = reader.read(&mut buf[..1])?;
    if got == 0 {
        return Ok(false); // 干净 EOF
    }
    while got < buf.len() {
        let n = reader.read(&mut buf[got..])?;
        if n == 0 {
            return Err(invalid_data("partial record at end of file"));
        }
        got += n;
    }
    Ok(true)
}

/// 尝试从 hint 文件重建指定分段的索引。
///   Ok(true)  -> 成功，index 已更新
///   Ok(false) -> hint 无效（结构不完整/越界），应回退全量重放
/// 结构校验（宁可不加速也不能读错）：
///   1. 必须逐字节精确解析到文件尾，任何残留字节即整个 hint 作废
///   2. 每条 offset + entry_len 不得超过分段文件大小
fn load_index_from_hint(
    hint_path: &Path,
    file_id: u64,
    seg_len: u64,
    index: &mut KeyDir,
) -> io::Result<bool> {
    let file = match File::open(hint_path) {
        Ok(f) => f,
        Err(_) => return Ok(false),
    };
    let mut reader = BufReader::new(file);

    // hint 里读到一半 EOF = 结构残缺，整个 hint 作废（回退全量重放），
    // 而不是让 open 失败 —— 容错是 hint 信任策略的一部分
    macro_rules! try_hint_read {
        ($buf:expr) => {
            match fill_exact(&mut reader, $buf) {
                Ok(v) => v,
                Err(_) => return Ok(false),
            }
        };
    }

    loop {
        // header：key_len + value_len + entry_len + offset = 20 字节
        let mut header = [0u8; 20];
        if !try_hint_read!(&mut header) {
            return Ok(true); // 干净 EOF：整个 hint 有效
        }
        let key_len =
            u32::from_be_bytes([header[0], header[1], header[2], header[3]]) as usize;
        let _value_len =
            u32::from_be_bytes([header[4], header[5], header[6], header[7]]) as usize;
        let entry_len = u32::from_be_bytes([header[8], header[9], header[10], header[11]]);
        let offset = u64::from_be_bytes([
            header[12], header[13], header[14], header[15], header[16], header[17],
            header[18], header[19],
        ]);

        const MAX_KEY: usize = 1 << 20;
        if key_len > MAX_KEY {
            return Ok(false);
        }
        if offset.saturating_add(entry_len as u64) > seg_len {
            return Ok(false); // 指向分段外的位置：hint 与数据不匹配
        }

        let mut key = vec![0u8; key_len];
        if !try_hint_read!(&mut key) {
            return Ok(false); // 尾部残缺
        }
        let key = match String::from_utf8(key) {
            Ok(k) => k,
            Err(_) => return Ok(false),
        };

        index.insert(key, ValuePos { file_id, offset });
    }
}

// ---------------- 布隆过滤器（v0.7） ----------------

/// 布隆过滤器文件魔数
const BLOOM_MAGIC: &[u8; 4] = b"MBF1";

/// 极简布隆过滤器（零依赖）：位图 + 双哈希（Kirsch-Mitzenmacher 技巧）。
///
/// 定位说明（如实记录，避免自欺）：
/// 本引擎的 keydir 全量驻留内存，所以布隆过滤器对 get **没有** 提速作用——
/// 索引本身就是精确过滤器。它的价值在：
///   1. merge 时快速判断"某 key 是否可能在某分段中"，避免无谓的磁盘读
///   2. 为未来 keydir 分区/落盘（索引不再全量驻留）预留负查询加速能力
///
/// 因此 v0.7 先把能力实现并持久化（每分段一个 bloom 文件），随分段生命周期管理。
pub struct Bloom {
    /// 位图（以 u64 为单位）
    bits: Vec<u64>,
    /// 哈希函数个数
    k: u8,
    /// 位图总位数
    m: u64,
}

impl Bloom {
    /// 按预期元素数 n 与目标误判率 fp_rate 计算最优 m 与 k：
    ///   m = -n·ln(p) / (ln2)²，k = (m/n)·ln2
    pub fn with_capacity(n: usize, fp_rate: f64) -> Self {
        let n = n.max(1) as f64;
        let ln2 = std::f64::consts::LN_2;
        let m = ((-(n * fp_rate.ln())) / (ln2 * ln2)).ceil() as u64;
        let m = m.max(64);
        let k = (((m as f64 / n) * ln2).round() as u32).clamp(1, 16) as u8;
        Bloom {
            bits: vec![0u64; m.div_ceil(64) as usize],
            k,
            m,
        }
    }

    /// 双哈希：FNV-1a 作为 h1，另一个混合函数作为 h2（强制奇数，保证步长非零）。
    fn hashes(key: &[u8]) -> (u64, u64) {
        let mut h1: u64 = 0xcbf2_9ce4_8422_2325;
        for &b in key {
            h1 ^= b as u64;
            h1 = h1.wrapping_mul(0x0000_0100_0000_01b3);
        }
        let mut h2: u64 = 0x9E37_79B9_7F4A_7C15;
        for &b in key {
            h2 = (h2 ^ b as u64)
                .wrapping_mul(0x9E37_79B9_7F4A_7C15)
                .rotate_left(5);
        }
        (h1, h2 | 1)
    }

    pub fn insert(&mut self, key: &[u8]) {
        let (h1, h2) = Self::hashes(key);
        for i in 0..self.k as u64 {
            let idx = h1.wrapping_add(i.wrapping_mul(h2)) % self.m;
            self.bits[(idx / 64) as usize] |= 1u64 << (idx % 64);
        }
    }

    /// 返回 false 表示"一定不存在"（无假阴性）；返回 true 表示"可能存在"。
    pub fn may_contain(&self, key: &[u8]) -> bool {
        let (h1, h2) = Self::hashes(key);
        for i in 0..self.k as u64 {
            let idx = h1.wrapping_add(i.wrapping_mul(h2)) % self.m;
            if self.bits[(idx / 64) as usize] & (1u64 << (idx % 64)) == 0 {
                return false;
            }
        }
        true
    }

    /// 序列化：[magic 4B][k 1B][m 8B][bitmap]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(13 + self.bits.len() * 8);
        out.extend_from_slice(BLOOM_MAGIC);
        out.push(self.k);
        out.extend_from_slice(&self.m.to_be_bytes());
        for word in &self.bits {
            out.extend_from_slice(&word.to_be_bytes());
        }
        out
    }

    /// 反序列化：任何结构异常都返回 None（宁可退化也不误判）。
    pub fn from_bytes(buf: &[u8]) -> Option<Self> {
        if buf.len() < 13 || &buf[..4] != BLOOM_MAGIC {
            return None;
        }
        let k = buf[4];
        let m = u64::from_be_bytes([
            buf[5], buf[6], buf[7], buf[8], buf[9], buf[10], buf[11], buf[12],
        ]);
        if m == 0 || m > (1 << 40) || !(1..=16).contains(&k) {
            return None;
        }
        let words = m.div_ceil(64) as usize;
        let body = &buf[13..];
        if body.len() != words * 8 {
            return None;
        }
        let mut bits = Vec::with_capacity(words);
        for chunk in body.as_chunks::<8>().0 {
            bits.push(u64::from_be_bytes(*chunk));
        }
        Some(Bloom { bits, k, m })
    }

    /// 理论误判率（用于测试与文档对照）
    pub fn fp_rate_estimate(&self, n: usize) -> f64 {
        let n = n.max(1) as f64;
        (1.0 - (-(self.k as f64 * n) / self.m as f64).exp()).powi(self.k as i32)
    }
}

// ---------------- 共享状态（v0.5 并发模型） ----------------

/// active.log 的读写句柄 + 记账。写入串行化的核心：所有写都锁这里。
struct ActiveFile {
    /// 追加写句柄（O_APPEND）
    writer: BufWriter<File>,
    /// 随机读句柄（与 writer 同锁，flush 后可安全读）
    reader: File,
    /// active.log 当前字节数（O_APPEND 下 stream_position 不可靠，自己记账）
    bytes: u64,
}

/// 一个只读分段：数据文件读句柄 + 可选布隆过滤器。
struct Segment {
    file: Mutex<File>,
    /// merge 时生成；重启时从 bloom-<id>.filter 恢复（文件缺失/损坏则为 None）
    bloom: Option<Bloom>,
}

/// 被 Arc 共享的内部状态。
///
/// 锁顺序（所有代码路径必须一致，防死锁）：active -> index -> segments。
struct Shared {
    dir: PathBuf,
    /// 内存索引：key -> 最新值的位置
    index: RwLock<KeyDir>,
    /// 只读分段（id -> 句柄 + 布隆过滤器）。读用读锁，merge 换血用写锁
    segments: RwLock<HashMap<u64, Arc<Segment>>>,
    /// active.log 读写句柄（所有写入串行化的单点）
    active: Mutex<ActiveFile>,
    /// 下一个可用的分段 id
    next_segment_id: AtomicU64,
    /// 打开时发现的损坏尾部字节数（已截断丢弃，仅作报告用）
    corrupt_tail_bytes: AtomicU64,
}

impl Drop for Shared {
    fn drop(&mut self) {
        // 最后一个句柄释放时尽力 flush（此时锁必然无人竞争）
        if let Ok(mut active) = self.active.lock() {
            let _ = active.writer.flush();
        }
    }
}

/// Bitcask 实例句柄：Clone + Send + Sync，多读单写（Bitcask 论文 §4）。
///
/// 克隆它即可把同一个数据库共享给其他线程：
///
/// ```no_run
/// let db = mini_bitcask::Bitcask::open("./mydata")?;
/// let db2 = db.clone();
/// std::thread::spawn(move || { db2.set("k", "v").unwrap(); });
/// db.get("k")?;
/// # Ok::<(), std::io::Error>(())
/// ```
#[derive(Clone)]
pub struct Bitcask {
    shared: Arc<Shared>,
}

impl Bitcask {
    /// 打开（或创建）一个 Bitcask 数据目录，并重放日志重建内存索引。
    pub fn open(dir: impl AsRef<Path>) -> io::Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;

        // 清理上次 merge 中途崩溃留下的临时文件（rename 之前崩溃会遗留）
        for entry in fs::read_dir(&dir)? {
            let path = entry?.path();
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                if name.ends_with(".tmp")
                    && (name.starts_with(MERGE_TMP_PREFIX)
                        || name.starts_with(HINT_PREFIX)
                        || name.starts_with(BLOOM_PREFIX))
                {
                    let _ = fs::remove_file(&path);
                }
            }
        }

        let active_path = dir.join(ACTIVE_FILE);
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&active_path)?;
        let active_reader = file.try_clone()?;

        // 收集所有只读分段，按 id 升序（= 写入时间顺序）
        let mut segment_ids: Vec<u64> = fs::read_dir(&dir)?
            .filter_map(|e| e.ok())
            .filter_map(|e| parse_segment_id(&e.path()))
            .collect();
        segment_ids.sort_unstable();

        let mut index = KeyDir::new();
        let mut segments: HashMap<u64, Arc<Segment>> = HashMap::new();
        for &id in &segment_ids {
            let seg_path = dir.join(segment_file_name(id));
            let seg_file = OpenOptions::new().read(true).open(&seg_path)?;
            let seg_len = seg_file.metadata()?.len();

            // v0.4 快速路径：hint 结构完整 -> 直接建索引，跳过数据扫描
            // 回退路径：hint 缺失或无效 -> 全量重放数据文件（v0.3 行为）
            let mut loaded_from_hint = false;
            let hint_path = dir.join(hint_file_name(id));
            if hint_path.exists() {
                loaded_from_hint = load_index_from_hint(&hint_path, id, seg_len, &mut index)?;
            }
            if !loaded_from_hint {
                let mut data = seg_file.try_clone()?;
                Self::replay_file(&mut data, id, &mut index)?;
            }

            // v0.7：恢复该分段的布隆过滤器（缺失或损坏 -> None，只损失负查询加速）
            let bloom = fs::read(dir.join(bloom_file_name(id)))
                .ok()
                .and_then(|bytes| Bloom::from_bytes(&bytes));

            segments.insert(
                id,
                Arc::new(Segment {
                    file: Mutex::new(seg_file),
                    bloom,
                }),
            );
        }

        let corrupt_tail_bytes = AtomicU64::new(0);
        let shared = Shared {
            dir,
            index: RwLock::new(index),
            segments: RwLock::new(segments),
            next_segment_id: AtomicU64::new(segment_ids.last().copied().unwrap_or(0) + 1),
            active: Mutex::new(ActiveFile {
                writer: BufWriter::new(file),
                reader: active_reader,
                bytes: 0,
            }),
            corrupt_tail_bytes,
        };

        let db = Bitcask {
            shared: Arc::new(shared),
        };

        // active 重放在最后（active 永远是最新写入），
        // 这样"后写覆盖先写"的语义在重启后依然成立
        let (valid, corrupt) = {
            let mut active = db.shared.active.lock().unwrap();
            let r = Self::replay_file(&mut active.reader, ACTIVE_FILE_ID, &mut db.shared.index.write().unwrap())?;
            r
        };
        db.shared
            .corrupt_tail_bytes
            .store(corrupt, Ordering::Relaxed);
        if corrupt > 0 {
            // 截断损坏尾部：O_APPEND 会把新记录写到损坏数据之后，
            // 不截断的话下次重放仍会在损坏处停止，新数据永远不可见
            let active = db.shared.active.lock().unwrap();
            active.reader.set_len(valid)?;
        }
        db.shared.active.lock().unwrap().bytes = valid;

        Ok(db)
    }

    /// 重放单个文件的全部 entry，把索引写进 `index`。
    /// 返回 (有效字节数, 损坏尾部字节数)。
    fn replay_file(
        file: &mut File,
        file_id: u64,
        index: &mut KeyDir,
    ) -> io::Result<(u64, u64)> {
        let mut offset: u64 = 0;
        loop {
            match Self::read_entry_at(file, offset) {
                Ok(Some((key, flag, _value, entry_len))) => {
                    match flag {
                        FLAG_PUT => {
                            index.insert(key, ValuePos { file_id, offset });
                        }
                        FLAG_DEL => {
                            index.remove(&key);
                        }
                        _ => break, // 未知 flag：损坏
                    }
                    offset += entry_len;
                }
                Ok(None) => break, // EOF / 脏尾
                Err(e) if e.kind() == io::ErrorKind::InvalidData => break, // CRC 失败等
                Err(e) => return Err(e),
            }
        }
        let file_len = file.metadata()?.len();
        Ok((offset, file_len.saturating_sub(offset)))
    }

    /// 从指定偏移量读取一条 entry（含 CRC 校验）。
    /// 返回 Ok(None) 表示该位置已到 EOF 或剩余字节不足以构成完整 entry。
    fn read_entry_at(reader: &mut File, offset: u64) -> io::Result<Option<Entry>> {
        reader.seek(SeekFrom::Start(offset))?;

        // header：crc(u32) + key_len(u32) + flag(u8) + value_len(u32) = 13 字节
        let mut header = [0u8; 13];
        match reader.read_exact(&mut header) {
            Ok(()) => {}
            Err(_) => return Ok(None), // EOF / 脏尾
        }
        let expected_crc = u32::from_be_bytes([header[0], header[1], header[2], header[3]]);
        let key_len = u32::from_be_bytes([header[4], header[5], header[6], header[7]]) as usize;
        let flag = header[8];
        let value_len =
            u32::from_be_bytes([header[9], header[10], header[11], header[12]]) as usize;

        // 防御性校验：防止损坏日志导致疯狂分配内存
        const MAX_KEY: usize = 1 << 20; // 1 MiB
        const MAX_VALUE: usize = 64 << 20; // 64 MiB
        if key_len > MAX_KEY || value_len > MAX_VALUE {
            return Err(invalid_data("corrupt entry: key/value length exceeds limit"));
        }

        // payload = key_len+flag+value_len 这 9 字节 + key + value（crc 覆盖它全部）
        let mut payload = vec![0u8; 9 + key_len + value_len];
        payload[..9].copy_from_slice(&header[4..13]);
        reader.read_exact(&mut payload[9..])?;

        if crc32(&payload) != expected_crc {
            return Err(invalid_data("corrupt entry: CRC mismatch"));
        }

        let key = payload[9..9 + key_len].to_vec();
        let value = payload[9 + key_len..].to_vec();
        let key =
            String::from_utf8(key).map_err(|_| invalid_data("corrupt entry: key is not UTF-8"))?;
        Ok(Some((key, flag, value, (13 + key_len + value_len) as u64)))
    }

    /// 写入一个键值对（覆盖即追加新版本，旧值成为垃圾数据）。
    ///
    /// 并发语义：active 锁内完成"追加 + 更新索引"，保证多线程下
    /// 同一个 key 的索引永远指向最后完成的那次写入（last-write-wins）。
    pub fn set(&self, key: impl Into<String>, value: impl Into<Vec<u8>>) -> io::Result<()> {
        let key = key.into();
        let value = value.into();

        let entry = encode_entry(&key, FLAG_PUT, &value);
        // 锁顺序：active -> index
        let mut active = self.shared.active.lock().unwrap();
        let offset = active.bytes;
        active.writer.write_all(&entry)?;
        active.bytes += entry.len() as u64;
        self.shared
            .index
            .write()
            .unwrap()
            .insert(key, ValuePos { file_id: ACTIVE_FILE_ID, offset });
        Ok(())
    }

    /// 删除一个 key：追加墓碑记录 + 移除内存索引。
    /// 删除不存在的 key 也是合法操作（与其他 Bitcask 实现语义一致）。
    pub fn delete(&self, key: impl AsRef<str>) -> io::Result<()> {
        let key = key.as_ref();

        let entry = encode_entry(key, FLAG_DEL, &[]);
        let mut active = self.shared.active.lock().unwrap();
        active.writer.write_all(&entry)?;
        active.bytes += entry.len() as u64;
        self.shared.index.write().unwrap().remove(key);
        Ok(())
    }

    /// 读取一个 key 的最新值。线程安全，可与写、merge 并发。
    ///
    /// 并发正确性核心：索引位置可能在"读索引"和"读数据"之间被并发 merge 作废
    /// （旧分段被删 / active 被截断后复用）。因此每条读出的记录都要校验
    /// key 是否匹配，不匹配说明位置过期 -> 重读索引重试。
    pub fn get(&self, key: impl AsRef<str>) -> io::Result<Option<Vec<u8>>> {
        let key = key.as_ref();

        // 先读索引并立即释放（避免与写路径的 index 写锁形成死锁环）
        let Some(mut pos) = ({
            let index = self.shared.index.read().unwrap();
            index.get(key).copied()
        }) else {
            return Ok(None);
        };

        // 最多重试 3 次（正常情况下 1 次就命中）
        for _attempt in 0..3u32 {
            let result = if pos.file_id == ACTIVE_FILE_ID {
                let mut active = self.shared.active.lock().unwrap();
                // 写缓冲必须先落盘，否则读到的是旧内容（v0.1 的经典 bug）
                active.writer.flush()?;
                Self::read_entry_at(&mut active.reader, pos.offset)
            } else {
                let handle = {
                    let segments = self.shared.segments.read().unwrap();
                    segments.get(&pos.file_id).cloned()
                };
                match handle {
                    // 分段已被并发 merge 删除 -> 直接走重读索引逻辑
                    None => Ok(None),
                    Some(segment) => {
                        let mut reader = segment.file.lock().unwrap();
                        Self::read_entry_at(&mut reader, pos.offset)
                    }
                }
            };

            match result {
                Ok(Some((k, FLAG_PUT, value, _))) if k == key => return Ok(Some(value)),
                Ok(Some((k, FLAG_DEL, _, _))) if k == key => return Ok(None),
                // 读到 EOF / 另一个 key 的记录（位置过期）-> 重读索引
                Ok(Some(_)) | Ok(None) => {
                    let index = self.shared.index.read().unwrap();
                    match index.get(key).copied() {
                        None => return Ok(None), // merge 顺带清走了这个 key
                        Some(p) if p == pos => {
                            // 索引位置没变但数据对不上：真实不一致，如实报错
                            return Err(invalid_data(format!(
                                "index/key mismatch at file {} offset {}",
                                pos.file_id, pos.offset
                            )));
                        }
                        Some(p) => {
                            pos = p;
                        }
                    }
                }
                // 读错误（CRC 失败等）：可能是过期位置落在别的 entry 中间。
                // 重读索引：位置变了说明确实过期，重试；没变则是真实损坏
                Err(e) => {
                    let index = self.shared.index.read().unwrap();
                    match index.get(key).copied() {
                        Some(p) if p != pos => {
                            pos = p;
                        }
                        _ => return Err(e),
                    }
                }
            }
        }
        Err(invalid_data("get: too many retries (concurrent merge storm?)"))
    }

    /// 便捷方法：读取并转成 UTF-8 字符串（demo/CLI 用）。
    pub fn get_string(&self, key: impl AsRef<str>) -> io::Result<Option<String>> {
        match self.get(key)? {
            Some(v) => Ok(Some(String::from_utf8_lossy(&v).into_owned())),
            None => Ok(None),
        }
    }

    /// 合并压缩（Bitcask 论文 §3）：把全部存活数据重写进一个新分段文件，
    /// 然后删除旧分段、清空 active.log。线程安全（与写串行，与读并发安全）。
    ///
    /// 崩溃安全性（三步走）：
    ///   1. 写入 merge-<id>.tmp 与 hint 临时文件并 fsync
    ///   2. 原子 rename 成 segment-<id>.log（先数据后 hint）
    ///   3. 删除旧分段 + 截断 active.log
    ///
    /// 任何时刻崩溃，重启后数据都完整（见文件头注释）。
    ///
    /// 返回 (合并前字节数, 合并后字节数)。
    pub fn merge(&self) -> io::Result<(u64, u64)> {
        let before = self.stats().total_bytes;

        // 锁顺序：active -> (index 读) -> (segments 读 + 各分段锁)
        let mut active = self.shared.active.lock().unwrap();
        active.writer.flush()?;

        let snapshot: Vec<(String, ValuePos)> = {
            let index = self.shared.index.read().unwrap();
            index.iter().map(|(k, v)| (k.clone(), *v)).collect()
        };

        // 1) 先把存活数据全部读到内存（只在持锁期间读，随后释放）
        let mut live: Vec<(String, Vec<u8>)> = Vec::with_capacity(snapshot.len());
        {
            let segments = self.shared.segments.read().unwrap();
            for (key, pos) in &snapshot {
                let value = if pos.file_id == ACTIVE_FILE_ID {
                    let (_, _, value, _) = Self::read_entry_at(&mut active.reader, pos.offset)?
                        .ok_or_else(|| {
                            invalid_data(format!("merge failed: missing data for key {key:?}"))
                        })?;
                    value
                } else {
                    let segment = segments.get(&pos.file_id).ok_or_else(|| {
                        invalid_data(format!("merge failed: segment {} missing", pos.file_id))
                    })?;
                    let mut reader = segment.file.lock().unwrap();
                    let (_, _, value, _) = Self::read_entry_at(&mut reader, pos.offset)?
                        .ok_or_else(|| {
                            invalid_data(format!("merge failed: missing data for key {key:?}"))
                        })?;
                    value
                };
                live.push((key.clone(), value));
            }
        }

        let new_id = self.shared.next_segment_id.load(Ordering::Relaxed);
        let tmp_path = self.shared.dir.join(format!("{MERGE_TMP_PREFIX}{new_id:06}.tmp"));
        let hint_tmp_path = self.shared.dir.join(format!("{HINT_PREFIX}{new_id:06}.tmp"));
        let bloom_tmp_path = self.shared.dir.join(format!("{BLOOM_PREFIX}{new_id:06}.tmp"));
        let final_path = self.shared.dir.join(segment_file_name(new_id));
        let hint_final_path = self.shared.dir.join(hint_file_name(new_id));
        let bloom_final_path = self.shared.dir.join(bloom_file_name(new_id));

        // v0.7：为新分段构建布隆过滤器（目标误判率 1%）
        let mut bloom = Bloom::with_capacity(live.len(), 0.01);
        for (key, _) in &live {
            bloom.insert(key.as_bytes());
        }

        // 2) 存活数据重写进临时文件；同时写 hint（不含 value 的索引备份）
        let mut offset: u64 = 0;
        let mut new_index = KeyDir::new();
        {
            let tmp_file = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&tmp_path)?;
            let mut tmp_writer = BufWriter::new(tmp_file);

            let hint_file = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&hint_tmp_path)?;
            let mut hint_writer = BufWriter::new(hint_file);

            for (key, value) in &live {
                let entry = encode_entry(key, FLAG_PUT, value);
                tmp_writer.write_all(&entry)?;
                // hint 记录这条数据在新分段中的位置，value 本身不写
                hint_writer.write_all(&encode_hint_entry(
                    key,
                    value.len(),
                    entry.len() as u64,
                    offset,
                ))?;
                new_index.insert(
                    key.clone(),
                    ValuePos {
                        file_id: new_id,
                        offset,
                    },
                );
                offset += entry.len() as u64;
            }

            tmp_writer.flush()?;
            tmp_writer.get_ref().sync_all()?;
            hint_writer.flush()?;
            hint_writer.get_ref().sync_all()?;
        } // 关闭临时文件句柄

        // 布隆过滤器很小，直接写临时文件 + fsync
        fs::write(&bloom_tmp_path, bloom.to_bytes())?;
        if let Ok(f) = File::open(&bloom_tmp_path) {
            let _ = f.sync_all();
        }

        // 3) 原子替换：rename 在同一文件系统内是原子的。
        //    顺序：数据 -> hint -> bloom。中途崩溃只会损失加速文件，数据依然完整
        fs::rename(&tmp_path, &final_path)?;
        let _ = fs::rename(&hint_tmp_path, &hint_final_path); // hint 失败不致命
        let _ = fs::rename(&bloom_tmp_path, &bloom_final_path); // bloom 失败不致命

        // 4) 换血：删旧分段（含旧 hint/bloom）、截断 active、切换索引。
        //    锁顺序 active -> index -> segments，与读路径一致，无死锁风险
        let mut index = self.shared.index.write().unwrap();
        let mut segments = self.shared.segments.write().unwrap();
        for &old_id in segments.keys() {
            let _ = fs::remove_file(self.shared.dir.join(segment_file_name(old_id)));
            let _ = fs::remove_file(self.shared.dir.join(hint_file_name(old_id)));
            let _ = fs::remove_file(self.shared.dir.join(bloom_file_name(old_id)));
        }
        segments.clear();
        let new_reader = OpenOptions::new().read(true).open(&final_path)?;
        segments.insert(
            new_id,
            Arc::new(Segment {
                file: Mutex::new(new_reader),
                bloom: Some(bloom),
            }),
        );
        self.shared
            .next_segment_id
            .store(new_id + 1, Ordering::Relaxed);

        active.reader.set_len(0)?;
        active.reader.seek(SeekFrom::Start(0))?;
        active.bytes = 0;
        *index = new_index;
        // 注意：此时仍持有 index 写锁与 segments 写锁，绝不能再调 self.stats()（会重入死锁），
        // 合并后的总字节数直接用新分段的文件大小计算
        let after = fs::metadata(&final_path)?.len();
        Ok((before, after))
    }

    /// 布隆过滤器负查询：返回 false 表示「该 key 在索引与所有分段中一定不存在」。
    ///
    /// 返回 true 只代表「可能存在」（布隆过滤器有假阳性）。
    /// 注意：索引全量驻留内存时，这层过滤对 get 没有收益（见 Bloom 文档）；
    /// 它是为 keydir 分区/落盘场景准备的负查询加速能力，也可用于外部批量校验。
    pub fn may_contain(&self, key: impl AsRef<str>) -> bool {
        let key = key.as_ref();
        if self.shared.index.read().unwrap().contains_key(key) {
            return true;
        }
        let kb = key.as_bytes();
        let segments = self.shared.segments.read().unwrap();
        // 没有 bloom 的分段无法排除 -> 保守返回 true
        segments
            .values()
            .any(|s| s.bloom.as_ref().is_none_or(|b| b.may_contain(kb)))
    }

    /// 所有存活 key（BTreeMap 天然有序，无需额外排序）。
    pub fn keys(&self) -> Vec<String> {
        let index = self.shared.index.read().unwrap();
        index.keys().cloned().collect()
    }

    /// 范围扫描 `[start, end)`：按 key 升序返回键值对（左闭右开，与 BTreeMap::range 一致）。
    ///
    /// 实现取舍：先在索引读锁内收集 key 列表（短持锁），再逐个走 get 取 value。
    /// 这样不会被慢读阻塞写入，也自动继承 get 的并发 merge 重试逻辑；
    /// 代价是每条数据多一次索引查找（O(k log n)），
    /// 未来可用 "持锁读全部 value" 换取一次遍历（O(k) 但锁持有时间长）。
    pub fn range(&self, start: &str, end: &str) -> io::Result<Vec<(String, Vec<u8>)>> {
        let keys: Vec<String> = {
            let index = self.shared.index.read().unwrap();
            index
                .range(start.to_owned()..end.to_owned())
                .map(|(k, _)| k.clone())
                .collect()
        };
        self.collect_values(&keys)
    }

    /// 前缀扫描：返回所有以 `prefix` 开头的键值对（升序）。
    /// 常见用途：按命名空间枚举，如 `user:` 前缀下的全部用户。
    pub fn scan_prefix(&self, prefix: &str) -> io::Result<Vec<(String, Vec<u8>)>> {
        let keys: Vec<String> = {
            let index = self.shared.index.read().unwrap();
            index
                .range(prefix.to_owned()..)
                .take_while(|(k, _)| k.starts_with(prefix))
                .map(|(k, _)| k.clone())
                .collect()
        };
        self.collect_values(&keys)
    }

    /// 批量取 value：并发 merge 期间某条 key 可能刚好被清走，跳过即可。
    fn collect_values(&self, keys: &[String]) -> io::Result<Vec<(String, Vec<u8>)>> {
        let mut out = Vec::with_capacity(keys.len());
        for key in keys {
            if let Some(value) = self.get(key)? {
                out.push((key.clone(), value));
            }
        }
        Ok(out)
    }

    pub fn contains_key(&self, key: impl AsRef<str>) -> bool {
        self.shared.index.read().unwrap().contains_key(key.as_ref())
    }

    pub fn len(&self) -> usize {
        self.shared.index.read().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// 强制把内核缓冲区刷到磁盘。
    pub fn sync(&self) -> io::Result<()> {
        let mut active = self.shared.active.lock().unwrap();
        active.writer.flush()?;
        active.writer.get_ref().sync_all()
    }

    /// 数据目录路径。
    pub fn dir(&self) -> &Path {
        &self.shared.dir
    }

    /// 统计信息：垃圾（被覆盖/删除的旧值）越多，越说明需要 merge。
    ///
    /// 锁顺序必须与全局一致（active -> index -> segments）。
    /// 反过来先锁 index 再锁 active 会与写路径形成死锁环：
    /// 写线程持 active 等 index，stats 持 index 等 active —— v0.5 开发中的真实教训。
    pub fn stats(&self) -> Stats {
        let active_bytes = self.shared.active.lock().unwrap().bytes;
        let index = self.shared.index.read().unwrap();
        let segments = self.shared.segments.read().unwrap();
        let seg_bytes: u64 = segments
            .values()
            .filter_map(|s| s.file.lock().unwrap().metadata().ok().map(|m| m.len()))
            .sum();
        Stats {
            live_keys: index.len(),
            active_bytes,
            segment_count: segments.len(),
            total_bytes: seg_bytes + active_bytes,
            corrupt_tail_bytes: self.shared.corrupt_tail_bytes.load(Ordering::Relaxed),
            bloom_count: segments.values().filter(|s| s.bloom.is_some()).count(),
        }
    }
}

/// 简单统计信息。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Stats {
    /// 内存索引中的存活 key 数
    pub live_keys: usize,
    /// active.log 字节数
    pub active_bytes: u64,
    /// 只读分段文件数量
    pub segment_count: usize,
    /// active.log + 全部分段的字节数（含垃圾）
    pub total_bytes: u64,
    /// 打开时发现的损坏尾部字节数（已被截断丢弃，仅作报告用）
    pub corrupt_tail_bytes: u64,
    /// 已加载布隆过滤器的分段数
    pub bloom_count: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use std::thread;
    use std::time::Duration;

    fn temp_dir(tag: &str) -> PathBuf {
        let base = env::temp_dir().join("mini-bitcask-tests");
        let dir = base.join(format!("{tag}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        dir
    }

    #[test]
    fn test_set_get_overwrite() {
        let dir = temp_dir("basic");
        let db = Bitcask::open(&dir).unwrap();

        db.set("name", "cyrus").unwrap();
        assert_eq!(db.get_string("name").unwrap().as_deref(), Some("cyrus"));

        // 覆盖写：读到的是最新版本
        db.set("name", "v2").unwrap();
        assert_eq!(db.get_string("name").unwrap().as_deref(), Some("v2"));

        // 二进制 value 也能存
        db.set("blob", vec![0u8, 1, 2, 255]).unwrap();
        assert_eq!(db.get("blob").unwrap(), Some(vec![0u8, 1, 2, 255]));
    }

    #[test]
    fn test_delete_and_missing_key() {
        let dir = temp_dir("delete");
        let db = Bitcask::open(&dir).unwrap();

        assert_eq!(db.get_string("nope").unwrap(), None);
        db.set("k", "v").unwrap();
        db.delete("k").unwrap();
        assert_eq!(db.get_string("k").unwrap(), None);
        assert!(!db.contains_key("k"));
        // 删除不存在的 key 不报错
        db.delete("never-existed").unwrap();
    }

    #[test]
    fn test_persistence_across_reopen() {
        let dir = temp_dir("reopen");

        {
            let db = Bitcask::open(&dir).unwrap();
            db.set("a", "1").unwrap();
            db.set("b", "2").unwrap();
            db.set("a", "1-updated").unwrap(); // a 有多个版本，日志里有垃圾
            db.delete("b").unwrap();
            db.sync().unwrap();
        }

        // 重新打开：重放日志后应恢复正确状态
        let db2 = Bitcask::open(&dir).unwrap();
        assert_eq!(db2.get_string("a").unwrap().as_deref(), Some("1-updated"));
        assert_eq!(db2.get_string("b").unwrap(), None);
        assert_eq!(db2.len(), 1);
        assert_eq!(db2.keys(), vec!["a".to_string()]);
    }

    #[test]
    fn test_large_values_and_many_keys() {
        let dir = temp_dir("stress");
        let db = Bitcask::open(&dir).unwrap();

        let big_value = vec![42u8; 1 << 20]; // 1 MiB value
        for i in 0..100u32 {
            db.set(format!("key{i}"), big_value.clone()).unwrap();
        }
        assert_eq!(db.len(), 100);
        assert_eq!(db.get("key99").unwrap().unwrap().len(), 1 << 20);

        // 覆盖一半，制造垃圾数据
        for i in 0..50u32 {
            db.set(format!("key{i}"), b"small".to_vec()).unwrap();
        }
        let stats = db.stats();
        assert_eq!(stats.live_keys, 100);
        // 日志里应该有明显的垃圾占比（50 个 1MiB 旧版本）
        assert!(stats.total_bytes > 50 * (1 << 20));
    }

    #[test]
    fn test_crc_rejects_corrupt_tail() {
        let dir = temp_dir("crc");
        let data_file = dir.join(ACTIVE_FILE);

        {
            let db = Bitcask::open(&dir).unwrap();
            db.set("a", "1").unwrap();
            db.set("b", "2").unwrap();
            db.set("c", "3").unwrap();
            db.sync().unwrap();
        }

        // 模拟崩溃写坏最后一条记录：翻转日志最后一个字节
        let mut bytes = fs::read(&data_file).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF;
        fs::write(&data_file, &bytes).unwrap();

        // 重新打开：前两条完好，损坏的 c 被 CRC 拦下并截断
        let db = Bitcask::open(&dir).unwrap();
        assert_eq!(db.get_string("a").unwrap().as_deref(), Some("1"));
        assert_eq!(db.get_string("b").unwrap().as_deref(), Some("2"));
        assert_eq!(db.get_string("c").unwrap(), None);
        assert!(db.stats().corrupt_tail_bytes > 0);
        assert_eq!(db.len(), 2);

        // 截断之后继续写入，新数据必须可见
        db.set("d", "4").unwrap();
        db.sync().unwrap();

        let db2 = Bitcask::open(&dir).unwrap();
        assert_eq!(db2.get_string("d").unwrap().as_deref(), Some("4"));
        assert_eq!(db2.len(), 3);
        assert_eq!(db2.stats().corrupt_tail_bytes, 0);
    }

    #[test]
    fn test_crc32_known_vectors() {
        // 与标准 CRC-32 (zlib/zip) 对齐的已知向量
        assert_eq!(crc32(b""), 0x0000_0000);
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
        assert_eq!(crc32(b"hello world"), 0x0D4A_1185);
    }

    // ---------- v0.3：merge / compaction ----------

    #[test]
    fn test_merge_compacts_garbage() {
        let dir = temp_dir("merge-compact");
        let db = Bitcask::open(&dir).unwrap();

        // 100 个 key，每个覆盖写 10 次 → 90% 是垃圾
        let value = vec![7u8; 1024];
        for _round in 0..10 {
            for i in 0..100u32 {
                db.set(format!("key{i}"), &value[..]).unwrap();
            }
        }
        let before = db.stats();
        assert_eq!(before.live_keys, 100);

        let (bytes_before, bytes_after) = db.merge().unwrap();
        assert_eq!(bytes_before, before.total_bytes);
        // 压缩后应该只剩 100 条记录：大致降到原来的 1/10
        assert!(
            bytes_after * 5 < bytes_before,
            "merge 未生效: {bytes_before} -> {bytes_after}"
        );

        // 数据必须全部完好
        for i in 0..100u32 {
            assert_eq!(db.get(format!("key{i}")).unwrap().unwrap(), value);
        }

        // 重启后依然完好（数据已落在 segment 文件里）
        drop(db);
        let db2 = Bitcask::open(&dir).unwrap();
        assert_eq!(db2.len(), 100);
        assert_eq!(db2.get("key42").unwrap().unwrap(), value);
        assert_eq!(db2.stats().segment_count, 1);
    }

    #[test]
    fn test_merge_keeps_deleted_keys_deleted() {
        let dir = temp_dir("merge-delete");
        let db = Bitcask::open(&dir).unwrap();

        db.set("keep", "v").unwrap();
        db.set("gone", "v").unwrap();
        db.delete("gone").unwrap();
        db.merge().unwrap();

        assert_eq!(db.get_string("keep").unwrap().as_deref(), Some("v"));
        assert_eq!(db.get_string("gone").unwrap(), None);

        drop(db);
        let db2 = Bitcask::open(&dir).unwrap();
        assert_eq!(db2.len(), 1);
        assert_eq!(db2.get_string("gone").unwrap(), None);
    }

    #[test]
    fn test_merge_then_write_then_merge_again() {
        let dir = temp_dir("merge-twice");
        let db = Bitcask::open(&dir).unwrap();

        db.set("a", "1").unwrap();
        db.merge().unwrap();
        db.set("b", "2").unwrap();
        db.merge().unwrap();

        assert_eq!(db.get_string("a").unwrap().as_deref(), Some("1"));
        assert_eq!(db.get_string("b").unwrap().as_deref(), Some("2"));
        assert_eq!(db.stats().segment_count, 1); // 旧分段已被清理

        drop(db);
        let db2 = Bitcask::open(&dir).unwrap();
        assert_eq!(db2.len(), 2);
        assert_eq!(db2.get_string("b").unwrap().as_deref(), Some("2"));
    }

    #[test]
    fn test_leftover_merge_tmp_is_cleaned_on_open() {
        let dir = temp_dir("merge-tmp");
        fs::create_dir_all(&dir).unwrap();
        let leftover = dir.join(format!("{MERGE_TMP_PREFIX}000042.tmp"));
        fs::write(&leftover, b"garbage from crashed merge").unwrap();
        let leftover_hint = dir.join(format!("{HINT_PREFIX}000042.tmp"));
        fs::write(&leftover_hint, b"garbage from crashed merge hint").unwrap();

        let db = Bitcask::open(&dir).unwrap();
        assert!(!leftover.exists(), "崩溃遗留的 merge 临时文件应被清理");
        assert!(!leftover_hint.exists(), "崩溃遗留的 hint 临时文件应被清理");
        assert_eq!(db.len(), 0);
    }

    #[test]
    fn test_empty_merge_creates_no_segment() {
        let dir = temp_dir("merge-empty");
        let db = Bitcask::open(&dir).unwrap();

        db.set("x", "1").unwrap();
        db.delete("x").unwrap();
        db.merge().unwrap();

        assert!(db.is_empty());
        assert_eq!(db.stats().total_bytes, 0);

        drop(db);
        let db2 = Bitcask::open(&dir).unwrap();
        assert_eq!(db2.len(), 0);
    }

    // ---------- v0.4：hint file ----------

    #[test]
    fn test_hint_rebuilds_index_without_scanning_data() {
        let dir = temp_dir("hint");
        let seg_path = dir.join(segment_file_name(1));
        let hint_path = dir.join(hint_file_name(1));

        let value = vec![9u8; 4096];
        {
            let db = Bitcask::open(&dir).unwrap();
            for i in 0..50u32 {
                db.set(format!("key{i}"), value.clone()).unwrap();
            }
            db.merge().unwrap();
            db.sync().unwrap();
        }
        assert!(hint_path.exists(), "merge 后应生成 hint 文件");

        // 破坏数据文件的 value 内容（CRC 必然失效）。
        // 若重启走的是 hint 快速路径，索引照常重建；若走数据重放则 key 会丢失。
        let mut bytes = fs::read(&seg_path).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF;
        fs::write(&seg_path, &bytes).unwrap();

        let db = Bitcask::open(&dir).unwrap();
        // 50 个 key 全部由 hint 恢复（数据文件已被破坏，重放路径做不到这一点）
        assert_eq!(db.len(), 50);
        // 分段写入顺序是哈希序，无法预知被破坏的是哪个 key：
        // 恰好一个 key 的数据被 CRC 拦下，其余 49 个照常可读
        let broken: Vec<String> = (0..50u32)
            .map(|i| format!("key{i}"))
            .filter(|k| db.get(k).is_err())
            .collect();
        assert_eq!(broken.len(), 1, "恰好一条数据被破坏: {broken:?}");
    }

    #[test]
    fn test_hint_fallback_when_missing_or_invalid() {
        let dir = temp_dir("hint-fallback");
        let value = vec![1u8; 512];

        {
            let db = Bitcask::open(&dir).unwrap();
            for i in 0..20u32 {
                db.set(format!("k{i}"), value.clone()).unwrap();
            }
            db.merge().unwrap();
        }
        let hint = dir.join(hint_file_name(1));

        // 场景 1：hint 尾部残缺 -> 整个 hint 作废，回退全量重放
        let mut bytes = fs::read(&hint).unwrap();
        bytes.truncate(bytes.len() - 3); // 切掉 3 个字节，最后一条 hint 记录不完整
        fs::write(&hint, &bytes).unwrap();

        let db = Bitcask::open(&dir).unwrap();
        assert_eq!(db.len(), 20, "残缺 hint 必须被拒绝，回退到数据重放");
        assert_eq!(db.get("k0").unwrap().unwrap(), value);
        drop(db);

        // 场景 2：hint 缺失 -> 回退全量重放
        fs::remove_file(&hint).unwrap();
        let db2 = Bitcask::open(&dir).unwrap();
        assert_eq!(db2.len(), 20);
        assert_eq!(db2.get("k7").unwrap().unwrap(), value);
    }

    #[test]
    fn test_merge_removes_old_hint_files() {
        let dir = temp_dir("hint-cleanup");
        let db = Bitcask::open(&dir).unwrap();

        db.set("a", "1").unwrap();
        db.merge().unwrap();
        assert!(dir.join(hint_file_name(1)).exists());

        db.set("b", "2").unwrap();
        db.merge().unwrap(); // 第二次 merge：旧分段 1 及其 hint 应被删除

        assert!(!dir.join(hint_file_name(1)).exists());
        assert!(dir.join(hint_file_name(2)).exists());
        assert_eq!(db.len(), 2);

        drop(db);
        let db2 = Bitcask::open(&dir).unwrap();
        assert_eq!(db2.len(), 2);
        assert_eq!(db2.get_string("a").unwrap().as_deref(), Some("1"));
        assert_eq!(db2.get_string("b").unwrap().as_deref(), Some("2"));
    }

    // ---------- v0.5：并发句柄 ----------

    // ---------- v0.6：BTreeMap 有序索引 ----------

    #[test]
    fn test_range_query_sorted_and_half_open() {
        let dir = temp_dir("range");
        let db = Bitcask::open(&dir).unwrap();

        for k in ["a", "b", "c", "d", "e"] {
            db.set(k, format!("v-{k}")).unwrap();
        }

        // 左闭右开：包含 b/c/d，不含 a/e
        let rows = db.range("b", "e").unwrap();
        let keys: Vec<&str> = rows.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(keys, vec!["b", "c", "d"]);
        assert_eq!(rows[0].1, b"v-b".to_vec());

        // 空结果与边界
        assert!(db.range("x", "z").unwrap().is_empty());
        assert_eq!(db.range("", "a").unwrap().len(), 0); // [.., "a") 不含 a

        // 删除后范围查询即时反映
        db.delete("c").unwrap();
        let keys: Vec<String> = db.range("b", "e").unwrap().into_iter().map(|(k, _)| k).collect();
        assert_eq!(keys, vec!["b".to_string(), "d".to_string()]);
    }

    #[test]
    fn test_scan_prefix() {
        let dir = temp_dir("prefix");
        let db = Bitcask::open(&dir).unwrap();

        db.set("user:1", "alice").unwrap();
        db.set("user:2", "bob").unwrap();
        db.set("user:10", "carol").unwrap();
        db.set("order:1", "o1").unwrap();
        db.set("usr:1", "typo").unwrap();

        let rows = db.scan_prefix("user:").unwrap();
        let keys: Vec<String> = rows.iter().map(|(k, _)| k.clone()).collect();
        // BTreeMap 字典序："user:10" 排在 "user:2" 之前（字符串序，不是数值序）
        assert_eq!(keys, vec!["user:1", "user:10", "user:2"]);

        assert_eq!(db.scan_prefix("nope:").unwrap().len(), 0);
        assert_eq!(db.scan_prefix("").unwrap().len(), 5); // 空前缀 = 全部
    }

    #[test]
    fn test_range_survives_reopen_and_merge() {
        let dir = temp_dir("range-reopen");

        {
            let db = Bitcask::open(&dir).unwrap();
            for i in 0..20u32 {
                db.set(format!("key{i:02}"), format!("v{i}")).unwrap();
            }
            db.merge().unwrap();
            db.sync().unwrap();
        }

        // 重启（hint 或重放）+ 索引仍有序
        let db = Bitcask::open(&dir).unwrap();
        let keys: Vec<String> = db
            .range("key05", "key09")
            .unwrap()
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        assert_eq!(
            keys,
            vec!["key05", "key06", "key07", "key08"]
                .into_iter()
                .map(String::from)
                .collect::<Vec<_>>()
        );
        assert_eq!(db.keys().len(), 20);
    }

    // ---------- v0.7：布隆过滤器 ----------

    #[test]
    fn test_bloom_no_false_negatives_and_low_fp_rate() {
        let n = 10_000;
        let mut bloom = Bloom::with_capacity(n, 0.01);
        for i in 0..n {
            bloom.insert(format!("key{i}").as_bytes());
        }
        // 无假阴性：插入过的 key 一定报"可能存在"
        for i in 0..n {
            assert!(bloom.may_contain(format!("key{i}").as_bytes()));
        }
        // 假阳性率应接近理论值（1%），给足余量避免抖动
        let probes = 10_000;
        let mut fp = 0;
        for i in 0..probes {
            if bloom.may_contain(format!("absent-{i}").as_bytes()) {
                fp += 1;
            }
        }
        let rate = fp as f64 / probes as f64;
        assert!(rate < 0.03, "假阳性率过高: {rate:.4}（理论约 {:.4}）", bloom.fp_rate_estimate(n));
    }

    #[test]
    fn test_bloom_serde_roundtrip() {
        let mut bloom = Bloom::with_capacity(100, 0.01);
        for i in 0..100 {
            bloom.insert(format!("k{i}").as_bytes());
        }
        let bytes = bloom.to_bytes();
        let restored = Bloom::from_bytes(&bytes).expect("应能反序列化");
        for i in 0..100 {
            assert!(restored.may_contain(format!("k{i}").as_bytes()));
        }
        // 损坏数据必须被拒绝（宁可没有 bloom，也不能误判）
        assert!(Bloom::from_bytes(b"bad magic....").is_none());
        assert!(Bloom::from_bytes(&bytes[..bytes.len() - 1]).is_none());
    }

    #[test]
    fn test_bloom_files_lifecycle() {
        let dir = temp_dir("bloom-life");

        {
            let db = Bitcask::open(&dir).unwrap();
            for i in 0..50u32 {
                db.set(format!("key{i:02}"), "v").unwrap();
            }
            assert!(!db.may_contain("definitely-absent"));
            db.merge().unwrap();

            // merge 后应生成 bloom 文件，且 stats 反映出来
            assert!(dir.join(bloom_file_name(1)).exists());
            assert_eq!(db.stats().bloom_count, 1);
            assert!(db.may_contain("key07"));
            assert!(!db.may_contain("definitely-absent"));

            // 第二次 merge：旧 bloom 应随旧分段一起清理
            db.set("key00", "v2").unwrap();
            db.merge().unwrap();
            assert!(!dir.join(bloom_file_name(1)).exists());
            assert!(dir.join(bloom_file_name(2)).exists());
        }

        // 重启后 bloom 应被恢复（不用重新 merge）
        let db2 = Bitcask::open(&dir).unwrap();
        assert_eq!(db2.stats().bloom_count, 1);
        assert!(db2.may_contain("key42"));
        assert!(!db2.may_contain("nope-not-here"));
    }

    #[test]
    fn test_bloom_missing_file_degrades_gracefully() {
        let dir = temp_dir("bloom-miss");
        {
            let db = Bitcask::open(&dir).unwrap();
            db.set("a", "1").unwrap();
            db.merge().unwrap();
        }
        // 删掉 bloom 文件：引擎依然可用，只是 bloom_count 为 0
        fs::remove_file(dir.join(bloom_file_name(1))).unwrap();
        let db = Bitcask::open(&dir).unwrap();
        assert_eq!(db.stats().bloom_count, 0);
        assert_eq!(db.get_string("a").unwrap().as_deref(), Some("1"));
        // 没有 bloom 的分段无法排除任何 key -> 保守返回 true
        assert!(db.may_contain("whatever"));
    }

    #[test]
    fn test_handle_clone_shares_state() {
        let dir = temp_dir("clone");
        let db = Bitcask::open(&dir).unwrap();
        let db2 = db.clone();

        db.set("k", "v1").unwrap();
        assert_eq!(db2.get_string("k").unwrap().as_deref(), Some("v1"));
        db2.set("k", "v2").unwrap();
        assert_eq!(db.get_string("k").unwrap().as_deref(), Some("v2"));

        db2.delete("k").unwrap();
        assert_eq!(db.get_string("k").unwrap(), None);
        drop(db2);
        assert_eq!(db.len(), 0); // db 依然可用（Arc 引用计数）
    }

    #[test]
    fn test_concurrent_writer_and_readers() {
        let dir = temp_dir("concurrent");
        let db = Bitcask::open(&dir).unwrap();

        // 1 个写线程 + 4 个读线程同时跑 2000 个 key（key/value 统一零填充）
        let writer = {
            let db = db.clone();
            thread::spawn(move || {
                for i in 0..2000u32 {
                    db.set(format!("key{i:04}"), format!("value{i:04}")).unwrap();
                }
            })
        };
        let readers: Vec<_> = (0..4)
            .map(|_| {
                let db = db.clone();
                thread::spawn(move || {
                    for _ in 0..2000 {
                        let keys = db.keys();
                        if let Some(k) = keys.first().cloned() {
                            // 读到的值必须与 key 一致（不允许串位）
                            if let Some(v) = db.get(&k).unwrap() {
                                let expect = format!("value{}", k.trim_start_matches("key"));
                                assert_eq!(String::from_utf8_lossy(&v), expect);
                            }
                        }
                        assert!(db.len() <= 2000);
                    }
                })
            })
            .collect();

        writer.join().unwrap();
        for r in readers {
            r.join().unwrap();
        }

        assert_eq!(db.len(), 2000);
        for i in 0..2000u32 {
            let expect = format!("value{i:04}");
            assert_eq!(
                db.get_string(format!("key{i:04}")).unwrap().as_deref(),
                Some(expect.as_str())
            );
        }
    }

    #[test]
    fn test_merge_during_concurrent_reads_and_writes() {
        let dir = temp_dir("merge-race");
        let db = Bitcask::open(&dir).unwrap();

        // 预置数据，随后一边写、一边反复 merge、一边读
        for i in 0..200u32 {
            db.set(format!("key{i:04}"), format!("value{i:04}")).unwrap();
        }

        let writer = {
            let db = db.clone();
            thread::spawn(move || {
                for i in 200..600u32 {
                    db.set(format!("key{i:04}"), format!("value{i:04}")).unwrap();
                }
            })
        };
        let merger = {
            let db = db.clone();
            thread::spawn(move || {
                for _ in 0..5 {
                    thread::sleep(Duration::from_millis(2));
                    db.merge().unwrap();
                }
            })
        };
        let reader = {
            let db = db.clone();
            thread::spawn(move || {
                for _ in 0..3000 {
                    let keys = db.keys();
                    if let Some(k) = keys.first().cloned() {
                        // 并发 merge 期间读必须永远成功（不允许 Err 漏出）
                        let v = db
                            .get(&k)
                            .unwrap_or_else(|e| panic!("并发 merge 期间读失败: {e}"));
                        if let Some(v) = v {
                            let expect = format!("value{}", k.trim_start_matches("key"));
                            assert_eq!(String::from_utf8_lossy(&v), expect);
                        }
                    }
                }
            })
        };

        writer.join().unwrap();
        merger.join().unwrap();
        reader.join().unwrap();

        // 结束后所有 600 个 key 必须完好
        assert_eq!(db.len(), 600);
        for i in 0..600u32 {
            let expect = format!("value{i:04}");
            assert_eq!(
                db.get_string(format!("key{i:04}")).unwrap().as_deref(),
                Some(expect.as_str())
            );
        }
        db.sync().unwrap();

        // 重启验证
        drop(db);
        let db2 = Bitcask::open(&dir).unwrap();
        assert_eq!(db2.len(), 600);
        assert_eq!(
            db2.get_string("key0599").unwrap().as_deref(),
            Some("value0599")
        );
    }
}

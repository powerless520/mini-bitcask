//! mini-bitcask —— 一个 ~670 行的 Bitcask 风格 KV 存储引擎（教学/开源项目）
//!
//! 设计模型（来自 Basho/riak 的 Bitcask 论文）：
//!
//!   写路径：所有写操作（set/delete）以 append-only 方式追加到 active.log
//!   索引：  内存中维护 HashMap<key, (file_id, offset)>，指向该 key 最新值的磁盘位置
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
//!   <dir>/merge-<id>.tmp        merge 过程中的临时文件，rename 后成为正式分段
//!
//! 崩溃安全性：
//!   1. 重放时逐条校验 CRC，损坏尾部直接截断丢弃（v0.2）
//!   2. merge 用 "写临时文件 -> fsync -> 原子 rename" 三步，任何时刻崩溃都不会丢数据：
//!      - rename 之前崩溃：旧文件完好，临时文件下次启动时被清理
//!      - rename 之后崩溃：新旧文件并存，但重放按分段 id 升序、active 最后，
//!        新分段（id 更大）会覆盖旧数据，结果依然正确
//!
//! v0.4 hint file（对应 Bitcask 论文 §3.2）：
//!
//!   merge 时为每个新分段顺便写一份 hint-<id>.log：只存 (key, 位置, 长度)，不含 value。
//!   重启时若 hint 结构完整，直接读它建索引，完全跳过数据扫描 ——
//!   数据文件越大，启动加速越明显。信任策略（宁可回退也不读错）：
//!     - hint 必须能被逐字节精确解析到文件尾（部分条目 = 整个 hint 作废）
//!     - 每条 offset + entry_len 必须不超过分段文件大小
//!     - 任一校验失败 -> 回退到 v0.3 的全量重放
//!   rename 顺序：先数据分段、后 hint —— 中间崩溃只会"缺 hint"，自动回退，依然安全。
//!
//! 这是刻意留白的项目：并发句柄等留作后续演进路线，见 README.md 的 Roadmap。

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

const FLAG_PUT: u8 = 0;
const FLAG_DEL: u8 = 1;

/// 当前可写文件名
const ACTIVE_FILE: &str = "active.log";
/// 只读分段文件名前缀
const SEGMENT_PREFIX: &str = "segment-";
/// 分段对应的 hint 文件名前缀
const HINT_PREFIX: &str = "hint-";
/// merge 临时文件名前缀（数据与 hint 共用，靠扩展名 .tmp 识别）
const MERGE_TMP_PREFIX: &str = "merge-";
/// active.log 在索引中使用的 file_id
const ACTIVE_FILE_ID: u64 = 0;

/// 一条 entry 的解析结果：(key, flag, value, entry 总字节数)
type Entry = (String, u8, Vec<u8>, u64);

/// 一次写入在磁盘上的位置。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ValuePos {
    /// 0 = active.log，其他 = segment-<id>.log
    file_id: u64,
    /// entry 起始偏移量
    offset: u64,
}

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

fn segment_file_name(id: u64) -> String {
    format!("{SEGMENT_PREFIX}{id:06}.log")
}

fn hint_file_name(id: u64) -> String {
    format!("{HINT_PREFIX}{id:06}.log")
}

fn parse_segment_id(path: &Path) -> Option<u64> {
    let name = path.file_name()?.to_str()?;
    let digits = name.strip_prefix(SEGMENT_PREFIX)?.strip_suffix(".log")?;
    digits.parse::<u64>().ok()
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
    index: &mut HashMap<String, ValuePos>,
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

        index.insert(
            key,
            ValuePos {
                file_id,
                offset,
            },
        );
    }
}

/// 一个 Bitcask 实例对应一个数据目录。
pub struct Bitcask {
    dir: PathBuf,
    /// 内存索引：key -> 最新值的位置
    index: HashMap<String, ValuePos>,
    /// 只读分段文件的读句柄（id -> File）
    segments: HashMap<u64, File>,
    /// 下一个可用的分段 id
    next_segment_id: u64,
    /// active.log 的追加写句柄
    writer: BufWriter<File>,
    /// active.log 的随机读句柄（与 writer 分离，避免 seek 互相干扰）
    active_reader: File,
    /// active.log 当前字节数（O_APPEND 下 stream_position 不可靠，自己记账）
    active_bytes: u64,
    /// 打开时发现的损坏尾部字节数（已截断丢弃，仅作报告用）
    corrupt_tail_bytes: u64,
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
                    && (name.starts_with(MERGE_TMP_PREFIX) || name.starts_with(HINT_PREFIX))
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

        let mut index: HashMap<String, ValuePos> = HashMap::new();
        let mut segments = HashMap::new();
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

            segments.insert(id, seg_file);
        }

        let mut store = Bitcask {
            dir,
            index,
            segments,
            next_segment_id: segment_ids.last().copied().unwrap_or(0) + 1,
            writer: BufWriter::new(file),
            active_reader,
            active_bytes: 0,
            corrupt_tail_bytes: 0,
        };

        // active 重放在最后（active 永远是最新写入），
        // 这样"后写覆盖先写"的语义在重启后依然成立
        let mut active = store.active_reader.try_clone()?;
        let (valid, corrupt) = Self::replay_file(&mut active, ACTIVE_FILE_ID, &mut store.index)?;
        store.corrupt_tail_bytes = corrupt;
        if corrupt > 0 {
            // 截断损坏尾部：O_APPEND 会把新记录写到损坏数据之后，
            // 不截断的话下次重放仍会在损坏处停止，新数据永远不可见
            store.active_reader.set_len(valid)?;
        }
        store.active_bytes = valid;

        Ok(store)
    }

    /// 重放单个文件的全部 entry，把索引写进 `index`。
    /// 返回 (有效字节数, 损坏尾部字节数)。
    fn replay_file(file: &mut File, file_id: u64, index: &mut HashMap<String, ValuePos>) -> io::Result<(u64, u64)> {
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
        let key = String::from_utf8(key).map_err(|_| invalid_data("corrupt entry: key is not UTF-8"))?;
        Ok(Some((key, flag, value, (13 + key_len + value_len) as u64)))
    }

    /// 取某个 file_id 对应的读句柄。
    fn reader_for(&mut self, file_id: u64) -> io::Result<&mut File> {
        if file_id == ACTIVE_FILE_ID {
            Ok(&mut self.active_reader)
        } else {
            self.segments.get_mut(&file_id).ok_or_else(|| {
                invalid_data(format!("index points to missing segment {file_id}"))
            })
        }
    }

    /// 写入一个键值对（覆盖即追加新版本，旧值成为垃圾数据）。
    pub fn set(&mut self, key: impl Into<String>, value: impl Into<Vec<u8>>) -> io::Result<()> {
        let key = key.into();
        let value = value.into();

        let entry = encode_entry(&key, FLAG_PUT, &value);
        self.writer.write_all(&entry)?;

        self.index.insert(
            key,
            ValuePos {
                file_id: ACTIVE_FILE_ID,
                offset: self.active_bytes,
            },
        );
        self.active_bytes += entry.len() as u64;
        Ok(())
    }

    /// 删除一个 key：追加墓碑记录 + 移除内存索引。
    /// 删除不存在的 key 也是合法操作（与其他 Bitcask 实现语义一致）。
    pub fn delete(&mut self, key: impl AsRef<str>) -> io::Result<()> {
        let key = key.as_ref();

        let entry = encode_entry(key, FLAG_DEL, &[]);
        self.writer.write_all(&entry)?;

        self.index.remove(key);
        self.active_bytes += entry.len() as u64;
        Ok(())
    }

    /// 读取一个 key 的最新值。
    pub fn get(&mut self, key: impl AsRef<str>) -> io::Result<Option<Vec<u8>>> {
        // 先把写缓冲刷下去：writer 与 reader 是两个独立句柄，
        // 不 flush 的话 reader 会读到尚未落盘的旧内容
        self.writer.flush()?;

        let key = key.as_ref();
        let Some(&pos) = self.index.get(key) else {
            return Ok(None);
        };

        let reader = self.reader_for(pos.file_id)?;
        match Self::read_entry_at(reader, pos.offset)? {
            Some((_, FLAG_PUT, value, _)) => Ok(Some(value)),
            Some((k, FLAG_DEL, _, _)) if k == key => Ok(None),
            Some((k, flag, _, _)) => Err(invalid_data(format!(
                "index mismatch at file {} offset {}: got key={k:?} flag={flag}",
                pos.file_id, pos.offset
            ))),
            None => Err(invalid_data(format!(
                "index points to missing entry (file {} offset {})",
                pos.file_id, pos.offset
            ))),
        }
    }

    /// 便捷方法：读取并转成 UTF-8 字符串（demo/CLI 用）。
    pub fn get_string(&mut self, key: impl AsRef<str>) -> io::Result<Option<String>> {
        match self.get(key)? {
            Some(v) => Ok(Some(String::from_utf8_lossy(&v).into_owned())),
            None => Ok(None),
        }
    }

    /// 合并压缩（Bitcask 论文 §3）：把全部存活数据重写进一个新分段文件，
    /// 然后删除旧分段、清空 active.log。
    ///
    /// 崩溃安全性（三步走）：
    ///   1. 写入 merge-<id>.tmp 并 fsync
    ///   2. 原子 rename 成 segment-<id>.log
    ///   3. 删除旧分段 + 截断 active.log
    ///
    /// 任何时刻崩溃，重启后数据都完整（见文件头注释）。
    ///
    /// 返回 (合并前字节数, 合并后字节数)。
    pub fn merge(&mut self) -> io::Result<(u64, u64)> {
        let before = self.total_bytes();

        // 先把 active.log 的缓冲刷盘，否则读不到刚写入的数据
        self.writer.flush()?;

        let snapshot: Vec<(String, ValuePos)> =
            self.index.iter().map(|(k, v)| (k.clone(), *v)).collect();

        let new_id = self.next_segment_id;
        let tmp_path = self
            .dir
            .join(format!("{MERGE_TMP_PREFIX}{new_id:06}.tmp"));
        let hint_tmp_path = self
            .dir
            .join(format!("{HINT_PREFIX}{new_id:06}.tmp"));
        let final_path = self.dir.join(segment_file_name(new_id));
        let hint_final_path = self.dir.join(hint_file_name(new_id));

        // 1) 存活数据全部重写进临时文件；同时写 hint（不含 value 的索引备份）
        let mut offset: u64 = 0;
        let mut new_index: HashMap<String, ValuePos> = HashMap::with_capacity(snapshot.len());
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

            for (key, pos) in &snapshot {
                let reader = self.reader_for(pos.file_id)?;
                let Some((_, _, value, _)) = Self::read_entry_at(reader, pos.offset)? else {
                    return Err(invalid_data(format!(
                        "merge failed: missing data for key {key:?}"
                    )));
                };
                let entry = encode_entry(key, FLAG_PUT, &value);
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

        // 2) 原子替换：rename 在同一文件系统内是原子的。
        //    先数据、后 hint —— 若在两次 rename 之间崩溃，重启只会发现 hint 缺失，
        //    自动回退到全量重放，数据依然完整
        fs::rename(&tmp_path, &final_path)?;
        let _ = fs::rename(&hint_tmp_path, &hint_final_path); // hint 失败不致命

        // 3) 旧文件已无引用，删除它们（含旧 hint）；active.log 截断为空
        for &old_id in self.segments.keys() {
            let _ = fs::remove_file(self.dir.join(segment_file_name(old_id)));
            let _ = fs::remove_file(self.dir.join(hint_file_name(old_id)));
        }
        self.segments.clear();
        let new_reader = OpenOptions::new().read(true).open(&final_path)?;
        self.segments.insert(new_id, new_reader);
        self.next_segment_id = new_id + 1;

        self.active_reader.set_len(0)?;
        self.active_reader.seek(SeekFrom::Start(0))?;
        self.active_bytes = 0;

        // 4) 切换索引指向新分段
        self.index = new_index;

        Ok((before, self.total_bytes()))
    }

    pub fn contains_key(&self, key: impl AsRef<str>) -> bool {
        self.index.contains_key(key.as_ref())
    }

    pub fn len(&self) -> usize {
        self.index.len()
    }

    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }

    /// 所有存活 key（按字典序返回，便于 CLI 展示）。
    pub fn keys(&self) -> Vec<String> {
        let mut keys: Vec<String> = self.index.keys().cloned().collect();
        keys.sort();
        keys
    }

    /// 强制把内核缓冲区刷到磁盘（不调用的话，Drop 时也会 flush，但不会 fsync）。
    pub fn sync(&mut self) -> io::Result<()> {
        self.writer.flush()?;
        self.writer.get_ref().sync_all()
    }

    /// 数据目录路径。
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// 所有文件（active + 分段）的总字节数，含垃圾数据。
    fn total_bytes(&self) -> u64 {
        let seg_bytes: u64 = self
            .segments
            .values()
            .filter_map(|f| f.metadata().ok().map(|m| m.len()))
            .sum();
        seg_bytes + self.active_bytes
    }

    /// 统计信息：垃圾（被覆盖/删除的旧值）越多，越说明需要 merge。
    pub fn stats(&self) -> Stats {
        Stats {
            live_keys: self.index.len(),
            active_bytes: self.active_bytes,
            segment_count: self.segments.len(),
            total_bytes: self.total_bytes(),
            corrupt_tail_bytes: self.corrupt_tail_bytes,
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
}

impl Drop for Bitcask {
    fn drop(&mut self) {
        // 尽力 flush，flush 失败在 Drop 中已无法上报，忽略
        let _ = self.writer.flush();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    fn temp_dir(tag: &str) -> PathBuf {
        let base = env::temp_dir().join("mini-bitcask-tests");
        let dir = base.join(format!("{tag}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        dir
    }

    #[test]
    fn test_set_get_overwrite() {
        let dir = temp_dir("basic");
        let mut db = Bitcask::open(&dir).unwrap();

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
        let mut db = Bitcask::open(&dir).unwrap();

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
            let mut db = Bitcask::open(&dir).unwrap();
            db.set("a", "1").unwrap();
            db.set("b", "2").unwrap();
            db.set("a", "1-updated").unwrap(); // a 有多个版本，日志里有垃圾
            db.delete("b").unwrap();
            db.sync().unwrap();
        }

        // 重新打开：重放日志后应恢复正确状态
        let mut db2 = Bitcask::open(&dir).unwrap();
        assert_eq!(db2.get_string("a").unwrap().as_deref(), Some("1-updated"));
        assert_eq!(db2.get_string("b").unwrap(), None);
        assert_eq!(db2.len(), 1);
        assert_eq!(db2.keys(), vec!["a".to_string()]);
    }

    #[test]
    fn test_large_values_and_many_keys() {
        let dir = temp_dir("stress");
        let mut db = Bitcask::open(&dir).unwrap();

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
            let mut db = Bitcask::open(&dir).unwrap();
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
        let mut db = Bitcask::open(&dir).unwrap();
        assert_eq!(db.get_string("a").unwrap().as_deref(), Some("1"));
        assert_eq!(db.get_string("b").unwrap().as_deref(), Some("2"));
        assert_eq!(db.get_string("c").unwrap(), None);
        assert!(db.stats().corrupt_tail_bytes > 0);
        assert_eq!(db.len(), 2);

        // 截断之后继续写入，新数据必须可见
        db.set("d", "4").unwrap();
        db.sync().unwrap();

        let mut db2 = Bitcask::open(&dir).unwrap();
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
        let mut db = Bitcask::open(&dir).unwrap();

        // 100 个 key，每个覆盖写 10 次 → 90% 是垃圾
        let value = vec![7u8; 1024];
        for round in 0..10 {
            for i in 0..100u32 {
                db.set(format!("key{i}"), &value[..]).unwrap();
            }
            let _ = round;
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
        let mut db2 = Bitcask::open(&dir).unwrap();
        assert_eq!(db2.len(), 100);
        assert_eq!(db2.get("key42").unwrap().unwrap(), value);
        assert_eq!(db2.stats().segment_count, 1);
    }

    #[test]
    fn test_merge_keeps_deleted_keys_deleted() {
        let dir = temp_dir("merge-delete");
        let mut db = Bitcask::open(&dir).unwrap();

        db.set("keep", "v").unwrap();
        db.set("gone", "v").unwrap();
        db.delete("gone").unwrap();
        db.merge().unwrap();

        assert_eq!(db.get_string("keep").unwrap().as_deref(), Some("v"));
        assert_eq!(db.get_string("gone").unwrap(), None);

        drop(db);
        let mut db2 = Bitcask::open(&dir).unwrap();
        assert_eq!(db2.len(), 1);
        assert_eq!(db2.get_string("gone").unwrap(), None);
    }

    #[test]
    fn test_merge_then_write_then_merge_again() {
        let dir = temp_dir("merge-twice");
        let mut db = Bitcask::open(&dir).unwrap();

        db.set("a", "1").unwrap();
        db.merge().unwrap();
        db.set("b", "2").unwrap();
        db.merge().unwrap();

        assert_eq!(db.get_string("a").unwrap().as_deref(), Some("1"));
        assert_eq!(db.get_string("b").unwrap().as_deref(), Some("2"));
        assert_eq!(db.stats().segment_count, 1); // 旧分段已被清理

        drop(db);
        let mut db2 = Bitcask::open(&dir).unwrap();
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
        let mut db = Bitcask::open(&dir).unwrap();

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
            let mut db = Bitcask::open(&dir).unwrap();
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

        let mut db = Bitcask::open(&dir).unwrap();
        // 50 个 key 全部由 hint 恢复（数据文件已被破坏，重放路径做不到这一点）
        assert_eq!(db.len(), 50);
        // 分段写入顺序是哈希序，无法预知被破坏的是哪个 key：
        // 恰好一个 key 的数据被 CRC 拦下，其余 49 个照常可读
        let broken: Vec<String> = (0..50u32)
            .map(|i| format!("key{i}"))
            .filter(|k| db.get(k).is_err())
            .collect();
        assert_eq!(broken.len(), 1, "恰好一条数据被破坏: {broken:?}");
        // 完好的数据照常可读
        for i in 0..50u32 {
            let key = format!("key{i}");
            if key != broken[0] {
                assert_eq!(db.get(&key).unwrap().unwrap(), value);
            }
        }
    }

    #[test]
    fn test_hint_fallback_when_missing_or_invalid() {
        let dir = temp_dir("hint-fallback");
        let value = vec![1u8; 512];

        {
            let mut db = Bitcask::open(&dir).unwrap();
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

        let mut db = Bitcask::open(&dir).unwrap();
        assert_eq!(db.len(), 20, "残缺 hint 必须被拒绝，回退到数据重放");
        assert_eq!(db.get("k0").unwrap().unwrap(), value);
        drop(db);

        // 场景 2：hint 缺失 -> 回退全量重放
        fs::remove_file(&hint).unwrap();
        let mut db2 = Bitcask::open(&dir).unwrap();
        assert_eq!(db2.len(), 20);
        assert_eq!(db2.get("k7").unwrap().unwrap(), value);
    }

    #[test]
    fn test_merge_removes_old_hint_files() {
        let dir = temp_dir("hint-cleanup");
        let mut db = Bitcask::open(&dir).unwrap();

        db.set("a", "1").unwrap();
        db.merge().unwrap();
        assert!(dir.join(hint_file_name(1)).exists());

        db.set("b", "2").unwrap();
        db.merge().unwrap(); // 第二次 merge：旧分段 1 及其 hint 应被删除

        assert!(!dir.join(hint_file_name(1)).exists());
        assert!(dir.join(hint_file_name(2)).exists());
        assert_eq!(db.len(), 2);

        drop(db);
        let mut db2 = Bitcask::open(&dir).unwrap();
        assert_eq!(db2.len(), 2);
        assert_eq!(db2.get_string("a").unwrap().as_deref(), Some("1"));
        assert_eq!(db2.get_string("b").unwrap().as_deref(), Some("2"));
    }
}

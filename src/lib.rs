//! mini-bitcask —— 一个 ~300 行的 Bitcask 风格 KV 存储引擎（教学/开源项目）
//!
//! 设计模型（来自 Basho/riak 的 Bitcask 论文）：
//!
//!   写路径：所有写操作（set/delete）以 append-only 方式追加到 data.log
//!   索引：  内存中维护 HashMap<key, offset>，offset 指向该 key 最新值的磁盘位置
//!   读路径：查内存索引 -> 按偏移量直接 seek 读取 value
//!   启动：  重放（replay）data.log 全量条目，重建内存索引
//!
//! 磁盘条目（entry）二进制布局（大端序）：
//!
//!   ┌────────┬──────────┬────────┬──────────────┬─────────────┬─────────────┐
//!   │  crc   │ key_len  │  flag  │  value_len   │  key bytes  │ value bytes │
//!   │  u32   │  u32     │  u8    │  u32         │  key_len B  │ value_len B │
//!   └────────┴──────────┴────────┴──────────────┴─────────────┴─────────────┘
//!
//!   crc = CRC-32（IEEE，多项式 0xEDB88320），覆盖 key_len 起至 value 结尾的全部字节
//!   flag = 0 表示 PUT，1 表示 DELETE（墓碑，value 为空）
//!
//! v0.2 崩溃安全性：重放时逐条校验 CRC，遇到校验失败/不完整的"损坏尾部"
//! 直接截断丢弃（Bitcask 论文的标准做法），保证内存索引永远指向完整记录，
//! 且后续写入永远基于干净的日志。
//!
//! 这是刻意留白的项目：merge/compaction、hint file、并发句柄等
//! 留作后续演进路线，见 README.md 的 Roadmap。

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

const FLAG_PUT: u8 = 0;
const FLAG_DEL: u8 = 1;

/// 一条 entry 的解析结果：(key, flag, value, entry 总字节数)
type Entry = (String, u8, Vec<u8>, u64);

/// CRC-32（IEEE 802.3，多项式 0xEDB88320），查表法。
/// 故意手写而非引入 crc crate —— 本项目坚持零依赖，顺便看清校验和的本质。
fn crc32(data: &[u8]) -> u32 {
    static TABLE: OnceLock<[u32; 256]> = OnceLock::new();
    let table = TABLE.get_or_init(|| {
        let mut t = [0u32; 256];
        for (i, slot) in t.iter_mut().enumerate() {
            let mut c = i as u32;
            for _ in 0..8 {
                c = if c & 1 != 0 { 0xEDB8_8320 ^ (c >> 1) } else { c >> 1 };
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

fn invalid_data(msg: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.to_string())
}

/// 一个 Bitcask 实例对应一个数据目录。
///
/// 文件布局：
///   <dir>/data.log   —— append-only 的数据文件
pub struct Bitcask {
    dir: PathBuf,
    /// 内存索引：key -> 该 key 最新 entry 在 data.log 中的起始偏移量
    index: HashMap<String, u64>,
    /// 追加写入句柄（O_APPEND）
    writer: BufWriter<File>,
    /// 随机读取句柄（与 writer 分离，避免 seek 互相干扰）
    reader: File,
    /// 自维护的写偏移量（O_APPEND 模式下文件句柄的 stream_position 不可靠）
    write_offset: u64,
    /// 打开时发现的损坏尾部字节数（已截断丢弃，仅作报告用）
    corrupt_tail_bytes: u64,
}

impl Bitcask {
    /// 打开（或创建）一个 Bitcask 数据目录，并重放日志重建内存索引。
    pub fn open(dir: impl AsRef<Path>) -> io::Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;
        let data_file = dir.join("data.log");

        // 同一个文件开两个句柄：writer 只追加，reader 只随机读
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&data_file)?;
        let reader = file.try_clone()?;

        let mut store = Bitcask {
            dir,
            index: HashMap::new(),
            writer: BufWriter::new(file),
            reader,
            write_offset: 0,
            corrupt_tail_bytes: 0,
        };
        store.replay()?;
        Ok(store)
    }

    /// 重放 data.log，重建内存索引（也校准 write_offset）。
    ///
    /// 损坏处理策略（v0.2）：
    ///   - EOF / 不完整条目      -> 正常脏尾，静默丢弃
    ///   - CRC 校验失败          -> 视为损坏尾部，报告字节数并截断
    ///   - 其他 IO 错误（磁盘等） -> 直接向上抛出，不吞掉
    fn replay(&mut self) -> io::Result<()> {
        let mut offset: u64 = 0;
        loop {
            match Self::read_entry_at(&mut self.reader, offset) {
                Ok(Some((key, flag, _value, entry_len))) => {
                    match flag {
                        FLAG_PUT => {
                            self.index.insert(key, offset);
                        }
                        FLAG_DEL => {
                            self.index.remove(&key);
                        }
                        _ => break, // 未知 flag：损坏，进入尾部处理
                    }
                    offset += entry_len;
                }
                Ok(None) => break, // EOF / 脏尾
                Err(e) if e.kind() == io::ErrorKind::InvalidData => break, // CRC 失败等损坏尾部
                Err(e) => return Err(e),
            }
        }

        let file_len = self.reader.metadata()?.len();
        self.corrupt_tail_bytes = file_len.saturating_sub(offset);
        if self.corrupt_tail_bytes > 0 {
            // 截断丢弃损坏尾部（Bitcask 标准做法）：
            // 不截断的话，O_APPEND 会把新记录写到损坏数据之后，
            // 下次重放仍会在损坏处停止，新数据永远不可见
            self.reader.set_len(offset)?;
        }
        self.write_offset = offset;
        Ok(())
    }

    /// 从指定偏移量读取一条 entry（含 CRC 校验）。
    /// 返回 Ok(None) 表示该位置已到 EOF 或剩余字节不足以构成完整 entry。
    fn read_entry_at(reader: &mut File, offset: u64) -> io::Result<Option<Entry>> {
        reader.seek(SeekFrom::Start(offset))?;

        // 读 header：crc(u32) + key_len(u32) + flag(u8) + value_len(u32) = 13 字节
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
            return Err(invalid_data(
                "corrupt entry: key/value length exceeds limit",
            ));
        }

        // payload = key_len(u32)+flag(u8)+value_len(u32) 这 9 字节 + key + value
        // （crc 覆盖除自身以外的全部字节，所以把 header 的后 9 字节也放进 payload）
        let mut payload = vec![0u8; 9 + key_len + value_len];
        payload[..9].copy_from_slice(&header[4..13]);
        reader.read_exact(&mut payload[9..])?;

        if crc32(&payload) != expected_crc {
            return Err(invalid_data("corrupt entry: CRC mismatch"));
        }

        let key = payload[9..9 + key_len].to_vec();
        let value = payload[9 + key_len..].to_vec();
        let key = String::from_utf8(key)
            .map_err(|_| invalid_data("corrupt entry: key is not UTF-8"))?;
        Ok(Some((key, flag, value, (13 + key_len + value_len) as u64)))
    }

    /// 写入一个键值对（覆盖即追加新版本，旧值成为垃圾数据）。
    pub fn set(&mut self, key: impl Into<String>, value: impl Into<Vec<u8>>) -> io::Result<()> {
        let key = key.into();
        let value = value.into();

        // 先组装 payload（key_len+flag+value_len+key+value），统一计算 CRC
        let mut payload = Vec::with_capacity(9 + key.len() + value.len());
        payload.extend_from_slice(&(key.len() as u32).to_be_bytes());
        payload.push(FLAG_PUT);
        payload.extend_from_slice(&(value.len() as u32).to_be_bytes());
        payload.extend_from_slice(key.as_bytes());
        payload.extend_from_slice(&value);

        self.writer.write_all(&crc32(&payload).to_be_bytes())?;
        self.writer.write_all(&payload)?;

        // O_APPEND 保证落到文件末尾，偏移量由我们自己记账
        self.index.insert(key, self.write_offset);
        self.write_offset += (4 + payload.len()) as u64;
        Ok(())
    }

    /// 删除一个 key：追加墓碑记录 + 移除内存索引。
    /// 删除不存在的 key 也是合法操作（与其他 Bitcask 实现语义一致）。
    pub fn delete(&mut self, key: impl AsRef<str>) -> io::Result<()> {
        let key = key.as_ref();

        let mut payload = Vec::with_capacity(9 + key.len());
        payload.extend_from_slice(&(key.len() as u32).to_be_bytes());
        payload.push(FLAG_DEL);
        payload.extend_from_slice(&0u32.to_be_bytes());
        payload.extend_from_slice(key.as_bytes());

        self.writer.write_all(&crc32(&payload).to_be_bytes())?;
        self.writer.write_all(&payload)?;

        self.index.remove(key);
        self.write_offset += (4 + payload.len()) as u64;
        Ok(())
    }

    /// 读取一个 key 的最新值。
    pub fn get(&mut self, key: impl AsRef<str>) -> io::Result<Option<Vec<u8>>> {
        // 先把写缓冲刷下去：writer（BufWriter）与 reader 是两个独立句柄，
        // 不 flush 的话 reader 会读到尚未落盘的旧内容（v0.1 的第一个真实 bug，见 README）
        self.writer.flush()?;
        let key = key.as_ref();
        let Some(&offset) = self.index.get(key) else {
            return Ok(None);
        };
        match Self::read_entry_at(&mut self.reader, offset)? {
            Some((_, FLAG_PUT, value, _)) => Ok(Some(value)),
            Some((k, FLAG_DEL, _, _)) if k == key => Ok(None),
            Some((k, flag, _, _)) => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("index mismatch at offset {offset}: got key={k:?} flag={flag}"),
            )),
            None => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "index points to missing entry (log truncated?)",
            )),
        }
    }

    /// 便捷方法：读取并转成 UTF-8 字符串（demo/CLI 用）。
    pub fn get_string(&mut self, key: impl AsRef<str>) -> io::Result<Option<String>> {
        match self.get(key)? {
            Some(v) => Ok(Some(String::from_utf8_lossy(&v).into_owned())),
            None => Ok(None),
        }
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

    /// v0.1 的统计信息：日志总字节数中"活数据"的占比。
    /// 垃圾（被覆盖/删除的旧值）越多，越说明需要 merge —— 这正是 Roadmap 的第一个课题。
    pub fn stats(&self) -> Stats {
        Stats {
            live_keys: self.index.len(),
            log_bytes: self.write_offset,
            corrupt_tail_bytes: self.corrupt_tail_bytes,
        }
    }
}

/// 简单统计信息。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Stats {
    /// 内存索引中的存活 key 数
    pub live_keys: usize,
    /// data.log 当前总字节数（含垃圾，不含已截断的损坏尾部）
    pub log_bytes: u64,
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
            db.set("a", "1-updated").unwrap(); // a 有两个版本，日志里有一个垃圾条目
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
        assert!(stats.log_bytes > 50 * (1 << 20));
    }

    #[test]
    fn test_crc_rejects_corrupt_tail() {
        let dir = temp_dir("crc");
        let data_file = dir.join("data.log");

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
}

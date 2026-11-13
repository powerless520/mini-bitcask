//! mini-bitcask 交互式 CLI（REPL）
//!
//! 用法：
//!   cargo run -- <数据目录>
//!
//! 命令：
//!   set <key> <value>   写入
//!   get <key>           读取
//!   del <key>           删除
//!   list                列出所有 key
//!   stats               查看统计（key 数 / 总字节数 / 分段数）
//!   merge               合并压缩（把存活数据重写进新分段，删除垃圾）
//!   range <a> <b>       范围扫描 [a, b)，按 key 升序
//!   prefix <p>          前缀扫描（如 prefix user:）
//!   bloom <k>           布隆过滤器负查询
//!   exit                退出（自动 flush）

use mini_bitcask::Bitcask;
use std::io::{self, BufRead, Write};
use std::path::PathBuf;
use std::process::exit;

fn main() {
    let dir = match std::env::args().nth(1) {
        Some(d) => PathBuf::from(d),
        None => {
            eprintln!("用法: mini-bitcask <数据目录>");
            eprintln!("示例: mini-bitcask ./mydata");
            exit(1);
        }
    };

    let db = match Bitcask::open(&dir) {
        Ok(db) => db,
        Err(e) => {
            eprintln!("打开数据目录 {dir:?} 失败: {e}");
            exit(1);
        }
    };

    println!("mini-bitcask v0.8 (dir: {})", dir.display());
    println!("命令: set <k> <v> | get <k> | del <k> | list | stats");
    println!("      merge | range <a> <b> | prefix <p> | bloom <k> | exit");
    println!("共 {} 个 key 从日志中恢复", db.len());

    let stdin = io::stdin();
    let mut stdout = io::stdout();

    loop {
        print!("> ");
        if stdout.flush().is_err() {
            break;
        }

        let mut line = String::new();
        if stdin.lock().read_line(&mut line).unwrap_or(0) == 0 {
            break; // EOF (Ctrl-D)
        }
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        let mut parts = line.splitn(3, char::is_whitespace);
        let cmd = parts.next().unwrap_or("");
        let rest = parts.next().unwrap_or("");
        let arg3 = parts.next().unwrap_or("");

        let result = match (cmd, rest, arg3) {
            ("set", k, v) if !k.is_empty() && !v.is_empty() => {
                db.set(k, v).map(|_| format!("OK (key={k})"))
            }
            ("get", k, _) if !k.is_empty() => db.get_string(k).map(|v| match v {
                Some(s) => s,
                None => "(nil)".to_string(),
            }),
            ("del", k, _) if !k.is_empty() => db.delete(k).map(|_| "OK".to_string()),
            ("list", "", _) => Ok(db.keys().join("\n")),
            ("stats", "", _) => {
                let s = db.stats();
                let mut out = format!(
                    "live_keys={} total_bytes={} (~{:.1} KiB) segments={} active={} B blooms={}",
                    s.live_keys,
                    s.total_bytes,
                    s.total_bytes as f64 / 1024.0,
                    s.segment_count,
                    s.active_bytes,
                    s.bloom_count
                );
                if s.corrupt_tail_bytes > 0 {
                    out.push_str(&format!(
                        " [打开时截断了 {} 字节损坏尾部]",
                        s.corrupt_tail_bytes
                    ));
                }
                Ok(out)
            }
            ("merge", "", _) => db.merge().map(|(before, after)| {
                let ratio = if before > 0 {
                    after as f64 / before as f64 * 100.0
                } else {
                    100.0
                };
                format!("merge 完成: {before} B -> {after} B (压缩到 {ratio:.1}%)")
            }),
            ("range", start, end) if !start.is_empty() && !end.is_empty() => {
                db.range(start, end).map(|rows| format_rows(&rows))
            }
            ("prefix", p, _) if !p.is_empty() => db.scan_prefix(p).map(|rows| format_rows(&rows)),
            ("bloom", k, _) if !k.is_empty() => Ok(if db.contains_key(k) {
                format!("{k}: 存在（精确命中内存索引）")
            } else if db.may_contain(k) {
                format!("{k}: 可能存在（布隆过滤器假阳性）")
            } else {
                format!("{k}: 一定不存在（布隆过滤器排除）")
            }),
            ("exit", "", _) | ("quit", "", _) => {
                let _ = db.sync();
                println!("bye");
                break;
            }
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "未知命令，支持: set/get/del/list/stats/merge/range/prefix/bloom/exit",
            )),
        };

        match result {
            Ok(msg) => println!("{msg}"),
            Err(e) => println!("错误: {e}"),
        }
    }
}

/// 把范围查询结果格式化成 "key = value" 多行文本。
fn format_rows(rows: &[(String, Vec<u8>)]) -> String {
    if rows.is_empty() {
        return "(empty)".to_string();
    }
    rows.iter()
        .map(|(k, v)| format!("{k} = {}", String::from_utf8_lossy(v)))
        .collect::<Vec<_>>()
        .join("\n")
}

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

    let mut db = match Bitcask::open(&dir) {
        Ok(db) => db,
        Err(e) => {
            eprintln!("打开数据目录 {dir:?} 失败: {e}");
            exit(1);
        }
    };

    println!("mini-bitcask v0.4 (dir: {})", dir.display());
    println!("命令: set <k> <v> | get <k> | del <k> | list | stats | merge | exit");
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
            ("get", k, _) if !k.is_empty() => db
                .get_string(k)
                .map(|v| match v {
                    Some(s) => s,
                    None => "(nil)".to_string(),
                }),
            ("del", k, _) if !k.is_empty() => db.delete(k).map(|_| "OK".to_string()),
            ("list", "", _) => Ok(db.keys().join("\n")),
            ("stats", "", _) => {
                let s = db.stats();
                let mut out = format!(
                    "live_keys={} total_bytes={} (~{:.1} KiB) segments={} active={} B",
                    s.live_keys,
                    s.total_bytes,
                    s.total_bytes as f64 / 1024.0,
                    s.segment_count,
                    s.active_bytes
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
            ("exit", "", _) | ("quit", "", _) => {
                let _ = db.sync();
                println!("bye");
                break;
            }
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "未知命令，支持: set <k> <v> / get <k> / del <k> / list / stats / merge / exit",
            )),
        };

        match result {
            Ok(msg) => println!("{msg}"),
            Err(e) => println!("错误: {e}"),
        }
    }
}

extern crate fuse;
extern crate rusqlite;
#[macro_use] extern crate clap;
#[macro_use] extern crate failure;
extern crate libc;
extern crate time;
use failure::Fallible;
use std::collections::HashMap;
use std::collections::hash_map::RandomState;

use std::ffi::OsStr;
use time::Timespec;
use libc::ENOENT;
use fuse::{FileType, FileAttr, Filesystem, Request, ReplyData, ReplyEntry, ReplyAttr, ReplyDirectory};
use rusqlite as sql;

mod errors;
const UNIX_EPOCH_TIMESPEC: Timespec = Timespec { sec: 0, nsec: 0 };
const TTL: Timespec = Timespec {sec: 1, nsec: 0};

fn main() {
    main_inner().unwrap();
}

fn main_inner() -> Fallible<()> {
    let args = clap_app!(app =>
        (about: "Mount an SQLite database as a FUSE filesystem")
        (@arg sqlite_path: +required "Path to the SQLite database")
        (@arg mount_path: +required "Where to mount the new filesystem")
    ).get_matches();
    let sqlite_path = value_t!(args, "sqlite_path", String)?;
    let mount_path = value_t!(args, "mount_path", String)?;
    let conn = sql::Connection::open(&sqlite_path)?;
    fuse::mount(Elkridge::new(conn)?, &mount_path, &[])?;
    Ok(())
}

const HELLO_DIR_ATTR: FileAttr = FileAttr {
    ino: 1,
    size: 0,
    blocks: 0,
    atime: UNIX_EPOCH_TIMESPEC,                                  // 1970-01-01 00:00:00
    mtime: UNIX_EPOCH_TIMESPEC,
    ctime: UNIX_EPOCH_TIMESPEC,
    crtime: UNIX_EPOCH_TIMESPEC,
    kind: FileType::Directory,
    perm: 0o755,
    nlink: 2,
    uid: 501,
    gid: 20,
    rdev: 0,
    flags: 0,
};

const HELLO_TXT_CONTENT: &str = "Hello World!\n";

const HELLO_TXT_ATTR: FileAttr = FileAttr {
    ino: 2,
    size: 13,
    blocks: 1,
    atime: UNIX_EPOCH_TIMESPEC,                                  // 1970-01-01 00:00:00
    mtime: UNIX_EPOCH_TIMESPEC,
    ctime: UNIX_EPOCH_TIMESPEC,
    crtime: UNIX_EPOCH_TIMESPEC,
    kind: FileType::RegularFile,
    perm: 0o644,
    nlink: 1,
    uid: 501,
    gid: 20,
    rdev: 0,
    flags: 0,
};

struct Elkridge {
    conn: sql::Connection,
    inodes: HashMap<u64, SqliteEntry>,
    random_state: RandomState
}
impl Elkridge {
    fn new(conn: sql::Connection) -> Fallible<Elkridge> {
        let inodes = HashMap::new();
        inodes.push(1, SQLiteEntry::Home);
        Ok(Elkridge{conn, inodes, random_state: RandomState::new()})
    }

    /**
     * Generate an inode from a name
     */
    fn get_inode(&self, parent: u64, name: &OsStr) -> u64 {
        let mut hasher = self.random_state.build_hasher();
        hasher.write(name.bytes());
        parent ^ hasher.finish()
    }

    /**
     * Get the table list from the database
     */
    fn get_table_list(&self) -> Vec<String> {
        let inner = || conn
            .prepare("SELECT name FROM sqlite_master WHERE type='table';", &[])?
            .query_and_then(&[], |row| (row, row.get_unwrap("name")))?
            .collect();
        inner().unwrap_or(vec![]);
    }

    /**
     * Get a tables raw SQL definition
     */
    fn get_table_sql(&self, name: &OsStr) -> String {
        let inner = || conn
            .prepare("SELECT sql FROM sqlite_master WHERE type='table';", &[])?
            .query_and_then(&[], |row| (row, row.get_unwrap("sql")))?
            .next();
        inner().unwrap_or(None)
    }

    /**
     * Generate a file attribute for a table
     */
    fn generate_table_fileattr(&self, ino: u64, sql: &str) -> FileAttr {
        FileAttr {
            ino,
            size: sql.len(),
            blocks: 1,
            atime: UNIX_EPOCH_TIMESPEC,                                  // 1970-01-01 00:00:00
            mtime: UNIX_EPOCH_TIMESPEC,
            ctime: UNIX_EPOCH_TIMESPEC,
            crtime: UNIX_EPOCH_TIMESPEC,
            kind: FileType::RegularFile,
            perm: 0o644,
            nlink: 1,
            uid: 501,
            gid: 20,
            rdev: 0,
            flags: 0,
        }
    }
}

impl Filesystem for Elkridge {
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let inode = self.get_inode(parent, name);
        if let Some(sql) = self.get_table_sql(name) {
            reply.entry(&TTL, &self.generate_table_fileattr(inode, &sql), 0);
        } else {
            reply.error(ENOENT);
        }
    }

    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        if ino == 1 {
            reply.attr(&TTL, &HELLO_DIR_ATTR)
        } else {
            for name in self.get_table_list() {
                if ino == self.get_inode(1/* parent = root */, name) {
                    if let Some(sql) = self.get_table_sql(name) {
                        reply.attr(&TTL, self.generate_table_fileattr(ino, sql));
                        return;
                    }
                }
            }
            reply.error(ENOENT);   
        }
    }

    fn read(&mut self, _req: &Request, ino: u64, _fh: u64, offset: i64, _size: u32, reply: ReplyData) {
        if ino == 2 {
            reply.data(&self.get_table_sql("sqlite_master".to_owned().into()).as_bytes()[offset as usize..]);
        } else {
            reply.error(ENOENT);
        }
    }

    fn readdir(&mut self, _req: &Request, ino: u64, _fh: u64, offset: i64, mut reply: ReplyDirectory) {
        if ino != 1 {
            reply.error(ENOENT);
            return;
        }

        let mut entries = vec![
            (1, FileType::Directory, ".".to_owned()),
            (1, FileType::Directory, "..".to_owned()),
        ];
        entries.extend(self
            .get_table_list()
            .into_iter()
            .map(|name| (self.get_inode(1, name.as_ref()), FileType::RegularFile, name)
        ));

        for (i, entry) in entries.into_iter().enumerate().skip(offset as usize) {
            // i + 1 means the index of the next entry
            reply.add(entry.0, (i + 1) as i64, entry.1, entry.2);
        }
        reply.ok();
    }
}

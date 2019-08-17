extern crate fuse;
extern crate rusqlite;
#[macro_use] extern crate clap;
#[macro_use] extern crate failure;
extern crate libc;
extern crate time;
use failure::Fallible;

use std::ffi::OsStr;
use time::Timespec;
use libc::ENOENT;
use fuse::{FileType, FileAttr, Filesystem, Request, ReplyData, ReplyEntry, ReplyAttr, ReplyDirectory};
use rusqlite as sql;
use basic::BasicFilesystem;

mod errors;
mod basic;
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

struct Elkridge {
    conn: sql::Connection
}
impl Elkridge {
    fn new(conn: sql::Connection) -> Fallible<Elkridge> {
        // Check that the connection is sane
        conn.execute_batch("
        CREATE TABLE IF NOT EXISTS Inode(
            inode   INTEGER PRIMARY KEY,
            size    INTEGER NOT NULL DEFAULT 0 CHECK ( size >= 0 ) ,
            blocks  INTEGER NOT NULL DEFAULT 0 CHECK ( blocks >= 0 ),
            atime   INTEGER NOT NULL DEFAULT ( strftime('%s') ),
            mtime   INTEGER NOT NULL DEFAULT ( strftime('%s') ),
            ctime   INTEGER NOT NULL DEFAULT ( strftime('%s') ),
            crtime  INTEGER NOT NULL DEFAULT ( strftime('%s') ),
                --- Kind may not be immediately clear, so:
                -- 0: NamedPipe,
                -- 1: CharDevice,
                -- 2: BlockDevice,
                -- 3: Directory,
                -- 4: RegularFile, -- the default
                -- 5: Symlink,
                -- 6: Socket,
            kind    INTEGER NOT NULL DEFAULT 4, 
            perm    INTEGER NOT NULL DEFAULT 420, -- in decimal; aka rwxr--r-- aka 0644
            uid     INTEGER NOT NULL DEFAULT 0, -- root
            gid     INTEGER NOT NULL DEFAULT 0, -- root
            rdev    INTEGER NOT NULL DEFAULT 0,
            flags   INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS Path(
            inode   INTEGER NOT NULL UNIQUE REFERENCES Inode(inode) ON DELETE CASCADE ON UPDATE CASCADE,
            parent  INTEGER NOT NULL REFERENCES Inode(inode) ON DELETE RESTRICT ON UPDATE CASCADE,
            name    TEXT NOT NULL CHECK ( length(name) > 0 ),
            PRIMARY KEY (parent, name)
        );
        CREATE TABLE IF NOT EXISTS Page(
            inode   INTEGER NOT NULL REFERENCES Inode(inode) ON DELETE CASCADE ON UPDATE CASCADE,
            start  INTEGER NOT NULL DEFAULT 0 CHECK (start >= 0),
            finish INTEGER NOT NULL DEFAULT 0 CHECK (finish >= 0),
            content BLOB NOT NULL
        );
        CREATE INDEX IF NOT EXISTS Page__inode ON Page(inode);
        -- Create a root node
        INSERT OR IGNORE INTO Inode(inode, kind) VALUES (0, 3);
        -- Create a root path
        INSERT OR IGNORE INTO Path(inode, parent, name) VALUES (0, 0, '');
        ")?;
        Ok(Elkridge{conn})
    }

    /// Generate a file attribute for a table
    fn generate_fileattr_from_row(&self, row: &sql::Row) -> sql::Result<FileAttr> {
        Ok(FileAttr {
            // These three are fussy because technically we are straing an unsigned int as a signed int in sqlite
            // It's a no-op to convert between them and it's lossless but beware this when using the sqlite tables
            // Negative inodes, sizes, and blocks are possible for this reason, which is why we have the CHECKs in the DDL
            // Removing the CHECKs will still work with this code but may confuse you
            ino:    row.get::<&str, i64>("inode")? as u64,
            size:   row.get::<&str, i64>("size")? as u64,
            blocks: row.get::<&str, i64>("blocks")? as u64,
            atime:  Timespec::new(row.get("atime")?, 0),
            mtime:  Timespec::new(row.get("mtime")?, 0),
            ctime:  Timespec::new(row.get("ctime")?, 0),
            crtime: Timespec::new(row.get("crtime")?, 0),
            kind:   match row.get("kind")? {
                // Convert codes back to enum
                0 => FileType::NamedPipe,
                1 => FileType::CharDevice,
                2 => FileType::BlockDevice,
                3 => FileType::Directory,
                4 => FileType::RegularFile,
                5 => FileType::Symlink,
                6 => FileType::Socket,
                _ => FileType::RegularFile
            },
            perm:   row.get("perm")?,
            nlink:  row.get("nlink")?,
            uid:    row.get("uid")?,
            gid:    row.get("gid")?,
            rdev:   0, // Not sure about these, for safety let's leave these alone
            flags:  0, // Not sure about these, for safety let's leave these alone
        })
    }

    /// Convert a file type from its corresponding code
    /// (remember that the inverse can be done trivially using "as")
    fn filetype_from_code(code: i8) -> FileType {
        match code {
            // Convert codes back to enum
            0 => FileType::NamedPipe,
            1 => FileType::CharDevice,
            2 => FileType::BlockDevice,
            3 => FileType::Directory,
            4 => FileType::RegularFile,
            5 => FileType::Symlink,
            6 => FileType::Socket,
            _ => FileType::RegularFile
        }
    }
}

impl Filesystem for Elkridge {
    /// Search for an inode by parent and name (e.g. using the path)
    fn lookup(&mut self, req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        match self.lookup_basic(req, parent, name) {
            Ok(res) => reply.entry(&TTL, &res, 0),
            Err(e) => {
                println!("Error: Failed to find {} {:?}.", name.to_str().unwrap_or("[Invalid name]"), e);
                reply.error(ENOENT);
            }
        }
    }

    /// Directly retrieve the info for an inode
    fn getattr(&mut self, req: &Request, ino: u64, reply: ReplyAttr) {
        match self.getattr_basic(req, ino) {
            Ok(res) => reply.attr(&TTL, &res),
            Err(e) => {
                println!("Error: Failed to find inode {} {:?}.", ino, e);
                reply.error(ENOENT);
            }
        }
    }

    /// Read some data from a page
    fn read(&mut self, req: &Request, ino: u64, fh: u64, offset: i64, size: u32, reply: ReplyData) {
        match self.read_basic(req, ino, fh, offset, size) {
            Ok(buf) => reply.data(&buf),
            Err(e) => {
                println!("Error: Performing read on ino:{} {:?}.", ino, e);
                reply.error(ENOENT);
            }
        }
    }

    /// Get the list of children in a directory
    fn readdir(&mut self, req: &Request, ino: u64, fh: u64, offset: i64, mut reply: ReplyDirectory) {
        match self.readdir_basic(req, ino, fh, offset) {
            Ok(entries) => {
                for entry in entries {
                    reply.add(entry.ino, entry.offset, entry.kind, &entry.name);
                }
                reply.ok()
            },
            Err(e) => {
                println!("Error: Performing readdir on ino:{} {:?}.", ino, e);
                reply.error(ENOENT);
            }
        }
    }
}

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
            inode   INTEGER PRIMARY KEY NOT NULL REFERENCES Inode(inode) ON DELETE CASCADE ON UPDATE CASCADE,
            parent  INTEGER NOT NULL REFERENCES Inode(inode) ON DELETE RESTRICT ON UPDATE CASCADE,
            name    TEXT NOT NULL CHECK ( length(name) > 0 )
        );
        CREATE INDEX IF NOT EXISTS Path__parent ON Path(parent);
        CREATE TABLE IF NOT EXISTS Page(
            inode   INTEGER NOT NULL REFERENCES Inode(inode) ON DELETE CASCADE ON UPDATE CASCADE,
            start  INTEGER NOT NULL DEFAULT 0 CHECK (offset >= 0),
            finish INTEGER NOT NULL DEFAULT 0 CHECK (offset >= 0),
            content BLOB NOT NULL
        );
        CREATE INDEX IF NOT EXISTS Page__inode ON Page(inode);
        -- Create a root node
        INSERT OR IGNORE INTO Inode(inode, kind) VALUES (0, 3);
        -- Create a root path
        INSERT OR IGNORE INTO Path(inode, parent, name, kind) VALUES (0, 0, '/');
        ")?;
        Ok(Elkridge{conn})
    }

    /**
     * Generate a file attribute for a table
     */
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
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let search_result = self.conn.query_row(
            "SELECT *,
                (SELECT count(*) FROM Path WHERE Path.inode = Inode.inode) AS nlink
            FROM Inode
            WHERE parent = ? AND name = ?",
            &[
                &(parent as i64) as &sql::ToSql,
                &name.to_str().unwrap_or("")
            ],
            |row| self.generate_fileattr_from_row(row)
        );
        match search_result {
            Ok(res) => reply.entry(&TTL, &res, 0),
            Err(e) => {
                println!("Error: Failed to find {} {:?}.", name.to_str().unwrap_or("[Invalid name]"), e);
                reply.error(ENOENT);
            }
        }
    }

    /// Directly retrieve the info for an inode
    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        let search_result = self.conn.query_row(
            "SELECT *,
                (SELECT count(*) FROM Path WHERE Path.inode = Inode.inode) AS nlink
            FROM Inode
            WHERE inode = ?",
            &[
                &(ino as i64) as &sql::ToSql,
            ],
            |row| self.generate_fileattr_from_row(row)
        );
        match search_result {
            Ok(res) => reply.attr(&TTL, &res),
            Err(e) => {
                println!("Error: Failed to find inode {} {:?}.", ino, e);
                reply.error(ENOENT);
            }
        }
    }

    /// Read some data from a page
    fn read(&mut self, _req: &Request, ino: u64, _fh: u64, offset: i64, size: u32, reply: ReplyData) {
        // Wrap so we can use ?
        let inner : sql::Result<Vec<u8>> = (|| {
            let mut stmt = self.conn.prepare(
                "SELECT content, offset_byte
                FROM Page
                WHERE inode = ?
                    AND start <= ?
                    AND finish >= ?
                ORDER BY start")?;
            let mut buf : Vec<u8> = Vec::with_capacity(size as usize * 4/3);
            stmt.query_and_then(
                &[
                    &(ino as i64),
                    &offset,
                    &(offset + size as i64)
                ],
                // TODO: The type annotations here seem ugly
                |row| Ok(buf.extend_from_slice(&row.get::<&str, Vec<u8>>("content")?)) as sql::Result<()>
            )?;
            Ok(buf)
        })();
        
        match inner {
            Ok(buf) => reply.data(&buf),
            Err(e) => {
                println!("Error: Performing read on ino:{} {:?}.", ino, e);
                reply.error(ENOENT);
            }
        }
    }

    /// Get the list of children in a directory
    fn readdir(&mut self, _req: &Request, ino: u64, _fh: u64, offset: i64, mut reply: ReplyDirectory) {
        // Wrap so we can use ?
        let inner : sql::Result<()> = (|| {
            let mut stmt = self.conn.prepare(
                "SELECT inode, name, kind
                FROM Path
                NATURAL JOIN Inode
                WHERE Path.parent = ?")?;
            stmt.query_and_then(
                &[ &(ino as i64) ],
                // TODO: The type annotations here seem ugly
                |row| Ok(reply.add(
                    row.get::<&str, i64>("inode")? as u64,              // ino
                    0,                                                  // offset
                    Elkridge::filetype_from_code(row.get("inode")?),    // kind
                    row.get::<&str, String>("name")?                                    // name
                    )) as sql::Result<bool>
            )?;
            Ok(())
        })();
        
        match inner {
            Ok(_) => reply.ok(),
            Err(e) => {
                println!("Error: Performing readdir on ino:{} {:?}.", ino, e);
                reply.error(ENOENT);
            }
        }
    }
}

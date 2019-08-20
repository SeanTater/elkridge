use failure::Fallible;
use Elkridge;
use fuse::{FileType, FileAttr, Request};
use rusqlite as sql;
use std::ffi::{OsStr, OsString};

/// Implementation of Filesystem, returning Fallible responses instead of using reply objects
/// 
/// The major advantage of this is just the use of Try.
pub trait BasicFilesystem {
    fn lookup_basic(&mut self, req: &Request, parent: u64, name: &OsStr) -> Fallible<FileAttr>;
    fn getattr_basic(&mut self, req: &Request, ino: u64) -> Fallible<FileAttr>;
    fn read_basic(&mut self, req: &Request, ino: u64, _fh: u64, offset: i64, size: u32) -> Fallible<Vec<u8>>;
    fn readdir_basic(&mut self, req: &Request, ino: u64, _fh: u64, _offset: i64) -> Fallible<Vec<DirectoryEntry>>;
    fn mkdir_basic(
        &mut self, 
        req: &Request, 
        parent: u64, 
        name: &OsStr, 
        mode: u32
    ) -> Fallible<FileAttr>;
    fn rmdir_basic(
        &mut self, 
        req: &Request, 
        parent: u64, 
        name: &OsStr
    ) -> Fallible<()>;
}

impl BasicFilesystem for Elkridge {
    /// Search for an inode by parent and name (e.g. using the path)
    fn lookup_basic(&mut self, _req: &Request, parent: u64, name: &OsStr) -> Fallible<FileAttr> {
        Ok(self.conn.query_row(
            "SELECT *,
                (SELECT count(*) FROM Path WHERE Path.inode = Inode.inode) AS nlink
            FROM Inode
            WHERE parent = ? AND name = ?",
            &[
                &(parent as i64) as &dyn sql::ToSql,
                &name.to_str().unwrap_or("")
            ],
            |row| self.generate_fileattr_from_row(row)
        )?)
    }

    /// Directly retrieve the info for an inode
    fn getattr_basic(&mut self, _req: &Request, ino: u64) -> Fallible<FileAttr> {
        Ok(self.conn.query_row(
            "SELECT *,
                (SELECT count(*) FROM Path WHERE Path.inode = Inode.inode) AS nlink
            FROM Inode
            WHERE inode = ?",
            &[
                &(ino as i64) as &dyn sql::ToSql,
            ],
            |row| self.generate_fileattr_from_row(row)
        )?)
    }

    /// Read some data from a page
    fn read_basic(&mut self, _req: &Request, ino: u64, _fh: u64, offset: i64, size: u32) -> Fallible<Vec<u8>> {
        // Wrap so we can use ?
        let mut stmt = self.conn.prepare(
            "SELECT content, start
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
    }

    /// Get the list of children in a directory
    fn readdir_basic(&mut self, _req: &Request, ino: u64, _fh: u64, _offset: i64) -> Fallible<Vec<DirectoryEntry>> {
        // Wrap so we can use ?
        let mut stmt = self.conn.prepare(
            "SELECT inode, name, kind
            FROM Path
            NATURAL JOIN Inode
            WHERE Path.parent = ?")?;
        let entries = stmt.query_map(
            &[ &(ino as i64) ],
            // TODO: The type annotations here seem ugly
            |row| Ok(DirectoryEntry{
                ino: row.get::<&str, i64>("inode")? as u64,              // ino
                offset: 0,                                               // offset
                kind: Elkridge::filetype_from_code(row.get("inode")?),   // kind
                name: row.get::<&str, String>("name")?.into()            // name
            })
        )?.collect::<sql::Result<_>>()?;
        Ok(entries)
    }
    fn mkdir_basic(
        &mut self, 
        req: &Request, 
        parent: u64, 
        name: &OsStr, 
        mode: u32
    ) -> Fallible<FileAttr> {
        let txn : sql::Transaction = self.conn.transaction()?;
        let maybe_inode = txn.query_row(
            "SELECT inode FROM Path
                WHERE parent = ? AND name = ?; ",
            &[ &(parent as i64) as &dyn sql::ToSql, &name.to_string_lossy() ],
            |row| row.get::<&str, i64>("is_there"))
            .ok();
        let definitely_inode = match maybe_inode {
            Some(ino) => ino,
            None => {
                txn.execute(
                    "INSERT OR IGNORE INTO Inode(perm) VALUES (?);",
                    &[mode])?;
                let new_inode = txn.last_insert_rowid();
                txn.execute(
                    "INSERT OR IGNORE INTO Path(inode, parent, name) VALUES (?,?,?);",
                    &[
                        &new_inode,
                        &(parent as i64) as &dyn sql::ToSql,
                        &name.to_string_lossy()
                    ])?;
                new_inode
            }
        };
        txn.commit()?;
        self.getattr_basic(req, definitely_inode as u64)
    }
    fn rmdir_basic(
        &mut self, 
        _req: &Request, 
        parent: u64, 
        name: &OsStr
    ) -> Fallible<()> {
        self.conn.execute("DELETE FROM Path WHERE parent=? AND name = ?;",
            &[
                &(parent as i64) as &dyn sql::ToSql,
                &name.to_string_lossy()
            ])?;
        Ok(())
    }
}

/// Directory Entry, used as part of the return type of readdir()
pub struct DirectoryEntry {
    pub ino: u64,
    pub offset: i64,
    pub kind: FileType,
    pub name: OsString
}
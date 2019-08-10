use rusqlite as sql;

#[derive(Debug, Fail)]
enum Error {
    #[fail(display = "SQLite error: {}", err)]
    SQLError{err: sql::Error}
}
impl From<rusqlite::Error> for Error {
    fn from(err: sql::Error) -> Self {
        Error::SQLError{err}
    }
}

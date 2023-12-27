use crate::config::DatabaseConfig;

use super::{new_postgres, new_sqlite};

pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    Int64(i64),
    Float64(f64),
    String(String),
    Bytes(Vec<u8>),
}
pub struct Row {
    values: Vec<Value>,
}

impl Row {
    pub fn new(values: Vec<Value>) -> Self {
        Self { values }
    }

    pub fn values(&self) -> &[Value] {
        &self.values
    }

    pub fn into_values(self) -> Vec<Value> {
        self.values
    }
}

#[async_trait::async_trait]
pub trait RowsBackend<'a>: Send + Sync {
    fn columns(&self) -> &[String];

    async fn next(&mut self) -> Option<Result<Row>>;
}

pub struct Rows<'a> {
    inner: Box<dyn RowsBackend<'a> + 'a>,
}

impl<'a> Rows<'a> {
    pub fn new<T: RowsBackend<'a> + 'a>(rows: T) -> Self {
        let inner = Box::new(rows);
        Self { inner }
    }
}

impl<'a> Rows<'a> {
    pub fn columns(&self) -> &[String] {
        self.inner.columns()
    }

    pub async fn next(&mut self) -> Option<Result<Row>> {
        self.inner.next().await
    }
}

#[async_trait::async_trait]
pub trait TransactionBackend<'a>: Send + Sync {
    async fn commit(self: Box<Self>) -> Result<()>;

    async fn rollback(self: Box<Self>) -> Result<()>;

    async fn execute(&mut self, statement: &str) -> Result<()>;

    async fn query(&mut self, statement: &str) -> Result<Rows>;
}

pub struct Transaction<'a> {
    inner: Box<dyn TransactionBackend<'a> + 'a>,
}

impl<'a> Transaction<'a> {
    pub fn new<T: TransactionBackend<'a> + 'a>(tx: T) -> Self {
        let inner = Box::new(tx);
        Self { inner }
    }

    pub async fn commit(self) -> Result<()> {
        self.inner.commit().await
    }

    pub async fn rollback(self) -> Result<()> {
        self.inner.rollback().await
    }

    pub async fn execute(&mut self, statement: &str) -> Result<()> {
        self.inner.execute(statement).await
    }

    pub async fn query(&mut self, statement: &str) -> Result<Rows> {
        self.inner.query(statement).await
    }
}

#[derive(Clone, Copy, Default)]
pub enum IsolationLevel {
    ReadUncommitted,
    #[default]
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

#[derive(Clone, Copy, Default)]
pub struct TransactionOptions {
    pub isolation_level: IsolationLevel,
    pub read_only: bool,
}

#[async_trait::async_trait]
pub trait ConnectionBackend: Send + Sync {
    async fn transaction(&mut self, options: TransactionOptions) -> Result<Transaction>;

    async fn execute(&mut self, statement: &str) -> Result<()>;

    async fn query(&mut self, statement: &str) -> Result<Rows>;
}

pub struct Connection {
    inner: Box<dyn ConnectionBackend>,
}

impl Connection {
    pub fn new<T: ConnectionBackend + 'static>(conn: T) -> Self {
        let inner = Box::new(conn);
        Self { inner }
    }
}

impl Connection {
    pub async fn transaction<'a>(
        &'a mut self,
        options: TransactionOptions,
    ) -> Result<Transaction<'a>> {
        self.inner.transaction(options).await
    }

    pub async fn execute(&mut self, statement: &str) -> Result<()> {
        self.inner.execute(statement).await
    }

    pub async fn query(&mut self, statement: &str) -> Result<Rows> {
        self.inner.query(statement).await
    }
}

#[derive(Clone, Copy, Default)]
pub struct ConnectionOptions {
    pub read_only: bool,
}

#[async_trait::async_trait]
pub trait DatabaseBackend: Send + Sync {
    async fn connection(&self, options: ConnectionOptions) -> Result<Connection>;

    fn clone_database(&self) -> Database;
}

struct OwnedTransaction {
    conn: *mut (dyn ConnectionBackend),
    tx: Option<Box<dyn TransactionBackend<'static>>>,
}

impl Drop for OwnedTransaction {
    fn drop(&mut self) {
        drop(self.tx.take());
        drop(unsafe { Box::from_raw(self.conn) });
    }
}

unsafe impl Send for OwnedTransaction {}

unsafe impl Sync for OwnedTransaction {}

#[async_trait::async_trait]
impl<'a> TransactionBackend<'a> for OwnedTransaction {
    async fn commit(mut self: Box<Self>) -> Result<()> {
        self.tx.take().unwrap().commit().await
    }

    async fn rollback(mut self: Box<Self>) -> Result<()> {
        self.tx.take().unwrap().rollback().await
    }

    async fn execute(&mut self, statement: &str) -> Result<()> {
        self.tx.as_mut().unwrap().execute(statement).await
    }

    async fn query(&mut self, statement: &str) -> Result<Rows> {
        self.tx.as_mut().unwrap().query(statement).await
    }
}

struct OwnedRows {
    conn: *mut (dyn ConnectionBackend),
    rows: Option<Box<dyn RowsBackend<'static>>>,
}

impl Drop for OwnedRows {
    fn drop(&mut self) {
        drop(self.rows.take());
        drop(unsafe { Box::from_raw(self.conn) });
    }
}

unsafe impl Send for OwnedRows {}

unsafe impl Sync for OwnedRows {}

#[async_trait::async_trait]
impl<'a> RowsBackend<'a> for OwnedRows {
    fn columns(&self) -> &[String] {
        self.rows.as_ref().unwrap().columns()
    }

    async fn next(&mut self) -> Option<Result<Row>> {
        self.rows.as_mut().unwrap().next().await
    }
}

pub struct Database {
    inner: Box<dyn DatabaseBackend>,
}

impl Database {
    pub fn new<T: DatabaseBackend + 'static>(db: T) -> Self {
        let inner = Box::new(db);
        Self { inner }
    }

    pub async fn connection(&self, options: ConnectionOptions) -> Result<Connection> {
        self.inner.connection(options).await
    }

    pub async fn transaction(&self, options: TransactionOptions) -> Result<Transaction> {
        let conn_options = ConnectionOptions {
            read_only: options.read_only,
        };
        let conn = self.connection(conn_options).await?;
        let conn = Box::leak(conn.inner);
        let mut tx = OwnedTransaction { conn, tx: None };
        tx.tx = Some(conn.transaction(options).await?.inner);
        Ok(Transaction::new(tx))
    }

    pub async fn execute(&self, statement: &str) -> Result<()> {
        let mut conn = self.connection(Default::default()).await?;
        conn.execute(statement).await
    }

    pub async fn query(&self, statement: &str) -> Result<Rows> {
        let conn = self.connection(Default::default()).await?;
        let conn = Box::leak(conn.inner);
        let mut rows = OwnedRows { conn, rows: None };
        rows.rows = Some(conn.query(statement).await?.inner);
        Ok(Rows::new(rows))
    }
}

impl Clone for Database {
    fn clone(&self) -> Database {
        self.inner.clone_database()
    }
}

#[async_trait::async_trait]
pub trait Executor<'a> {
    async fn execute(&mut self, statement: &str) -> Result<()>;

    async fn query(&mut self, statement: &str) -> Result<Rows>;
}

#[async_trait::async_trait]
impl<'a> Executor<'a> for Transaction<'a> {
    async fn execute(&mut self, statement: &str) -> Result<()> {
        Transaction::execute(self, statement).await
    }

    async fn query(&mut self, statement: &str) -> Result<Rows> {
        Transaction::query(self, statement).await
    }
}

#[async_trait::async_trait]
impl<'a> Executor<'a> for Connection {
    async fn execute(&mut self, statement: &str) -> Result<()> {
        Connection::execute(self, statement).await
    }

    async fn query(&mut self, statement: &str) -> Result<Rows> {
        Connection::query(self, statement).await
    }
}

#[async_trait::async_trait]
impl<'a> Executor<'a> for Database {
    async fn execute(&mut self, statement: &str) -> Result<()> {
        Database::execute(self, statement).await
    }

    async fn query(&mut self, statement: &str) -> Result<Rows> {
        Database::query(self, statement).await
    }
}

pub fn new_database(config: &DatabaseConfig) -> Result<Database> {
    let db = match config {
        DatabaseConfig::SQLite(config) => new_sqlite(config)?,
        DatabaseConfig::Postgres(config) => new_postgres(config)?,
    };
    Ok(db)
}

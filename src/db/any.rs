use crate::config::DatabaseConfig;

use super::{
    new_postgres, new_sqlite, Connection, Database, Error, Executor, Row, Rows, Transaction,
};

#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Null,
    Bool,
    Int64(i64),
    Float64(f64),
    String(String),
    Bytes(Vec<u8>),
}

pub struct AnyRow {
    values: Vec<Value>,
}

impl AnyRow {
    pub fn new(values: Vec<Value>) -> Self {
        Self { values }
    }
}

impl Row for AnyRow {
    type Value = Value;

    fn values(&self) -> &[Value] {
        &self.values
    }
}

#[async_trait::async_trait]
pub trait AnyRowsBackend<'a>: Send + Sync {
    fn columns(&self) -> &[String];

    async fn next(&mut self) -> Option<Result<AnyRow, Error>>;
}

pub struct AnyRows<'a> {
    inner: Box<dyn AnyRowsBackend<'a> + 'a>,
}

impl<'a> AnyRows<'a> {
    pub fn new<T: AnyRowsBackend<'a> + 'a>(rows: T) -> Self {
        let inner = Box::new(rows);
        Self { inner }
    }
}

#[async_trait::async_trait]
impl<'a> Rows<'a> for AnyRows<'a> {
    type Row = AnyRow;

    fn columns(&self) -> &[String] {
        self.inner.columns()
    }

    async fn next(&mut self) -> Option<Result<Self::Row, Error>> {
        self.inner.next().await
    }
}

#[async_trait::async_trait]
pub trait AnyTransactionBackend<'a>: Send + Sync {
    async fn commit(self: Box<Self>) -> Result<(), Error>;

    async fn rollback(self: Box<Self>) -> Result<(), Error>;

    async fn execute(&mut self, statement: &str) -> Result<(), Error>;

    async fn query(&mut self, statement: &str) -> Result<AnyRows<'_>, Error>;
}

pub struct AnyTransaction<'a> {
    inner: Box<dyn AnyTransactionBackend<'a> + 'a>,
}

impl<'a> AnyTransaction<'a> {
    pub fn new<T: AnyTransactionBackend<'a> + 'a>(tx: T) -> Self {
        let inner = Box::new(tx);
        Self { inner }
    }
}

#[async_trait::async_trait]
impl<'a> Executor<'a> for AnyTransaction<'a> {
    type Rows<'b> = AnyRows<'b>
    where
        Self: 'b;

    async fn execute(&mut self, statement: &str) -> Result<(), Error> {
        self.inner.execute(statement).await
    }

    async fn query(&mut self, statement: &str) -> Result<Self::Rows<'_>, Error> {
        self.inner.query(statement).await
    }
}

#[async_trait::async_trait]
impl<'a> Transaction<'a> for AnyTransaction<'a> {
    async fn commit(self) -> Result<(), Error> {
        self.inner.commit().await
    }

    async fn rollback(self) -> Result<(), Error> {
        self.inner.rollback().await
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
pub trait AnyConnectionBackend: Send + Sync {
    async fn transaction(
        &mut self,
        options: TransactionOptions,
    ) -> Result<AnyTransaction<'_>, Error>;

    async fn execute(&mut self, statement: &str) -> Result<(), Error>;

    async fn query(&mut self, statement: &str) -> Result<AnyRows<'_>, Error>;
}

pub struct AnyConnection {
    inner: Box<dyn AnyConnectionBackend>,
}

impl AnyConnection {
    pub fn new<T: AnyConnectionBackend + 'static>(conn: T) -> Self {
        let inner = Box::new(conn);
        Self { inner }
    }
}

#[async_trait::async_trait]
impl<'a> Executor<'a> for AnyConnection {
    type Rows<'b> = AnyRows<'b>
    where
        Self: 'b;

    async fn execute(&mut self, statement: &str) -> Result<(), Error> {
        self.inner.execute(statement).await
    }

    async fn query(&mut self, statement: &str) -> Result<Self::Rows<'_>, Error> {
        self.inner.query(statement).await
    }
}

#[async_trait::async_trait]
impl Connection for AnyConnection {
    type Transaction<'a> = AnyTransaction<'a>;

    type TransactionOptions = TransactionOptions;

    async fn transaction<'a>(
        &'a mut self,
        options: Self::TransactionOptions,
    ) -> Result<Self::Transaction<'a>, Error> {
        self.inner.transaction(options).await
    }
}

#[derive(Clone, Copy, Default)]
pub struct ConnectionOptions {
    pub read_only: bool,
}

#[async_trait::async_trait]
pub trait AnyDatabaseBackend: Send + Sync {
    async fn connection(&self, options: ConnectionOptions) -> Result<AnyConnection, Error>;

    fn clone(&self) -> AnyDatabase;
}

struct OwnedTransaction {
    conn: *mut (dyn AnyConnectionBackend),
    tx: Option<Box<dyn AnyTransactionBackend<'static>>>,
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
impl<'a> AnyTransactionBackend<'a> for OwnedTransaction {
    async fn commit(mut self: Box<Self>) -> Result<(), Error> {
        self.tx.take().unwrap().commit().await
    }

    async fn rollback(mut self: Box<Self>) -> Result<(), Error> {
        self.tx.take().unwrap().rollback().await
    }

    async fn execute(&mut self, statement: &str) -> Result<(), Error> {
        self.tx.as_mut().unwrap().execute(statement).await
    }

    async fn query(&mut self, statement: &str) -> Result<AnyRows, Error> {
        self.tx.as_mut().unwrap().query(statement).await
    }
}

struct OwnedRows {
    conn: *mut (dyn AnyConnectionBackend),
    rows: Option<Box<dyn AnyRowsBackend<'static>>>,
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
impl<'a> AnyRowsBackend<'a> for OwnedRows {
    fn columns(&self) -> &[String] {
        self.rows.as_ref().unwrap().columns()
    }

    async fn next(&mut self) -> Option<Result<AnyRow, Error>> {
        self.rows.as_mut().unwrap().next().await
    }
}

pub struct AnyDatabase {
    inner: Box<dyn AnyDatabaseBackend>,
}

impl AnyDatabase {
    pub fn new<T: AnyDatabaseBackend + 'static>(db: T) -> Self {
        let inner = Box::new(db);
        Self { inner }
    }

    pub async fn transaction(&self, options: TransactionOptions) -> Result<AnyTransaction, Error> {
        let conn_options = ConnectionOptions {
            read_only: options.read_only,
        };
        let conn = self.connection(conn_options).await?;
        let conn = Box::leak(conn.inner);
        let mut tx = OwnedTransaction { conn, tx: None };
        tx.tx = Some(conn.transaction(options).await?.inner);
        Ok(AnyTransaction::new(tx))
    }

    pub async fn execute(&self, statement: &str) -> Result<(), Error> {
        let mut conn = self.connection(Default::default()).await?;
        conn.execute(statement).await
    }

    pub async fn query(&self, statement: &str) -> Result<AnyRows, Error> {
        let conn = self.connection(Default::default()).await?;
        let conn = Box::leak(conn.inner);
        let mut rows = OwnedRows { conn, rows: None };
        rows.rows = Some(conn.query(statement).await?.inner);
        Ok(AnyRows::new(rows))
    }
}

impl Clone for AnyDatabase {
    fn clone(&self) -> AnyDatabase {
        self.inner.clone()
    }
}

#[async_trait::async_trait]
impl Database for AnyDatabase {
    type Connection = AnyConnection;

    type ConnectionOptions = ConnectionOptions;

    async fn connection(
        &self,
        options: Self::ConnectionOptions,
    ) -> Result<Self::Connection, Error> {
        self.inner.connection(options).await
    }
}

pub async fn new_database(config: &DatabaseConfig) -> Result<AnyDatabase, Error> {
    let db = match config {
        DatabaseConfig::SQLite(config) => AnyDatabase::new(new_sqlite(config).await?),
        DatabaseConfig::Postgres(config) => AnyDatabase::new(new_postgres(config).await?),
    };
    Ok(db)
}

// Auto implement any backends.

#[async_trait::async_trait]
impl<'a, T, R> AnyRowsBackend<'a> for T
where
    T: Rows<'a, Row = R> + Send + Sync,
    R: Into<AnyRow>,
{
    fn columns(&self) -> &[String] {
        Rows::columns(self)
    }

    async fn next(&mut self) -> Option<Result<AnyRow, Error>> {
        Rows::next(self).await.map(|r| r.map(|v| v.into()))
    }
}

// #[async_trait::async_trait]
// impl<'a, T, R> AnyTransactionBackend<'a> for T
// where
//     for<'b> T: Transaction<'a, Rows<'b> = R> + Send + Sync + 'b,
//     for<'b> R: AnyRowsBackend<'b> + 'b,
// {
//     async fn commit(self: Box<Self>) -> Result<(), Error> {
//         Transaction::commit(*self).await
//     }

//     async fn rollback(self: Box<Self>) -> Result<(), Error> {
//         Transaction::rollback(*self).await
//     }

//     async fn execute(&mut self, statement: &str) -> Result<(), Error> {
//         Executor::execute(self, statement).await
//     }

//     async fn query(&mut self, statement: &str) -> Result<AnyRows<'_>, Error> {
//         let rows = Executor::query(self, statement).await?;
//         Ok(AnyRows::new(rows))
//     }
// }

// #[async_trait::async_trait]
// impl<C, T, R> AnyConnectionBackend for C
// where
//     for<'a, 'b> C: Connection<Transaction<'a> = T, Rows<'b> = R> + Send + Sync + 'a,
//     for<'a, 'b> T: AnyTransactionBackend<'a> + 'a,
//     for<'b> R: AnyRowsBackend<'b> + 'b,
// {
//     async fn transaction(
//         &mut self,
//         options: TransactionOptions,
//     ) -> Result<AnyTransaction<'_>, Error> {
//         let tx = Connection::transaction(self, options).await?;
//         Ok(AnyTransaction::new(tx))
//     }

//     async fn execute(&mut self, statement: &str) -> Result<(), Error> {
//         Executor::execute(self, statement).await
//     }

//     async fn query(&mut self, statement: &str) -> Result<AnyRows<'_>, Error> {
//         let rows = Executor::query(self, statement).await?;
//         Ok(AnyRows::new(rows))
//     }
// }

// #[async_trait::async_trait]
// impl<D, C, T, R> AnyDatabaseBackend for D
// where
//     for<'a> D: Database<Connection = C> + Send + Sync + Clone + 'a,
//     for<'a, 'b> C: Connection<Transaction<'a> = T, Rows<'b> = R> + Send + Sync + 'a,
//     for<'a, 'b> T: AnyTransactionBackend<'a> + 'a,
//     for<'b> R: AnyRowsBackend<'b> + 'b,
// {
//     async fn connection(&self, options: ConnectionOptions) -> Result<AnyConnection, Error> {
//         let conn = Database::connection(self, options).await?;
//         Ok(AnyConnection::new(conn))
//     }

//     fn clone(&self) -> AnyDatabase {
//         AnyDatabase::new(Clone::clone(self))
//     }
// }

use crate::config::DatabaseConfig;

use super::{
    new_postgres, new_sqlite, Connection, ConnectionOptions, Database, Error, Executor, Row, Rows,
    Transaction, TransactionOptions,
};

#[async_trait::async_trait]
pub trait AnyDatabaseBackend: Send + Sync {
    async fn connection(&self, options: ConnectionOptions) -> Result<AnyConnection, Error>;

    fn clone(&self) -> AnyDatabase;
}

#[async_trait::async_trait]
pub trait AnyConnectionBackend: Send + Sync {
    async fn transaction<'a>(
        &'a mut self,
        options: TransactionOptions,
    ) -> Result<AnyTransaction<'a>, Error>;

    async fn execute(&mut self, statement: &str) -> Result<(), Error>;

    async fn query<'a>(&'a mut self, statement: &str) -> Result<AnyRows<'a>, Error>;
}

#[async_trait::async_trait]
pub trait AnyTransactionBackend<'a>: Send + Sync {
    async fn commit(self: Box<Self>) -> Result<(), Error>;

    async fn rollback(self: Box<Self>) -> Result<(), Error>;

    async fn execute(&mut self, statement: &str) -> Result<(), Error>;

    async fn query<'b>(&'b mut self, statement: &str) -> Result<AnyRows<'b>, Error>;
}

// #[async_trait::async_trait]
// impl<'a, T, R> AnyTransactionBackend<'a> for T
// where
//     for<'b> T: Transaction<'a, Rows<'b> = R> + Send + Sync + 'b,
//     for<'b> R: Rows<'b> + AnyRowsBackend<'b> + 'b,
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

//     async fn query<'b>(&'b mut self, statement: &str) -> Result<AnyRows<'b>, Error> {
//         let rows = Executor::query(self, statement).await?;
//         Ok(AnyRows::new(rows))
//     }
// }

#[async_trait::async_trait]
pub trait AnyRowsBackend<'a>: Send + Sync {
    fn columns(&self) -> &[String];

    async fn next(&mut self) -> Option<Result<AnyRow, Error>>;
}

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

pub struct AnyDatabase {
    inner: Box<dyn AnyDatabaseBackend>,
}

impl AnyDatabase {
    pub fn new<T: AnyDatabaseBackend + 'static>(db: T) -> Self {
        let inner = Box::new(db);
        Self { inner }
    }
}

#[async_trait::async_trait]
impl Database for AnyDatabase {
    type Connection = AnyConnection;

    async fn connection(&self, options: ConnectionOptions) -> Result<Self::Connection, Error> {
        self.inner.connection(options).await
    }
}

impl Clone for AnyDatabase {
    fn clone(&self) -> AnyDatabase {
        self.inner.clone()
    }
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

    async fn query<'b>(&'b mut self, statement: &str) -> Result<Self::Rows<'b>, Error> {
        self.inner.query(statement).await
    }
}

#[async_trait::async_trait]
impl Connection for AnyConnection {
    type Transaction<'a> = AnyTransaction<'a>;

    async fn transaction<'a>(
        &'a mut self,
        options: TransactionOptions,
    ) -> Result<Self::Transaction<'a>, Error> {
        self.inner.transaction(options).await
    }
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
impl<'a> Transaction<'a> for AnyTransaction<'a> {
    async fn commit(self) -> Result<(), Error> {
        self.inner.commit().await
    }

    async fn rollback(self) -> Result<(), Error> {
        self.inner.rollback().await
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

    async fn query<'b>(&'b mut self, statement: &str) -> Result<Self::Rows<'b>, Error> {
        self.inner.query(statement).await
    }
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

pub enum Value {
    Null,
    Bool,
    I64(i64),
    F64(f64),
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

pub async fn new_database(config: &DatabaseConfig) -> Result<AnyDatabase, Error> {
    let db = match config {
        DatabaseConfig::SQLite(config) => AnyDatabase::new(new_sqlite(config).await?),
        DatabaseConfig::Postgres(config) => AnyDatabase::new(new_postgres(config).await?),
    };
    Ok(db)
}

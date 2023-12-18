use futures_core::Stream;

use crate::config::DatabaseConfig;

use super::{
    new_postgres, new_sqlite, Connection, ConnectionOptions, Database, Error, Executor, Rows,
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
}

#[async_trait::async_trait]
pub trait AnyTransactionBackend<'a>: Send + Sync {
    async fn execute(&mut self, statement: &str) -> Result<(), Error>;

    async fn query<'b>(&'b mut self, statement: &str) -> Result<AnyRows<'b>, Error>;

    async fn commit(self: Box<Self>) -> Result<(), Error>;

    async fn rollback(self: Box<Self>) -> Result<(), Error>;
}

#[async_trait::async_trait]
pub trait AnyRowsBackend<'a>: futures_core::Stream<Item = Result<(), Error>> + Send + Sync {}

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
impl Executor<'_> for AnyConnection {
    type Rows<'b> = AnyRows<'b>
    where
        Self: 'b;

    async fn execute(&mut self, statement: &str) -> Result<(), Error> {
        todo!()
    }

    async fn query<'r>(&'r mut self, statement: &str) -> Result<Self::Rows<'r>, Error> {
        todo!()
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
impl Transaction<'_> for AnyTransaction<'_> {
    async fn commit(self) -> Result<(), Error> {
        self.inner.commit().await
    }

    async fn rollback(self) -> Result<(), Error> {
        self.inner.rollback().await
    }
}

#[async_trait::async_trait]
impl Executor<'_> for AnyTransaction<'_> {
    type Rows<'b> = AnyRows<'b>
    where
        Self: 'b;

    async fn execute(&mut self, statement: &str) -> Result<(), Error> {
        self.inner.execute(statement).await
    }

    async fn query<'r>(&'r mut self, statement: &str) -> Result<Self::Rows<'r>, Error> {
        self.inner.query(statement).await
    }
}

pub struct AnyRows<'a> {
    inner: Box<dyn AnyRowsBackend<'a> + 'a>,
}

impl<'a> AnyRows<'a> {
    pub fn new<T: AnyRowsBackend<'a> + 'a>(conn: T) -> Self {
        let inner = Box::new(conn);
        Self { inner }
    }
}

impl Stream for AnyRows<'_> {
    type Item = Result<(), Error>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let inner = unsafe { std::pin::Pin::new_unchecked(self.inner.as_mut()) };
        inner.poll_next(cx)
    }
}

impl Rows<'_> for AnyRows<'_> {}

pub async fn new_database(config: &DatabaseConfig) -> Result<AnyDatabase, Error> {
    let db = match config {
        DatabaseConfig::SQLite(config) => AnyDatabase::new(new_sqlite(config).await?),
        DatabaseConfig::Postgres(config) => AnyDatabase::new(new_postgres(config).await?),
    };
    Ok(db)
}

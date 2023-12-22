pub type Error = Box<dyn std::error::Error + Send + Sync>;

pub trait Row {
    type Value;

    fn values(&self) -> &[Self::Value];
}

#[async_trait::async_trait]
pub trait Rows<'a> {
    type Row: Row;

    fn columns(&self) -> &[String];

    async fn next(&mut self) -> Option<Result<Self::Row, Error>>;
}

#[async_trait::async_trait]
pub trait Executor<'a> {
    type Rows<'b>: Rows<'b>
    where
        Self: 'b;

    async fn execute(&mut self, statement: &str) -> Result<(), Error>;

    async fn query(&mut self, statement: &str) -> Result<Self::Rows<'_>, Error>;
}

#[async_trait::async_trait]
pub trait Transaction<'a>: Executor<'a> {
    async fn commit(self) -> Result<(), Error>;

    async fn rollback(self) -> Result<(), Error>;
}

#[async_trait::async_trait]
pub trait Connection: for<'a> Executor<'a> {
    type Transaction<'a>: Transaction<'a>
    where
        Self: 'a;

    type TransactionOptions;

    async fn transaction(
        &mut self,
        options: Self::TransactionOptions,
    ) -> Result<Self::Transaction<'_>, Error>;
}

#[async_trait::async_trait]
pub trait Database {
    type Connection: Connection;

    type ConnectionOptions;

    async fn connection(&self, options: Self::ConnectionOptions)
        -> Result<Self::Connection, Error>;
}

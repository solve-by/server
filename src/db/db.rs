pub type Error = Box<dyn std::error::Error + Send + Sync>;

#[derive(Default)]
pub struct ConnectionOptions {
    pub read_only: bool,
}

#[async_trait::async_trait]
pub trait Database: Send + Sync {
    type Connection: Connection;

    async fn connection(&self, options: ConnectionOptions) -> Result<Self::Connection, Error>;
}

#[derive(Default)]
pub enum IsolationLevel {
    ReadUncommitted,
    #[default]
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

#[derive(Default)]
pub struct TransactionOptions {
    pub isolation: IsolationLevel,
    pub read_only: bool,
}

#[async_trait::async_trait]
pub trait Connection: Executor<'static> + Send + Sync {
    type Transaction<'a>: Transaction<'a>
    where
        Self: 'a;

    async fn transaction<'a>(
        &'a mut self,
        options: TransactionOptions,
    ) -> Result<Self::Transaction<'a>, Error>;
}

#[async_trait::async_trait]
pub trait Transaction<'a>: Executor<'a> {
    async fn commit(self) -> Result<(), Error>;

    async fn rollback(self) -> Result<(), Error>;
}

#[async_trait::async_trait]
pub trait Executor<'a> {
    type Rows<'b>: Rows<'b>
    where
        Self: 'b;

    async fn execute(&mut self, statement: &str) -> Result<(), Error>;

    async fn query<'b>(&'b mut self, statement: &str) -> Result<Self::Rows<'b>, Error>;
}

pub trait Rows<'a>: futures_core::Stream<Item = ()> {}

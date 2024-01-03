use std::marker::PhantomData;

use tokio::sync::oneshot;

use crate::config::SQLiteConfig;

use super::connection::{ConnectionHandle, ConnectionTask};
use super::query::QueryHandle;
use super::transaction::TransactionHandle;

pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;

pub type Value = deadpool_sqlite::rusqlite::types::Value;

#[derive(Clone, Debug)]
pub struct Row {
    pub(super) values: Vec<Value>,
}

impl Row {
    pub fn values(&self) -> &[Value] {
        &self.values
    }

    pub fn into_values(self) -> Vec<Value> {
        self.values
    }
}

pub struct Rows<'a> {
    handle: QueryHandle,
    _phantom: PhantomData<&'a ()>,
}

impl<'a> Rows<'a> {
    pub fn columns(&self) -> &[String] {
        self.handle.columns()
    }

    pub async fn next(&mut self) -> Option<Result<Row>> {
        self.handle.next().await
    }
}

impl<'a> Drop for Rows<'a> {
    fn drop(&mut self) {}
}

pub struct Transaction<'a> {
    tx: TransactionHandle,
    _phantom: PhantomData<&'a ()>,
}

impl<'a> Transaction<'a> {
    pub async fn commit(mut self) -> Result<()> {
        self.tx.commit().await
    }

    pub async fn rollback(mut self) -> Result<()> {
        self.tx.rollback().await
    }

    pub async fn execute(&mut self, statement: &str, arguments: &[Value]) -> Result<()> {
        self.tx
            .execute(statement.to_owned(), arguments.to_owned())
            .await
    }

    pub async fn query(&mut self, statement: &str, arguments: &[Value]) -> Result<Rows> {
        let handle = self
            .tx
            .query(statement.to_owned(), arguments.to_owned())
            .await?;
        Ok(Rows {
            handle,
            _phantom: PhantomData,
        })
    }
}

impl<'a> Drop for Transaction<'a> {
    fn drop(&mut self) {}
}

pub struct Connection {
    tx: Option<ConnectionHandle>,
    handle: Option<tokio::task::JoinHandle<()>>,
}

impl Connection {
    pub async fn transaction(&mut self) -> Result<Transaction> {
        let tx = self.tx.as_mut().unwrap().transaction().await?;
        Ok(Transaction {
            tx,
            _phantom: PhantomData,
        })
    }

    pub async fn execute<S, A>(&mut self, statement: S, arguments: A) -> Result<()>
    where
        S: Into<String>,
        A: Into<Vec<Value>>,
    {
        self.tx
            .as_mut()
            .unwrap()
            .execute(statement.into(), arguments.into())
            .await
    }

    pub async fn query<S, A>(&mut self, statement: S, arguments: A) -> Result<Rows>
    where
        S: Into<String>,
        A: Into<Vec<Value>>,
    {
        let handle = self
            .tx
            .as_mut()
            .unwrap()
            .query(statement.into(), arguments.into())
            .await?;
        Ok(Rows {
            handle,
            _phantom: PhantomData,
        })
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        drop(self.tx.take());
        if let Some(handle) = self.handle.take() {
            let _ = futures::executor::block_on(handle).unwrap();
        };
    }
}

#[derive(Clone)]
pub struct Database(deadpool_sqlite::Pool);

impl Database {
    pub fn new(config: &SQLiteConfig) -> Result<Self> {
        let config = deadpool_sqlite::Config::new(config.path.to_owned());
        let pool = config.create_pool(deadpool_sqlite::Runtime::Tokio1)?;
        Ok(Self(pool))
    }

    pub async fn connection(&self) -> Result<Connection> {
        let conn = self.0.get().await?;
        let task = ConnectionTask::new(conn);
        let (tx, rx) = oneshot::channel();
        let handle = tokio::task::spawn_blocking(move || task.blocking_run(tx));
        Ok(Connection {
            tx: Some(rx.await??),
            handle: Some(handle),
        })
    }
}

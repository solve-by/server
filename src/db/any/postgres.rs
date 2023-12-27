use crate::{config::PostgresConfig, db::postgres};

use super::{
    Connection, ConnectionBackend, ConnectionOptions, Database, DatabaseBackend, Result, Row, Rows,
    RowsBackend, Transaction, TransactionBackend, TransactionOptions, Value,
};

struct WrapRows<'a>(postgres::Rows<'a>);

#[async_trait::async_trait]
impl<'a> RowsBackend<'a> for WrapRows<'a> {
    fn columns(&self) -> &[String] {
        self.0.columns()
    }

    async fn next(&mut self) -> Option<Result<Row>> {
        let map_value = |v| match v {
            postgres::Value::Null => Value::Null,
            postgres::Value::Bool(v) => Value::Bool(v),
            postgres::Value::Int16(v) => Value::Int64(v.into()),
            postgres::Value::Int32(v) => Value::Int64(v.into()),
            postgres::Value::Int64(v) => Value::Int64(v),
            postgres::Value::Float32(v) => Value::Float64(v.into()),
            postgres::Value::Float64(v) => Value::Float64(v),
            postgres::Value::String(v) => Value::String(v),
            postgres::Value::Bytes(v) => Value::Bytes(v),
        };
        Some(
            self.0
                .next()
                .await?
                .map(|r| Row::new(r.into_values().into_iter().map(map_value).collect())),
        )
    }
}

struct WrapTransaction<'a>(postgres::Transaction<'a>);

#[async_trait::async_trait]
impl<'a> TransactionBackend<'a> for WrapTransaction<'a> {
    async fn commit(self: Box<Self>) -> Result<()> {
        self.0.commit().await
    }

    async fn rollback(self: Box<Self>) -> Result<()> {
        self.0.rollback().await
    }

    async fn execute(&mut self, statement: &str) -> Result<()> {
        self.0.execute(statement).await
    }

    async fn query(&mut self, statement: &str) -> Result<Rows> {
        let rows = self.0.query(statement).await?;
        Ok(Rows::new(WrapRows(rows)))
    }
}

struct WrapConnection(postgres::Connection);

#[async_trait::async_trait]
impl ConnectionBackend for WrapConnection {
    async fn transaction(&mut self, _options: TransactionOptions) -> Result<Transaction> {
        let tx = self.0.transaction(Default::default()).await?;
        Ok(Transaction::new(WrapTransaction(tx)))
    }

    async fn execute(&mut self, statement: &str) -> Result<()> {
        self.0.execute(statement).await
    }

    async fn query(&mut self, statement: &str) -> Result<Rows> {
        let rows = self.0.query(statement).await?;
        Ok(Rows::new(WrapRows(rows)))
    }
}

struct WrapDatabase(postgres::Database);

#[async_trait::async_trait]
impl DatabaseBackend for WrapDatabase {
    async fn connection(&self, _options: ConnectionOptions) -> Result<Connection> {
        let conn = self.0.connection(Default::default()).await?;
        Ok(Connection::new(WrapConnection(conn)))
    }
}

pub fn new_postgres(config: &PostgresConfig) -> Result<Database> {
    let db = postgres::Database::new(config)?;
    Ok(Database::new(WrapDatabase(db)))
}

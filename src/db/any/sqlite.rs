use crate::{config::SQLiteConfig, db::sqlite};

use super::{
    Connection, ConnectionBackend, ConnectionOptions, Database, DatabaseBackend, Result, Row, Rows,
    RowsBackend, Transaction, TransactionBackend, TransactionOptions, Value,
};

struct WrapRows<'a>(sqlite::Rows<'a>);

#[async_trait::async_trait]
impl<'a> RowsBackend<'a> for WrapRows<'a> {
    fn columns(&self) -> &[String] {
        self.0.columns()
    }

    async fn next(&mut self) -> Option<Result<Row>> {
        let map_value = |v| match v {
            sqlite::Value::Null => Value::Null,
            sqlite::Value::Integer(v) => Value::Int64(v),
            sqlite::Value::Real(v) => Value::Float64(v),
            sqlite::Value::Text(v) => Value::String(v),
            sqlite::Value::Blob(v) => Value::Bytes(v),
        };
        Some(
            self.0
                .next()
                .await?
                .map(|r| Row::new(r.into_values().into_iter().map(map_value).collect())),
        )
    }
}

struct WrapTransaction<'a>(sqlite::Transaction<'a>);

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

struct WrapConnection(sqlite::Connection);

#[async_trait::async_trait]
impl ConnectionBackend for WrapConnection {
    async fn transaction(&mut self, _options: TransactionOptions) -> Result<Transaction> {
        let tx = self.0.transaction().await?;
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

struct WrapDatabase(sqlite::Database);

#[async_trait::async_trait]
impl DatabaseBackend for WrapDatabase {
    async fn connection(&self, _options: ConnectionOptions) -> Result<Connection> {
        let conn = self.0.connection().await?;
        Ok(Connection::new(WrapConnection(conn)))
    }
}

pub fn new_sqlite(config: &SQLiteConfig) -> Result<Database> {
    let db = sqlite::Database::new(config)?;
    Ok(Database::new(WrapDatabase(db)))
}

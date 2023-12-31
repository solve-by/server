use std::collections::HashMap;
use std::sync::Arc;

use crate::db::postgres;

use super::sqlite::WrapQueryBuilder;
use super::{
    Connection, ConnectionBackend, ConnectionOptions, Database, DatabaseBackend, QueryBuilder,
    Result, Row, Rows, RowsBackend, Transaction, TransactionBackend, TransactionOptions, Value,
};

impl From<postgres::Value> for Value {
    fn from(value: postgres::Value) -> Self {
        match value {
            postgres::Value::Null => Value::Null,
            postgres::Value::Bool(v) => Value::Bool(v),
            postgres::Value::Int16(v) => Value::Int64(v.into()),
            postgres::Value::Int32(v) => Value::Int64(v.into()),
            postgres::Value::Int64(v) => Value::Int64(v),
            postgres::Value::Float32(v) => Value::Float64(v.into()),
            postgres::Value::Float64(v) => Value::Float64(v),
            postgres::Value::String(v) => Value::String(v),
            postgres::Value::Bytes(v) => Value::Bytes(v),
        }
    }
}

struct WrapRows<'a>(postgres::Rows<'a>, Arc<HashMap<String, usize>>);

#[async_trait::async_trait]
impl<'a> RowsBackend<'a> for WrapRows<'a> {
    fn columns(&self) -> &[String] {
        self.0.columns()
    }

    async fn next(&mut self) -> Option<Result<Row>> {
        Some(self.0.next().await?.map(|r| {
            Row::new(
                r.into_values().into_iter().map(|v| v.into()).collect(),
                self.1.clone(),
            )
        }))
    }
}

struct WrapTransaction<'a>(postgres::Transaction<'a>);

#[async_trait::async_trait]
impl<'a> TransactionBackend<'a> for WrapTransaction<'a> {
    fn builder(&self) -> QueryBuilder {
        QueryBuilder::new(WrapQueryBuilder::default())
    }

    async fn commit(self: Box<Self>) -> Result<()> {
        self.0.commit().await
    }

    async fn rollback(self: Box<Self>) -> Result<()> {
        self.0.rollback().await
    }

    async fn execute(&mut self, query: &str, values: &[Value]) -> Result<()> {
        self.0.execute(query).await
    }

    async fn query(&mut self, query: &str, values: &[Value]) -> Result<Rows> {
        let rows = self.0.query(query).await?;
        let mut columns = HashMap::with_capacity(rows.columns().len());
        for i in 0..rows.columns().len() {
            columns.insert(rows.columns()[i].clone(), i);
        }
        Ok(Rows::new(WrapRows(rows, Arc::new(columns))))
    }
}

struct WrapConnection(postgres::Connection);

#[async_trait::async_trait]
impl ConnectionBackend for WrapConnection {
    fn builder(&self) -> QueryBuilder {
        QueryBuilder::new(WrapQueryBuilder::default())
    }

    async fn transaction(&mut self, _options: TransactionOptions) -> Result<Transaction> {
        let tx = self.0.transaction(Default::default()).await?;
        Ok(Transaction::new(WrapTransaction(tx)))
    }

    async fn execute(&mut self, query: &str, values: &[Value]) -> Result<()> {
        self.0.execute(query).await
    }

    async fn query(&mut self, query: &str, values: &[Value]) -> Result<Rows> {
        let rows = self.0.query(query).await?;
        let mut columns = HashMap::with_capacity(rows.columns().len());
        for i in 0..rows.columns().len() {
            columns.insert(rows.columns()[i].clone(), i);
        }
        Ok(Rows::new(WrapRows(rows, Arc::new(columns))))
    }
}

struct WrapDatabase(postgres::Database);

#[async_trait::async_trait]
impl DatabaseBackend for WrapDatabase {
    fn builder(&self) -> QueryBuilder {
        QueryBuilder::new(WrapQueryBuilder::default())
    }

    async fn connection(&self, _options: ConnectionOptions) -> Result<Connection> {
        let conn = self.0.connection(Default::default()).await?;
        Ok(Connection::new(WrapConnection(conn)))
    }
}

impl Into<Database> for postgres::Database {
    fn into(self) -> Database {
        Database::new(WrapDatabase(self))
    }
}

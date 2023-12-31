use std::collections::HashMap;
use std::sync::Arc;

use crate::db::sqlite;

use super::{
    Connection, ConnectionBackend, ConnectionOptions, Database, DatabaseBackend, QueryBuilder,
    QueryBuilderBackend, Result, Row, Rows, RowsBackend, Transaction, TransactionBackend,
    TransactionOptions, Value,
};

impl From<Value> for sqlite::Value {
    fn from(value: Value) -> Self {
        match value {
            Value::Null => sqlite::Value::Null,
            Value::Bool(v) => sqlite::Value::Integer(v.into()),
            Value::Int64(v) => sqlite::Value::Integer(v),
            Value::Float64(v) => sqlite::Value::Real(v),
            Value::String(v) => sqlite::Value::Text(v),
            Value::Bytes(v) => sqlite::Value::Blob(v),
        }
    }
}

impl From<sqlite::Value> for Value {
    fn from(value: sqlite::Value) -> Self {
        match value {
            sqlite::Value::Null => Value::Null,
            sqlite::Value::Integer(v) => Value::Int64(v),
            sqlite::Value::Real(v) => Value::Float64(v),
            sqlite::Value::Text(v) => Value::String(v),
            sqlite::Value::Blob(v) => Value::Bytes(v),
        }
    }
}

struct WrapRows<'a>(sqlite::Rows<'a>, Arc<HashMap<String, usize>>);

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

struct WrapTransaction<'a>(sqlite::Transaction<'a>);

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
        let values: Vec<_> = values.iter().cloned().map(|v| v.into()).collect();
        self.0.execute(query, &values).await
    }

    async fn query(&mut self, query: &str, values: &[Value]) -> Result<Rows> {
        let values: Vec<_> = values.iter().cloned().map(|v| v.into()).collect();
        let rows = self.0.query(query, &values).await?;
        let mut columns = HashMap::with_capacity(rows.columns().len());
        for i in 0..rows.columns().len() {
            columns.insert(rows.columns()[i].clone(), i);
        }
        Ok(Rows::new(WrapRows(rows, Arc::new(columns))))
    }
}

struct WrapConnection(sqlite::Connection);

#[async_trait::async_trait]
impl ConnectionBackend for WrapConnection {
    fn builder(&self) -> QueryBuilder {
        QueryBuilder::new(WrapQueryBuilder::default())
    }

    async fn transaction(&mut self, _options: TransactionOptions) -> Result<Transaction> {
        let tx = self.0.transaction().await?;
        Ok(Transaction::new(WrapTransaction(tx)))
    }

    async fn execute(&mut self, query: &str, values: &[Value]) -> Result<()> {
        let values: Vec<_> = values.iter().cloned().map(|v| v.into()).collect();
        self.0.execute(&query, &values).await
    }

    async fn query(&mut self, query: &str, values: &[Value]) -> Result<Rows> {
        let values: Vec<_> = values.iter().cloned().map(|v| v.into()).collect();
        let rows = self.0.query(&query, &values).await?;
        let mut columns = HashMap::with_capacity(rows.columns().len());
        for i in 0..rows.columns().len() {
            columns.insert(rows.columns()[i].clone(), i);
        }
        Ok(Rows::new(WrapRows(rows, Arc::new(columns))))
    }
}

#[derive(Default)]
pub(super) struct WrapQueryBuilder {
    query: String,
    values: Vec<Value>,
}

impl QueryBuilderBackend for WrapQueryBuilder {
    fn push(&mut self, ch: char) {
        self.query.push(ch);
    }

    fn push_str(&mut self, part: &str) {
        self.query.push_str(part);
    }

    fn push_name(&mut self, name: &str) {
        assert!(name.find(|c| c == '"' || c == '\\').is_none());
        self.push('"');
        self.push_str(name);
        self.push('"');
    }

    fn push_value(&mut self, value: Value) {
        self.values.push(value);
        self.push_str(format!("${}", self.values.len()).as_str())
    }

    fn build(&self) -> (&str, &[Value]) {
        (&self.query, &self.values)
    }
}

struct WrapDatabase(sqlite::Database);

#[async_trait::async_trait]
impl DatabaseBackend for WrapDatabase {
    fn builder(&self) -> QueryBuilder {
        QueryBuilder::new(WrapQueryBuilder::default())
    }

    async fn connection(&self, _options: ConnectionOptions) -> Result<Connection> {
        let conn = self.0.connection().await?;
        Ok(Connection::new(WrapConnection(conn)))
    }
}

impl Into<Database> for sqlite::Database {
    fn into(self) -> Database {
        Database::new(WrapDatabase(self))
    }
}

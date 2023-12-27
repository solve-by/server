use std::str::FromStr;
use std::{marker::PhantomData, pin::Pin};

use deadpool_postgres::tokio_postgres::types::{to_sql_checked, FromSql, IsNull, ToSql, Type};
use futures::StreamExt;

use crate::config::PostgresConfig;

pub type Error = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug)]
pub enum Value {
    Null,
    Bool(bool),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    String(String),
    Bytes(Vec<u8>),
}

impl<'a> FromSql<'a> for Value {
    fn from_sql(ty: &Type, raw: &'a [u8]) -> Result<Self, Error> {
        match *ty {
            Type::BOOL => Ok(Value::Bool(FromSql::from_sql(ty, raw)?)),
            Type::INT2 => Ok(Value::Int16(FromSql::from_sql(ty, raw)?)),
            Type::INT4 => Ok(Value::Int32(FromSql::from_sql(ty, raw)?)),
            Type::INT8 => Ok(Value::Int64(FromSql::from_sql(ty, raw)?)),
            Type::FLOAT4 => Ok(Value::Float32(FromSql::from_sql(ty, raw)?)),
            Type::FLOAT8 => Ok(Value::Float64(FromSql::from_sql(ty, raw)?)),
            Type::VARCHAR => Ok(Value::String(FromSql::from_sql(ty, raw)?)),
            Type::BYTEA => Ok(Value::Bytes(FromSql::from_sql(ty, raw)?)),
            _ => unreachable!(),
        }
    }

    fn from_sql_null(_ty: &Type) -> Result<Self, Error> {
        Ok(Value::Null)
    }

    fn accepts(ty: &Type) -> bool {
        match *ty {
            Type::BOOL
            | Type::INT2
            | Type::INT4
            | Type::INT8
            | Type::FLOAT4
            | Type::FLOAT8
            | Type::VARCHAR
            | Type::BYTEA => true,
            _ => false,
        }
    }
}

impl ToSql for Value {
    fn to_sql(&self, ty: &Type, out: &mut tokio_util::bytes::BytesMut) -> Result<IsNull, Error> {
        match self {
            Value::Null => Ok(IsNull::Yes),
            Value::Bool(v) => ToSql::to_sql(v, ty, out),
            Value::Int16(v) => ToSql::to_sql(v, ty, out),
            Value::Int32(v) => ToSql::to_sql(v, ty, out),
            Value::Int64(v) => ToSql::to_sql(v, ty, out),
            Value::Float32(v) => ToSql::to_sql(v, ty, out),
            Value::Float64(v) => ToSql::to_sql(v, ty, out),
            Value::String(v) => ToSql::to_sql(v, ty, out),
            Value::Bytes(v) => ToSql::to_sql(v, ty, out),
        }
    }

    fn accepts(ty: &Type) -> bool {
        match *ty {
            Type::BOOL
            | Type::INT2
            | Type::INT4
            | Type::INT8
            | Type::FLOAT4
            | Type::FLOAT8
            | Type::VARCHAR
            | Type::BYTEA => true,
            _ => false,
        }
    }

    to_sql_checked!();
}

pub struct Row {
    values: Vec<Value>,
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
    columns: Vec<String>,
    rows: Pin<Box<deadpool_postgres::tokio_postgres::RowStream>>,
    _phantom: PhantomData<&'a ()>,
}

impl<'a> Rows<'a> {
    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    pub async fn next(&mut self) -> Option<Result<Row, Error>> {
        let map_row = |r: deadpool_postgres::tokio_postgres::Row| {
            let mut values = Vec::with_capacity(self.columns.len());
            for i in 0..self.columns.len() {
                values.push(r.get(i));
            }
            Row { values }
        };
        self.rows
            .next()
            .await
            .map(|r| r.map(map_row).or_else(|e| Err(e.into())))
    }
}

impl<'a> Drop for Rows<'a> {
    fn drop(&mut self) {}
}

pub struct Transaction<'a> {
    tx: Option<deadpool_postgres::Transaction<'a>>,
}

impl<'a> Transaction<'a> {
    pub async fn commit(mut self) -> Result<(), Error> {
        Ok(self.tx.take().unwrap().commit().await?)
    }

    pub async fn rollback(mut self) -> Result<(), Error> {
        Ok(self.tx.take().unwrap().rollback().await?)
    }

    pub async fn execute(&mut self, statement: &str) -> Result<(), Error> {
        self.tx
            .as_mut()
            .unwrap()
            .execute_raw(statement.into(), Vec::<i8>::new())
            .await?;
        Ok(())
    }

    pub async fn query(&mut self, statement: &str) -> Result<Rows, Error> {
        let tx = self.tx.as_mut().unwrap();
        let statement = deadpool_postgres::tokio_postgres::ToStatement::__convert(statement)
            .into_statement(tx.client())
            .await?;
        let columns: Vec<_> = statement
            .columns()
            .into_iter()
            .map(|c| c.name().to_owned())
            .collect();
        let rows = self
            .tx
            .as_mut()
            .unwrap()
            .query_raw(&statement, Vec::<i8>::new())
            .await?;
        Ok(Rows {
            columns,
            rows: Box::pin(rows),
            _phantom: PhantomData,
        })
    }
}

impl<'a> Drop for Transaction<'a> {
    fn drop(&mut self) {}
}

pub type IsolationLevel = deadpool_postgres::tokio_postgres::IsolationLevel;

#[derive(Clone, Copy)]
pub struct TransactionOptions {
    pub read_only: bool,
    pub isolation_level: IsolationLevel,
}

impl Default for TransactionOptions {
    fn default() -> Self {
        Self {
            read_only: Default::default(),
            isolation_level: IsolationLevel::ReadCommitted,
        }
    }
}

pub struct Connection {
    conn: deadpool_postgres::Client,
}

impl Connection {
    pub async fn transaction(&mut self, options: TransactionOptions) -> Result<Transaction, Error> {
        let tx_builder = self
            .conn
            .build_transaction()
            .read_only(options.read_only)
            .isolation_level(options.isolation_level);
        let tx = Some(tx_builder.start().await?);
        Ok(Transaction { tx })
    }

    pub async fn execute(&mut self, statement: &str) -> Result<(), Error> {
        self.conn
            .execute_raw(statement.into(), Vec::<i8>::new())
            .await?;
        Ok(())
    }

    pub async fn query(&mut self, statement: &str) -> Result<Rows, Error> {
        let statement = deadpool_postgres::tokio_postgres::ToStatement::__convert(statement)
            .into_statement(&self.conn)
            .await?;
        let columns: Vec<_> = statement
            .columns()
            .into_iter()
            .map(|c| c.name().to_owned())
            .collect();
        let rows = self.conn.query_raw(&statement, Vec::<i8>::new()).await?;
        Ok(Rows {
            columns,
            rows: Box::pin(rows),
            _phantom: PhantomData,
        })
    }
}

#[derive(Clone, Copy, Default)]
pub struct ConnectionOptions {
    pub read_only: bool,
}

#[derive(Clone)]
pub struct Database {
    read_only: deadpool_postgres::Pool,
    writable: deadpool_postgres::Pool,
}

impl Database {
    pub fn new(config: &PostgresConfig) -> Result<Self, Error> {
        let mut hosts = Vec::new();
        let mut ports = Vec::new();
        for host in &config.hosts {
            let parts: Vec<_> = host.rsplit(':').take(2).collect();
            if parts.len() != 2 {
                return Err(format!("invalid host format {}", host).into());
            }
            hosts.push(parts[0].to_owned());
            ports.push(u16::from_str(parts[1])?);
        }
        let mut pg_config = deadpool_postgres::Config {
            hosts: Some(hosts),
            ports: Some(ports),
            user: Some(config.user.to_owned()),
            password: Some(config.user.to_owned()),
            dbname: Some(config.name.to_owned()),
            target_session_attrs: Some(deadpool_postgres::TargetSessionAttrs::Any),
            ..Default::default()
        };
        let tls_config = rustls::ClientConfig::builder()
            .with_safe_defaults()
            .with_root_certificates(rustls::RootCertStore::empty())
            .with_no_client_auth();
        let runtime = Some(deadpool_postgres::Runtime::Tokio1);
        let tls = tokio_postgres_rustls::MakeRustlsConnect::new(tls_config);
        let read_only = pg_config.create_pool(runtime.clone(), tls.clone())?;
        pg_config.target_session_attrs = Some(deadpool_postgres::TargetSessionAttrs::ReadWrite);
        let writable = pg_config.create_pool(runtime.clone(), tls.clone())?;
        Ok(Self {
            read_only,
            writable,
        })
    }

    pub async fn connection(&self, options: ConnectionOptions) -> Result<Connection, Error> {
        let conn = if options.read_only {
            self.read_only.get().await
        } else {
            self.writable.get().await
        }?;
        Ok(Connection { conn })
    }
}

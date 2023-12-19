use std::pin::Pin;
use std::str::FromStr;

use futures::StreamExt;

use crate::config::PostgresConfig;

use super::{
    AnyConnection, AnyConnectionBackend, AnyDatabase, AnyDatabaseBackend, AnyRow, AnyRows,
    AnyTransaction, AnyTransactionBackend, Connection, ConnectionOptions, Database, Error,
    Executor, IsolationLevel, Rows, Transaction, TransactionOptions,
};

pub struct PostgresDatabase {
    read_only: deadpool_postgres::Pool,
    writable: deadpool_postgres::Pool,
}

impl PostgresDatabase {
    pub fn new() -> Self {
        todo!()
    }
}

#[async_trait::async_trait]
impl Database for PostgresDatabase {
    type Connection = PostgresConnection;

    async fn connection(&self, options: ConnectionOptions) -> Result<Self::Connection, Error> {
        let conn = if options.read_only {
            self.read_only.get().await
        } else {
            self.writable.get().await
        }?;
        Ok(PostgresConnection(conn))
    }
}

#[async_trait::async_trait]
impl AnyDatabaseBackend for PostgresDatabase {
    async fn connection(&self, options: ConnectionOptions) -> Result<AnyConnection, Error> {
        let conn = Database::connection(self, options).await?;
        Ok(AnyConnection::new(conn))
    }

    fn clone(&self) -> AnyDatabase {
        AnyDatabase::new(Self {
            read_only: self.read_only.clone(),
            writable: self.writable.clone(),
        })
    }
}

pub struct PostgresConnection(deadpool_postgres::Client);

#[async_trait::async_trait]
impl Executor<'_> for PostgresConnection {
    type Rows<'b> = PostgresRows
    where
        Self: 'b;

    async fn execute(&mut self, statement: &str) -> Result<(), Error> {
        self.0
            .execute_raw(statement.into(), Vec::<i8>::new())
            .await?;
        Ok(())
    }

    async fn query<'r>(&'r mut self, statement: &str) -> Result<Self::Rows<'r>, Error> {
        let rows = self.0.query_raw(statement.into(), Vec::<i8>::new()).await?;
        Ok(PostgresRows(Box::pin(rows)))
    }
}

#[async_trait::async_trait]
impl Connection for PostgresConnection {
    type Transaction<'a> = PostgresTransaction<'a>
    where
        Self: 'a;

    async fn transaction<'a>(
        &'a mut self,
        options: TransactionOptions,
    ) -> Result<Self::Transaction<'a>, Error> {
        use deadpool_postgres::tokio_postgres;
        let map_level = |v| match v {
            IsolationLevel::ReadUncommitted => tokio_postgres::IsolationLevel::ReadUncommitted,
            IsolationLevel::ReadCommitted => tokio_postgres::IsolationLevel::ReadCommitted,
            IsolationLevel::RepeatableRead => tokio_postgres::IsolationLevel::RepeatableRead,
            IsolationLevel::Serializable => tokio_postgres::IsolationLevel::Serializable,
        };
        let tx_builder = self
            .0
            .build_transaction()
            .read_only(options.read_only)
            .isolation_level(map_level(options.isolation_level));
        let tx = tx_builder.start().await?;
        Ok(PostgresTransaction(tx))
    }
}

#[async_trait::async_trait]
impl AnyConnectionBackend for PostgresConnection {
    async fn transaction<'a>(
        &'a mut self,
        options: TransactionOptions,
    ) -> Result<AnyTransaction<'a>, Error> {
        let tx = Connection::transaction(self, options).await?;
        Ok(AnyTransaction::new(tx))
    }

    async fn execute(&mut self, statement: &str) -> Result<(), Error> {
        Executor::execute(self, statement).await?;
        Ok(())
    }

    async fn query<'b>(&'b mut self, statement: &str) -> Result<AnyRows<'b>, Error> {
        let rows = Executor::query(self, statement).await?;
        Ok(AnyRows::new(rows))
    }
}

pub struct PostgresTransaction<'a>(deadpool_postgres::Transaction<'a>);

#[async_trait::async_trait]
impl Executor<'_> for PostgresTransaction<'_> {
    type Rows<'b> = PostgresRows
    where
        Self: 'b;

    async fn execute(&mut self, statement: &str) -> Result<(), Error> {
        self.0
            .execute_raw(statement.into(), Vec::<i8>::new())
            .await?;
        Ok(())
    }

    async fn query<'r>(&'r mut self, statement: &str) -> Result<Self::Rows<'r>, Error> {
        let rows: deadpool_postgres::tokio_postgres::RowStream =
            self.0.query_raw(statement.into(), Vec::<i8>::new()).await?;
        Ok(PostgresRows(Box::pin(rows)))
    }
}

#[async_trait::async_trait]
impl Transaction<'_> for PostgresTransaction<'_> {
    async fn commit(self) -> Result<(), Error> {
        Ok(self.0.commit().await?)
    }

    async fn rollback(self) -> Result<(), Error> {
        Ok(self.0.rollback().await?)
    }
}

#[async_trait::async_trait]
impl<'a> AnyTransactionBackend<'a> for PostgresTransaction<'a> {
    async fn commit(self: Box<Self>) -> Result<(), Error> {
        Ok(self.0.commit().await?)
    }

    async fn rollback(self: Box<Self>) -> Result<(), Error> {
        Ok(self.0.rollback().await?)
    }

    async fn execute(&mut self, statement: &str) -> Result<(), Error> {
        Executor::execute(self, statement).await?;
        Ok(())
    }

    async fn query<'b>(&'b mut self, statement: &str) -> Result<AnyRows<'b>, Error> {
        let rows = Executor::query(self, statement).await?;
        Ok(AnyRows::new(rows))
    }
}

pub struct PostgresRows(Pin<Box<deadpool_postgres::tokio_postgres::RowStream>>);

#[async_trait::async_trait]
impl Rows<'_> for PostgresRows {
    type Row = AnyRow;

    fn columns(&self) -> &[String] {
        &[]
    }

    async fn next(&mut self) -> Option<Result<Self::Row, Error>> {
        self.0.next().await.map(|_| Ok(AnyRow::new(Vec::new())))
    }
}

pub async fn new_postgres(config: &PostgresConfig) -> Result<PostgresDatabase, Error> {
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
    Ok(PostgresDatabase {
        read_only,
        writable,
    })
}

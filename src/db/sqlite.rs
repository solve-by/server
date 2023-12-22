use std::marker::PhantomData;

use deadpool_sqlite::SyncGuard;
use tokio::sync::{mpsc, oneshot};

use crate::config::SQLiteConfig;

use super::{
    AnyConnection, AnyConnectionBackend, AnyDatabase, AnyDatabaseBackend, AnyRow, AnyRows,
    AnyTransaction, AnyTransactionBackend, Connection, ConnectionOptions, Database, Error,
    Executor, Row, Rows, Transaction, TransactionOptions, Value,
};

pub type SQLiteValue = deadpool_sqlite::rusqlite::types::Value;

pub struct SQLiteRow {
    values: Vec<SQLiteValue>,
}

impl Row for SQLiteRow {
    type Value = SQLiteValue;

    fn values(&self) -> &[Self::Value] {
        &self.values
    }
}

impl Into<AnyRow> for SQLiteRow {
    fn into(self) -> AnyRow {
        let map_value = |v| match v {
            SQLiteValue::Null => Value::Null,
            SQLiteValue::Integer(v) => Value::Int64(v),
            SQLiteValue::Real(v) => Value::Float64(v),
            SQLiteValue::Text(v) => Value::String(v),
            SQLiteValue::Blob(v) => Value::Bytes(v),
        };
        AnyRow::new(self.values.into_iter().map(map_value).collect())
    }
}

pub struct SQLiteRows<'a> {
    handle: QueryHandle,
    _phantom: PhantomData<&'a ()>,
}

impl<'a> Drop for SQLiteRows<'a> {
    fn drop(&mut self) {}
}

#[async_trait::async_trait]
impl<'a> Rows<'a> for SQLiteRows<'a> {
    type Row = SQLiteRow;

    fn columns(&self) -> &[String] {
        &self.handle.columns
    }

    async fn next(&mut self) -> Option<Result<Self::Row, Error>> {
        self.handle.rx.recv().await
    }
}

pub struct SQLiteTransaction<'a> {
    tx: TransactionHandle,
    _phantom: PhantomData<&'a ()>,
}

impl<'a> Drop for SQLiteTransaction<'a> {
    fn drop(&mut self) {}
}

#[async_trait::async_trait]
impl<'a> Executor<'a> for SQLiteTransaction<'a> {
    type Rows<'b> = SQLiteRows<'b>
    where
        Self: 'b;

    async fn execute(&mut self, statement: &str) -> Result<(), Error> {
        self.tx.execute(statement).await
    }

    async fn query(&mut self, statement: &str) -> Result<Self::Rows<'_>, Error> {
        let handle = self.tx.query(statement).await?;
        Ok(SQLiteRows {
            handle,
            _phantom: PhantomData,
        })
    }
}

#[async_trait::async_trait]
impl<'a> Transaction<'a> for SQLiteTransaction<'a> {
    async fn commit(mut self) -> Result<(), Error> {
        self.tx.commit().await
    }

    async fn rollback(mut self) -> Result<(), Error> {
        self.tx.rollback().await
    }
}

#[async_trait::async_trait]
impl<'a> AnyTransactionBackend<'a> for SQLiteTransaction<'a> {
    async fn commit(self: Box<Self>) -> Result<(), Error> {
        Transaction::commit(*self).await
    }

    async fn rollback(self: Box<Self>) -> Result<(), Error> {
        Transaction::rollback(*self).await
    }

    async fn execute(&mut self, statement: &str) -> Result<(), Error> {
        Executor::execute(self, statement).await
    }

    async fn query(&mut self, statement: &str) -> Result<AnyRows, Error> {
        let rows = Executor::query(self, statement).await?;
        Ok(AnyRows::new(rows))
    }
}

pub struct SQLiteConnection {
    tx: Option<ConnectionHandle>,
    handle: Option<tokio::task::JoinHandle<()>>,
}

impl Drop for SQLiteConnection {
    fn drop(&mut self) {
        drop(self.tx.take());
        if let Some(handle) = self.handle.take() {
            let _ = futures::executor::block_on(handle).unwrap();
        };
    }
}

#[async_trait::async_trait]
impl<'a> Executor<'a> for SQLiteConnection {
    type Rows<'b> = SQLiteRows<'b>
    where
        Self: 'b;

    async fn execute(&mut self, statement: &str) -> Result<(), Error> {
        self.tx.as_mut().unwrap().execute(statement).await
    }

    async fn query(&mut self, statement: &str) -> Result<Self::Rows<'_>, Error> {
        let handle = self.tx.as_mut().unwrap().query(statement).await?;
        Ok(SQLiteRows {
            handle,
            _phantom: PhantomData,
        })
    }
}

#[async_trait::async_trait]
impl Connection for SQLiteConnection {
    type Transaction<'a> = SQLiteTransaction<'a>;

    type TransactionOptions = TransactionOptions;

    async fn transaction(
        &mut self,
        options: Self::TransactionOptions,
    ) -> Result<Self::Transaction<'_>, Error> {
        let tx = self.tx.as_mut().unwrap().transaction(options).await?;
        Ok(SQLiteTransaction {
            tx,
            _phantom: PhantomData,
        })
    }
}

#[async_trait::async_trait]
impl AnyConnectionBackend for SQLiteConnection {
    async fn transaction(
        &mut self,
        options: TransactionOptions,
    ) -> Result<AnyTransaction<'_>, Error> {
        let tx = Connection::transaction(self, options).await?;
        Ok(AnyTransaction::new(tx))
    }

    async fn execute(&mut self, statement: &str) -> Result<(), Error> {
        Executor::execute(self, statement).await?;
        Ok(())
    }

    async fn query(&mut self, statement: &str) -> Result<AnyRows, Error> {
        let rows = Executor::query(self, statement).await?;
        Ok(AnyRows::new(rows))
    }
}

#[derive(Clone)]
pub struct SQLiteDatabase(deadpool_sqlite::Pool);

#[async_trait::async_trait]
impl Database for SQLiteDatabase {
    type Connection = SQLiteConnection;

    type ConnectionOptions = ();

    async fn connection(
        &self,
        _options: Self::ConnectionOptions,
    ) -> Result<Self::Connection, Error> {
        let conn = self.0.get().await?;
        let task = ConnectionTask::new(conn);
        let (tx, rx) = oneshot::channel();
        let handle = tokio::task::spawn_blocking(move || task.run(tx));
        Ok(SQLiteConnection {
            tx: Some(rx.await??),
            handle: Some(handle),
        })
    }
}

#[async_trait::async_trait]
impl AnyDatabaseBackend for SQLiteDatabase {
    async fn connection(&self, _options: ConnectionOptions) -> Result<AnyConnection, Error> {
        let conn = Database::connection(self, ()).await?;
        Ok(AnyConnection::new(conn))
    }

    fn clone(&self) -> AnyDatabase {
        AnyDatabase::new(Clone::clone(self))
    }
}

pub async fn new_sqlite(config: &SQLiteConfig) -> Result<SQLiteDatabase, Error> {
    let config = deadpool_sqlite::Config::new(config.path.to_owned());
    let pool = config.create_pool(deadpool_sqlite::Runtime::Tokio1)?;
    Ok(SQLiteDatabase(pool))
}

struct ExecuteCommand {
    statement: String,
    tx: oneshot::Sender<Result<(), Error>>,
}

struct QueryCommand {
    statement: String,
    tx: oneshot::Sender<Result<QueryHandle, Error>>,
}

struct QueryHandle {
    columns: Vec<String>,
    rx: mpsc::Receiver<Result<SQLiteRow, Error>>,
}

struct QueryTask<'a> {
    stmt: deadpool_sqlite::rusqlite::Statement<'a>,
}

impl<'a> QueryTask<'a> {
    fn new(stmt: deadpool_sqlite::rusqlite::Statement<'a>) -> Self {
        Self { stmt }
    }

    fn run(mut self, handle_rx: oneshot::Sender<Result<QueryHandle, Error>>) {
        let columns: Vec<_> = self
            .stmt
            .column_names()
            .iter_mut()
            .map(|v| v.to_owned())
            .collect();
        let columns_len = columns.len();
        let mut rows = match self.stmt.query([]) {
            Ok(rows) => rows,
            Err(err) => {
                let _ = handle_rx.send(Err(err.into()));
                return;
            }
        };
        let (tx, rx) = mpsc::channel(1);
        if let Err(_) = handle_rx.send(Ok(QueryHandle { columns, rx })) {
            // Drop query if nobody listens result.
            return;
        }
        loop {
            let row = match rows.next() {
                Ok(Some(row)) => row,
                Ok(None) => return,
                Err(err) => {
                    _ = tx.blocking_send(Err(err.into()));
                    return;
                }
            };
            let mut values = Vec::with_capacity(columns_len);
            for i in 0..columns_len {
                let value: deadpool_sqlite::rusqlite::types::Value = match row.get(i) {
                    Ok(value) => value,
                    Err(err) => {
                        _ = tx.blocking_send(Err(err.into()));
                        return;
                    }
                };
                values.push(value);
            }
            if let Err(_) = tx.blocking_send(Ok(SQLiteRow { values })) {
                return;
            }
        }
    }
}

enum TransactionCommand {
    Commit {
        tx: oneshot::Sender<Result<(), Error>>,
    },
    Rollback {
        tx: oneshot::Sender<Result<(), Error>>,
    },
    Execute(ExecuteCommand),
    Query(QueryCommand),
}

struct TransactionHandle(mpsc::Sender<TransactionCommand>);

impl TransactionHandle {
    async fn commit(&mut self) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        self.0.send(TransactionCommand::Commit { tx }).await?;
        rx.await?
    }

    async fn rollback(&mut self) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        self.0.send(TransactionCommand::Rollback { tx }).await?;
        rx.await?
    }

    async fn execute(&mut self, statement: &str) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        self.0
            .send(TransactionCommand::Execute(ExecuteCommand {
                statement: statement.to_owned(),
                tx,
            }))
            .await?;
        rx.await?
    }

    async fn query(&mut self, statement: &str) -> Result<QueryHandle, Error> {
        let (tx, rx) = oneshot::channel();
        self.0
            .send(TransactionCommand::Query(QueryCommand {
                statement: statement.to_owned(),
                tx,
            }))
            .await?;
        Ok(rx.await??)
    }
}

impl Drop for TransactionHandle {
    fn drop(&mut self) {
        let _ = futures::executor::block_on(self.rollback());
    }
}

struct TransactionTask<'a, 'b> {
    conn: &'a mut SyncGuard<'b, deadpool_sqlite::rusqlite::Connection>,
}

impl<'a, 'b> TransactionTask<'a, 'b> {
    fn new(conn: &'a mut SyncGuard<'b, deadpool_sqlite::rusqlite::Connection>) -> Self {
        Self { conn }
    }

    fn run(self, handle_rx: oneshot::Sender<Result<TransactionHandle, Error>>) {
        let transaction = match self
            .conn
            .transaction_with_behavior(deadpool_sqlite::rusqlite::TransactionBehavior::Deferred)
        {
            Ok(conn) => conn,
            Err(err) => {
                let _ = handle_rx.send(Err(err.to_string().into()));
                return;
            }
        };
        let (tx, mut rx) = mpsc::channel(1);
        if let Err(_) = handle_rx.send(Ok(TransactionHandle(tx))) {
            // Drop transaction if nobody listens result.
            return;
        }
        while let Some(cmd) = rx.blocking_recv() {
            match cmd {
                TransactionCommand::Commit { tx } => {
                    let _ = tx.send(transaction.commit().map_err(|e| e.into()));
                    return;
                }
                TransactionCommand::Rollback { tx } => {
                    let _ = tx.send(transaction.rollback().map_err(|e| e.into()));
                    return;
                }
                TransactionCommand::Execute(cmd) => {
                    let _ = cmd.tx.send(
                        transaction
                            .execute(&cmd.statement, [])
                            .map_err(|e| e.into())
                            .map(|_| ()),
                    );
                }
                TransactionCommand::Query(cmd) => {
                    let stmt = match transaction.prepare(&cmd.statement) {
                        Ok(stmt) => stmt,
                        Err(err) => {
                            let _ = cmd.tx.send(Err(err.into()));
                            continue;
                        }
                    };
                    let task = QueryTask::new(stmt);
                    task.run(cmd.tx);
                }
            }
        }
    }
}

enum ConnectionCommand {
    Transaction {
        #[allow(unused)]
        options: TransactionOptions,
        tx: oneshot::Sender<Result<TransactionHandle, Error>>,
    },
    Execute(ExecuteCommand),
    Query(QueryCommand),
    Shutdown,
}

struct ConnectionHandle(mpsc::Sender<ConnectionCommand>);

impl ConnectionHandle {
    async fn transaction(
        &mut self,
        options: TransactionOptions,
    ) -> Result<TransactionHandle, Error> {
        let (tx, rx) = oneshot::channel();
        self.0
            .send(ConnectionCommand::Transaction { options, tx })
            .await?;
        rx.await?
    }

    async fn execute(&mut self, statement: &str) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        self.0
            .send(ConnectionCommand::Execute(ExecuteCommand {
                statement: statement.to_owned(),
                tx,
            }))
            .await?;
        rx.await?
    }

    async fn query(&mut self, statement: &str) -> Result<QueryHandle, Error> {
        let (tx, rx) = oneshot::channel();
        self.0
            .send(ConnectionCommand::Query(QueryCommand {
                statement: statement.to_owned(),
                tx,
            }))
            .await?;
        Ok(rx.await??)
    }
}

impl Drop for ConnectionHandle {
    fn drop(&mut self) {
        let _ = futures::executor::block_on(self.0.send(ConnectionCommand::Shutdown));
    }
}

struct ConnectionTask {
    conn: deadpool_sqlite::Connection,
}

impl ConnectionTask {
    fn new(conn: deadpool_sqlite::Connection) -> Self {
        Self { conn }
    }

    fn run(self, handle_rx: oneshot::Sender<Result<ConnectionHandle, Error>>) {
        let mut conn = match self.conn.lock() {
            Ok(conn) => conn,
            Err(err) => {
                let _ = handle_rx.send(Err(err.to_string().into()));
                return;
            }
        };
        let (tx, mut rx) = mpsc::channel(1);
        if let Err(_) = handle_rx.send(Ok(ConnectionHandle(tx))) {
            // Drop connection if nobody listens result.
            return;
        }
        while let Some(cmd) = rx.blocking_recv() {
            match cmd {
                ConnectionCommand::Transaction { tx, .. } => {
                    let task = TransactionTask::new(&mut conn);
                    task.run(tx);
                    continue;
                }
                ConnectionCommand::Execute(cmd) => {
                    let _ = cmd.tx.send(
                        conn.execute(&cmd.statement, [])
                            .map_err(|e| e.into())
                            .map(|_| ()),
                    );
                }
                ConnectionCommand::Query(cmd) => {
                    let stmt = match conn.prepare(&cmd.statement) {
                        Ok(stmt) => stmt,
                        Err(err) => {
                            let _ = cmd.tx.send(Err(err.into()));
                            continue;
                        }
                    };
                    let task = QueryTask::new(stmt);
                    task.run(cmd.tx);
                }
                ConnectionCommand::Shutdown => return,
            }
        }
    }
}

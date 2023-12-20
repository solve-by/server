use std::marker::PhantomData;

use deadpool_sqlite::SyncGuard;
use tokio::sync::{mpsc, oneshot};

use crate::config::SQLiteConfig;

use super::{
    AnyConnection, AnyConnectionBackend, AnyDatabase, AnyDatabaseBackend, AnyRow, AnyRows,
    AnyTransaction, AnyTransactionBackend, Connection, ConnectionOptions, Database, Error,
    Executor, Row, Rows, Transaction, TransactionOptions, Value,
};

struct ExecuteCommand {
    statement: String,
    tx: oneshot::Sender<Result<(), Error>>,
}

struct QueryCommand {
    statement: String,
    tx: oneshot::Sender<Result<SQLiteRows<'static>, Error>>,
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

    async fn query(&mut self, statement: &str) -> Result<SQLiteRows<'_>, Error> {
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
    tx: mpsc::Sender<ConnectionCommand>,
    rx: mpsc::Receiver<ConnectionCommand>,
}

impl ConnectionTask {
    pub fn new(conn: deadpool_sqlite::Connection) -> Self {
        let (tx, rx) = mpsc::channel(1);
        Self { conn, tx, rx }
    }

    fn run(mut self, rx: oneshot::Sender<Result<ConnectionHandle, Error>>) -> Result<(), Error> {
        let mut conn = match self.conn.lock() {
            Ok(conn) => conn,
            Err(err) => {
                let _ = rx.send(Err(err.to_string().into()));
                return Ok(());
            }
        };
        if let Err(_) = rx.send(Ok(ConnectionHandle(self.tx))) {
            // Drop connection if nobody listens result.
            return Ok(());
        }
        while let Some(cmd) = self.rx.blocking_recv() {
            match cmd {
                ConnectionCommand::Transaction { tx, .. } => {
                    let task = TransactionTask::new(&mut conn);
                    task.run(tx)?;
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
                    let mut stmt = match conn.prepare(&cmd.statement) {
                        Ok(stmt) => stmt,
                        Err(err) => {
                            let _ = cmd.tx.send(Err(err.into()));
                            continue;
                        }
                    };
                    let names: Vec<_> = stmt
                        .column_names()
                        .iter_mut()
                        .map(|v| v.to_owned())
                        .collect();
                    let names_len = names.len();
                    let mut rows = match stmt.query([]) {
                        Ok(rows) => rows,
                        Err(err) => {
                            let _ = cmd.tx.send(Err(err.into()));
                            continue;
                        }
                    };
                    let (tx, rx) = mpsc::channel(1);
                    if let Err(_) = cmd.tx.send(Ok(SQLiteRows {
                        rx,
                        names,
                        _phantom: PhantomData,
                    })) {
                        // Drop query if nobody listens result.
                        continue;
                    }
                    loop {
                        let row = match rows.next() {
                            Ok(Some(row)) => row,
                            Ok(None) => break,
                            Err(err) => {
                                _ = tx.blocking_send(Err(err.into()));
                                break;
                            }
                        };
                        let mut values = Vec::with_capacity(names_len);
                        for i in 0..names_len {
                            let value: deadpool_sqlite::rusqlite::types::Value = match row.get(i) {
                                Ok(value) => value,
                                Err(err) => {
                                    _ = tx.blocking_send(Err(err.into()));
                                    break;
                                }
                            };
                            values.push(value);
                        }
                        if let Err(_) = tx.blocking_send(Ok(SQLiteRow { values })) {
                            break;
                        }
                    }
                }
                ConnectionCommand::Shutdown => return Ok(()),
            }
        }
        Ok(())
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
    Shutdown,
}

struct TransactionHandle(mpsc::Sender<TransactionCommand>);

impl TransactionHandle {
    async fn commit(self) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        self.0.send(TransactionCommand::Commit { tx }).await?;
        rx.await?
    }

    async fn rollback(self) -> Result<(), Error> {
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

    async fn query(&mut self, statement: &str) -> Result<SQLiteRows, Error> {
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
        let _ = futures::executor::block_on(self.0.send(TransactionCommand::Shutdown));
    }
}

struct TransactionTask<'a, 'b> {
    conn: &'a mut SyncGuard<'b, deadpool_sqlite::rusqlite::Connection>,
    tx: mpsc::Sender<TransactionCommand>,
    rx: mpsc::Receiver<TransactionCommand>,
}

impl<'a, 'b> TransactionTask<'a, 'b> {
    pub fn new(conn: &'a mut SyncGuard<'b, deadpool_sqlite::rusqlite::Connection>) -> Self {
        let (tx, rx) = mpsc::channel(1);
        Self { conn, tx, rx }
    }

    pub fn run(
        mut self,
        rx: oneshot::Sender<Result<TransactionHandle, Error>>,
    ) -> Result<(), Error> {
        let transaction = match self.conn.transaction() {
            Ok(conn) => conn,
            Err(err) => {
                let _ = rx.send(Err(err.to_string().into()));
                return Ok(());
            }
        };
        if let Err(_) = rx.send(Ok(TransactionHandle(self.tx))) {
            // Drop transaction if nobody listens result.
            return Ok(());
        }
        while let Some(cmd) = self.rx.blocking_recv() {
            match cmd {
                TransactionCommand::Commit { tx } => {
                    let _ = tx.send(transaction.commit().map_err(|e| e.into()));
                    return Ok(());
                }
                TransactionCommand::Rollback { tx } => {
                    let _ = tx.send(transaction.rollback().map_err(|e| e.into()));
                    return Ok(());
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
                    let mut stmt = match transaction.prepare(&cmd.statement) {
                        Ok(stmt) => stmt,
                        Err(err) => {
                            let _ = cmd.tx.send(Err(err.into()));
                            continue;
                        }
                    };
                    let names: Vec<_> = stmt
                        .column_names()
                        .iter_mut()
                        .map(|v| v.to_owned())
                        .collect();
                    let names_len = names.len();
                    let mut rows = match stmt.query([]) {
                        Ok(rows) => rows,
                        Err(err) => {
                            let _ = cmd.tx.send(Err(err.into()));
                            continue;
                        }
                    };
                    let (tx, rx) = mpsc::channel(1);
                    if let Err(_) = cmd.tx.send(Ok(SQLiteRows {
                        rx,
                        names,
                        _phantom: PhantomData,
                    })) {
                        // Drop query if nobody listens result.
                        continue;
                    }
                    loop {
                        let row = match rows.next() {
                            Ok(Some(row)) => row,
                            Ok(None) => break,
                            Err(err) => {
                                _ = tx.blocking_send(Err(err.into()));
                                break;
                            }
                        };
                        let mut values = Vec::with_capacity(names_len);
                        for i in 0..names_len {
                            let value: deadpool_sqlite::rusqlite::types::Value = match row.get(i) {
                                Ok(value) => value,
                                Err(err) => {
                                    _ = tx.blocking_send(Err(err.into()));
                                    break;
                                }
                            };
                            values.push(value);
                        }
                        if let Err(_) = tx.blocking_send(Ok(SQLiteRow { values })) {
                            break;
                        }
                    }
                }
                TransactionCommand::Shutdown => return Ok(()),
            }
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct SQLiteDatabase(deadpool_sqlite::Pool);

#[async_trait::async_trait]
impl Database for SQLiteDatabase {
    type Connection = SQLiteConnection;

    async fn connection(&self, _options: ConnectionOptions) -> Result<Self::Connection, Error> {
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
    async fn connection(&self, options: ConnectionOptions) -> Result<AnyConnection, Error> {
        let conn = Database::connection(self, options).await?;
        Ok(AnyConnection::new(conn))
    }

    fn clone(&self) -> AnyDatabase {
        AnyDatabase::new(Clone::clone(self))
    }
}

pub struct SQLiteConnection {
    tx: Option<ConnectionHandle>,
    handle: Option<tokio::task::JoinHandle<Result<(), Error>>>,
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
        self.tx.as_mut().unwrap().query(statement).await
    }
}

#[async_trait::async_trait]
impl Connection for SQLiteConnection {
    type Transaction<'a> = SQLiteTransaction<'a>;

    async fn transaction(
        &mut self,
        options: TransactionOptions,
    ) -> Result<Self::Transaction<'_>, Error> {
        let tx = Some(self.tx.as_mut().unwrap().transaction(options).await?);
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

    async fn query(&mut self, statement: &str) -> Result<AnyRows<'_>, Error> {
        let rows = Executor::query(self, statement).await?;
        Ok(AnyRows::new(rows))
    }
}

pub struct SQLiteTransaction<'a> {
    tx: Option<TransactionHandle>,
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
        self.tx.as_mut().unwrap().execute(statement).await
    }

    async fn query(&mut self, statement: &str) -> Result<Self::Rows<'_>, Error> {
        self.tx.as_mut().unwrap().query(statement).await
    }
}

#[async_trait::async_trait]
impl<'a> Transaction<'a> for SQLiteTransaction<'a> {
    async fn commit(mut self) -> Result<(), Error> {
        self.tx.take().unwrap().commit().await
    }

    async fn rollback(mut self) -> Result<(), Error> {
        self.tx.take().unwrap().rollback().await
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

    async fn query(&mut self, statement: &str) -> Result<AnyRows<'_>, Error> {
        let rows = Executor::query(self, statement).await?;
        Ok(AnyRows::new(rows))
    }
}

pub struct SQLiteRows<'a> {
    rx: mpsc::Receiver<Result<SQLiteRow, Error>>,
    names: Vec<String>,
    _phantom: PhantomData<&'a ()>,
}

impl<'a> Drop for SQLiteRows<'a> {
    fn drop(&mut self) {}
}

#[async_trait::async_trait]
impl<'a> Rows<'a> for SQLiteRows<'a> {
    type Row = SQLiteRow;

    fn columns(&self) -> &[String] {
        &self.names
    }

    async fn next(&mut self) -> Option<Result<Self::Row, Error>> {
        self.rx.recv().await
    }
}

pub type SQLiteValue = deadpool_sqlite::rusqlite::types::Value;

pub struct SQLiteRow {
    values: Vec<SQLiteValue>,
}

impl Row for SQLiteRow {
    type Value = SQLiteValue;

    fn values(&self) -> &[SQLiteValue] {
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

pub async fn new_sqlite(config: &SQLiteConfig) -> Result<SQLiteDatabase, Error> {
    let config = deadpool_sqlite::Config::new(config.path.to_owned());
    let pool = config.create_pool(deadpool_sqlite::Runtime::Tokio1)?;
    Ok(SQLiteDatabase(pool))
}

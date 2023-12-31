use std::marker::PhantomData;

use deadpool_sqlite::SyncGuard;
use tokio::sync::{mpsc, oneshot};

use crate::config::SQLiteConfig;

pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;

pub type Value = deadpool_sqlite::rusqlite::types::Value;

#[derive(Clone, Debug)]
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
    handle: QueryHandle,
    _phantom: PhantomData<&'a ()>,
}

impl<'a> Rows<'a> {
    pub fn columns(&self) -> &[String] {
        &self.handle.columns
    }

    pub async fn next(&mut self) -> Option<Result<Row>> {
        self.handle.rx.recv().await
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

    pub async fn execute(&mut self, statement: &str) -> Result<()> {
        self.tx.execute(statement).await
    }

    pub async fn query(&mut self, statement: &str) -> Result<Rows> {
        let handle = self.tx.query(statement).await?;
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

    pub async fn execute(&mut self, statement: &str) -> Result<()> {
        self.tx.as_mut().unwrap().execute(statement).await
    }

    pub async fn query(&mut self, statement: &str) -> Result<Rows> {
        let handle = self.tx.as_mut().unwrap().query(statement).await?;
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
        let handle = tokio::task::spawn_blocking(move || task.run(tx));
        Ok(Connection {
            tx: Some(rx.await??),
            handle: Some(handle),
        })
    }
}

struct ExecuteCommand {
    statement: String,
    tx: oneshot::Sender<Result<()>>,
}

struct QueryCommand {
    statement: String,
    tx: oneshot::Sender<Result<QueryHandle>>,
}

struct QueryHandle {
    columns: Vec<String>,
    rx: mpsc::Receiver<Result<Row>>,
}

struct QueryTask<'a> {
    stmt: deadpool_sqlite::rusqlite::Statement<'a>,
}

impl<'a> QueryTask<'a> {
    fn new(stmt: deadpool_sqlite::rusqlite::Statement<'a>) -> Self {
        Self { stmt }
    }

    fn run(mut self, handle_rx: oneshot::Sender<Result<QueryHandle>>) {
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
                let value = match row.get(i) {
                    Ok(value) => value,
                    Err(err) => {
                        _ = tx.blocking_send(Err(err.into()));
                        return;
                    }
                };
                values.push(value);
            }
            if let Err(_) = tx.blocking_send(Ok(Row { values })) {
                return;
            }
        }
    }
}

enum TransactionCommand {
    Commit { tx: oneshot::Sender<Result<()>> },
    Rollback { tx: oneshot::Sender<Result<()>> },
    Execute(ExecuteCommand),
    Query(QueryCommand),
}

struct TransactionHandle(mpsc::Sender<TransactionCommand>);

impl TransactionHandle {
    async fn commit(&mut self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.0.send(TransactionCommand::Commit { tx }).await?;
        rx.await?
    }

    async fn rollback(&mut self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.0.send(TransactionCommand::Rollback { tx }).await?;
        rx.await?
    }

    async fn execute(&mut self, statement: &str) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.0
            .send(TransactionCommand::Execute(ExecuteCommand {
                statement: statement.to_owned(),
                tx,
            }))
            .await?;
        rx.await?
    }

    async fn query(&mut self, statement: &str) -> Result<QueryHandle> {
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

    fn run(self, handle_rx: oneshot::Sender<Result<TransactionHandle>>) {
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
        tx: oneshot::Sender<Result<TransactionHandle>>,
    },
    Execute(ExecuteCommand),
    Query(QueryCommand),
    Shutdown,
}

struct ConnectionHandle(mpsc::Sender<ConnectionCommand>);

impl ConnectionHandle {
    async fn transaction(&mut self) -> Result<TransactionHandle> {
        let (tx, rx) = oneshot::channel();
        self.0.send(ConnectionCommand::Transaction { tx }).await?;
        rx.await?
    }

    async fn execute(&mut self, statement: &str) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.0
            .send(ConnectionCommand::Execute(ExecuteCommand {
                statement: statement.to_owned(),
                tx,
            }))
            .await?;
        rx.await?
    }

    async fn query(&mut self, statement: &str) -> Result<QueryHandle> {
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

    fn run(self, handle_rx: oneshot::Sender<Result<ConnectionHandle>>) {
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

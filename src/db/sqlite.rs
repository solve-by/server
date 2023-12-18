use std::pin::Pin;
use std::task::{ready, Context, Poll};

use deadpool_sqlite::SyncGuard;
use futures_core::Stream;
use tokio::sync::{mpsc, oneshot};

use crate::config::SQLiteConfig;

use super::{
    AnyConnection, AnyConnectionBackend, AnyDatabase, AnyDatabaseBackend, AnyRows, AnyRowsBackend,
    AnyTransaction, AnyTransactionBackend, Connection, ConnectionOptions, Database, Error,
    Executor, Rows, Transaction, TransactionOptions,
};

struct ExecuteCommand {
    statement: String,
    tx: oneshot::Sender<Result<(), Error>>,
}

struct QueryCommand {
    statement: String,
    tx: oneshot::Sender<Result<mpsc::Receiver<Result<(), Error>>, Error>>,
}

enum ConnectionCommand {
    Transaction {
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

    async fn query<'r>(&'r mut self, statement: &str) -> Result<SQLiteRows, Error> {
        let (tx, rx) = oneshot::channel();
        self.0
            .send(ConnectionCommand::Query(QueryCommand {
                statement: statement.to_owned(),
                tx,
            }))
            .await?;
        Ok(SQLiteRows { rx: rx.await?? })
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
                    let mut rows = match stmt.query([]) {
                        Ok(rows) => rows,
                        Err(err) => {
                            let _ = cmd.tx.send(Err(err.into()));
                            continue;
                        }
                    };
                    let (tx, rx) = mpsc::channel(1);
                    if let Err(_) = cmd.tx.send(Ok(rx)) {
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
                        if let Err(_) = tx.blocking_send(Ok(())) {
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

    async fn query<'r>(&'r mut self, statement: &str) -> Result<SQLiteRows, Error> {
        let (tx, rx) = oneshot::channel();
        self.0
            .send(TransactionCommand::Query(QueryCommand {
                statement: statement.to_owned(),
                tx,
            }))
            .await?;
        Ok(SQLiteRows { rx: rx.await?? })
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
                    let mut rows = match stmt.query([]) {
                        Ok(rows) => rows,
                        Err(err) => {
                            let _ = cmd.tx.send(Err(err.into()));
                            continue;
                        }
                    };
                    let (tx, rx) = mpsc::channel(1);
                    if let Err(_) = cmd.tx.send(Ok(rx)) {
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
                        if let Err(_) = tx.blocking_send(Ok(())) {
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
        AnyDatabase::new(Self(self.0.clone()))
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
impl Executor<'static> for SQLiteConnection {
    type Rows<'b> = SQLiteRows
    where
        Self: 'b;

    async fn execute(&mut self, statement: &str) -> Result<(), Error> {
        self.tx.as_mut().unwrap().execute(statement).await
    }

    async fn query<'r>(&'r mut self, statement: &str) -> Result<Self::Rows<'r>, Error> {
        self.tx.as_mut().unwrap().query(statement).await
    }
}

#[async_trait::async_trait]
impl Connection for SQLiteConnection {
    type Transaction<'a> = SQLiteTransaction<'a>;

    async fn transaction<'a>(
        &'a mut self,
        options: TransactionOptions,
    ) -> Result<Self::Transaction<'a>, Error> {
        let tx = self.tx.as_mut().unwrap().transaction(options).await?;
        Ok(SQLiteTransaction { _conn: self, tx })
    }
}

#[async_trait::async_trait]
impl AnyConnectionBackend for SQLiteConnection {
    async fn transaction<'a>(
        &'a mut self,
        options: TransactionOptions,
    ) -> Result<AnyTransaction<'a>, Error> {
        let tx = Connection::transaction(self, options).await?;
        Ok(AnyTransaction::new(tx))
    }
}

pub struct SQLiteTransaction<'a> {
    _conn: &'a mut SQLiteConnection,
    tx: TransactionHandle,
}

#[async_trait::async_trait]
impl Executor<'_> for SQLiteTransaction<'_> {
    type Rows<'b> = SQLiteRows
    where
        Self: 'b;

    async fn execute(&mut self, statement: &str) -> Result<(), Error> {
        self.tx.execute(statement).await
    }

    async fn query<'r>(&'r mut self, statement: &str) -> Result<Self::Rows<'r>, Error> {
        self.tx.query(statement).await
    }
}

#[async_trait::async_trait]
impl Transaction<'_> for SQLiteTransaction<'_> {
    async fn commit(mut self) -> Result<(), Error> {
        self.tx.commit().await
    }

    async fn rollback(mut self) -> Result<(), Error> {
        self.tx.rollback().await
    }
}

#[async_trait::async_trait]
impl<'a> AnyTransactionBackend<'a> for SQLiteTransaction<'a> {
    async fn execute(&mut self, statement: &str) -> Result<(), Error> {
        Executor::execute(self, statement).await?;
        Ok(())
    }

    async fn query<'b>(&'b mut self, statement: &str) -> Result<AnyRows<'b>, Error> {
        let rows = Executor::query(self, statement).await?;
        Ok(AnyRows::new(rows))
    }

    async fn commit(mut self: Box<Self>) -> Result<(), Error> {
        self.tx.commit().await
    }

    async fn rollback(mut self: Box<Self>) -> Result<(), Error> {
        self.tx.rollback().await
    }
}

pub struct SQLiteRows {
    rx: mpsc::Receiver<Result<(), Error>>,
}

#[async_trait::async_trait]
impl Stream for SQLiteRows {
    type Item = Result<(), Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let row = ready!(self.rx.poll_recv(cx));
        Poll::Ready(row)
    }
}

#[async_trait::async_trait]
impl Rows<'_> for SQLiteRows {}

#[async_trait::async_trait]
impl AnyRowsBackend<'_> for SQLiteRows {}

pub async fn new_sqlite(config: &SQLiteConfig) -> Result<SQLiteDatabase, Error> {
    let config = deadpool_sqlite::Config::new(config.path.to_owned());
    let pool = config.create_pool(deadpool_sqlite::Runtime::Tokio1)?;
    Ok(SQLiteDatabase(pool))
}

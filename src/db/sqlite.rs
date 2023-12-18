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

enum SQLiteCommand {
    Transaction {
        options: TransactionOptions,
        tx: oneshot::Sender<Result<(), Error>>,
    },
    Execute {
        statement: String,
        tx: oneshot::Sender<Result<(), Error>>,
    },
    Query {
        statement: String,
        tx: oneshot::Sender<Result<mpsc::Receiver<()>, Error>>,
    },
    Commit {
        tx: oneshot::Sender<Result<(), Error>>,
    },
    Rollback {
        tx: oneshot::Sender<Result<(), Error>>,
    },
    Drop,
}

fn run_query(
    conn: &SyncGuard<'_, deadpool_sqlite::rusqlite::Connection>,
    statement: String,
    tx: oneshot::Sender<Result<mpsc::Receiver<()>, Error>>,
) {
    let mut stmt = match conn.prepare(&statement) {
        Ok(stmt) => stmt,
        Err(err) => {
            _ = tx.send(Err(err.into()));
            return;
        }
    };
    let mut rows = match stmt.query([]) {
        Ok(rows) => rows,
        Err(err) => {
            _ = tx.send(Err(err.into()));
            return;
        }
    };
    let (row_tx, row_rx) = mpsc::channel(1);
    tx.send(Ok(row_rx)).unwrap();
    loop {
        let row = match rows.next() {
            Ok(row) => row,
            Err(err) => return,
        };
        let row = match row {
            Some(row) => row,
            None => return,
        };
        if let Err(_) = row_tx.blocking_send(()) {
            return;
        }
    }
}

fn run_tx_query(
    transaction: &mut deadpool_sqlite::rusqlite::Transaction<'_>,
    statement: String,
    tx: oneshot::Sender<Result<mpsc::Receiver<()>, Error>>,
) {
    let mut stmt = match transaction.prepare(&statement) {
        Ok(stmt) => stmt,
        Err(err) => {
            _ = tx.send(Err(err.into()));
            return;
        }
    };
    let mut rows = match stmt.query([]) {
        Ok(rows) => rows,
        Err(err) => {
            _ = tx.send(Err(err.into()));
            return;
        }
    };
    let (row_tx, row_rx) = mpsc::channel(1);
    tx.send(Ok(row_rx)).unwrap();
    loop {
        let row = match rows.next() {
            Ok(row) => row,
            Err(err) => return,
        };
        let row = match row {
            Some(row) => row,
            None => return,
        };
        if let Err(_) = row_tx.blocking_send(()) {
            return;
        }
    }
}

fn run_transaction(
    rx: &mut mpsc::Receiver<SQLiteCommand>,
    conn: &mut SyncGuard<'_, deadpool_sqlite::rusqlite::Connection>,
    _options: TransactionOptions,
    tx: oneshot::Sender<Result<(), Error>>,
) {
    let mut transaction = match conn.transaction() {
        Ok(tx) => tx,
        Err(err) => {
            _ = tx.send(Err(err.into()));
            return;
        }
    };
    tx.send(Ok(())).unwrap();
    while let Some(cmd) = rx.blocking_recv() {
        match cmd {
            SQLiteCommand::Transaction { .. } => unreachable!(),
            SQLiteCommand::Execute { statement, tx } => {
                tx.send(match transaction.execute(&statement, []) {
                    Ok(_) => Ok(()),
                    Err(err) => Err(err.into()),
                })
                .unwrap();
            }
            SQLiteCommand::Query { statement, tx } => run_tx_query(&mut transaction, statement, tx),
            SQLiteCommand::Commit { tx } => {
                transaction.commit().unwrap();
                tx.send(Ok(())).unwrap();
                return;
            }
            SQLiteCommand::Rollback { tx } => {
                transaction.rollback().unwrap();
                tx.send(Ok(())).unwrap();
                return;
            }
            SQLiteCommand::Drop => return,
        }
    }
}

fn run_connection(mut rx: mpsc::Receiver<SQLiteCommand>, conn: deadpool_sqlite::Connection) {
    let mut conn = conn.lock().unwrap();
    while let Some(cmd) = rx.blocking_recv() {
        match cmd {
            SQLiteCommand::Transaction { options, tx } => {
                run_transaction(&mut rx, &mut conn, options, tx)
            }
            SQLiteCommand::Execute { statement, tx } => {
                tx.send(match conn.execute(&statement, []) {
                    Ok(_) => Ok(()),
                    Err(err) => Err(err.into()),
                })
                .unwrap();
            }
            SQLiteCommand::Query { statement, tx } => run_query(&conn, statement, tx),
            SQLiteCommand::Commit { .. } => unreachable!(),
            SQLiteCommand::Rollback { .. } => unreachable!(),
            SQLiteCommand::Drop => return,
        }
    }
}

pub struct SQLiteDatabase(deadpool_sqlite::Pool);

#[async_trait::async_trait]
impl Database for SQLiteDatabase {
    type Connection = SQLiteConnection;

    async fn connection(&self, _options: ConnectionOptions) -> Result<Self::Connection, Error> {
        let conn = self.0.get().await?;
        let (tx, rx) = mpsc::channel(1);
        let handle = tokio::task::spawn_blocking(move || run_connection(rx, conn));
        Ok(SQLiteConnection {
            tx,
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
    tx: tokio::sync::mpsc::Sender<SQLiteCommand>,
    handle: Option<tokio::task::JoinHandle<()>>,
}

impl Drop for SQLiteConnection {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            let tx = self.tx.clone();
            futures::executor::block_on(async move {
                let _ = tx.send(SQLiteCommand::Drop).await;
                handle.await
            })
            .unwrap();
        };
    }
}

#[async_trait::async_trait]
impl Executor<'static> for SQLiteConnection {
    type Rows<'b> = SQLiteRows
    where
        Self: 'b;

    async fn execute(&mut self, statement: &str) -> Result<(), Error> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.tx
            .send(SQLiteCommand::Execute {
                statement: statement.to_owned(),
                tx,
            })
            .await?;
        Ok(rx.await??)
    }

    async fn query<'r>(&'r mut self, statement: &str) -> Result<Self::Rows<'r>, Error> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.tx
            .send(SQLiteCommand::Query {
                statement: statement.to_owned(),
                tx,
            })
            .await?;
        Ok(SQLiteRows { rx: rx.await?? })
    }
}

#[async_trait::async_trait]
impl Connection for SQLiteConnection {
    type Transaction<'a> = SQLiteTransaction<'a>;

    async fn transaction<'a>(
        &'a mut self,
        options: TransactionOptions,
    ) -> Result<Self::Transaction<'a>, Error> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.tx
            .send(SQLiteCommand::Transaction { options, tx })
            .await?;
        rx.await??;
        Ok(SQLiteTransaction { conn: Some(self) })
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
    conn: Option<&'a mut SQLiteConnection>,
}

impl Drop for SQLiteTransaction<'_> {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            futures::executor::block_on(async move {
                let (tx, rx) = tokio::sync::oneshot::channel();
                let _ = conn.tx.send(SQLiteCommand::Rollback { tx }).await;
                let _ = rx.await;
            });
        };
    }
}

#[async_trait::async_trait]
impl Executor<'_> for SQLiteTransaction<'_> {
    type Rows<'b> = SQLiteRows
    where
        Self: 'b;

    async fn execute(&mut self, statement: &str) -> Result<(), Error> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.conn
            .as_ref()
            .unwrap()
            .tx
            .send(SQLiteCommand::Execute {
                statement: statement.to_owned(),
                tx,
            })
            .await?;
        Ok(rx.await??)
    }

    async fn query<'r>(&'r mut self, statement: &str) -> Result<Self::Rows<'r>, Error> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.conn
            .as_ref()
            .unwrap()
            .tx
            .send(SQLiteCommand::Query {
                statement: statement.to_owned(),
                tx,
            })
            .await?;
        Ok(SQLiteRows { rx: rx.await?? })
    }
}

#[async_trait::async_trait]
impl Transaction<'_> for SQLiteTransaction<'_> {
    async fn commit(mut self) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        self.conn
            .take()
            .unwrap()
            .tx
            .send(SQLiteCommand::Commit { tx })
            .await?;
        Ok(rx.await??)
    }

    async fn rollback(mut self) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        self.conn
            .take()
            .unwrap()
            .tx
            .send(SQLiteCommand::Rollback { tx })
            .await?;
        Ok(rx.await??)
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
        let (tx, rx) = oneshot::channel();
        self.conn
            .take()
            .unwrap()
            .tx
            .send(SQLiteCommand::Commit { tx })
            .await?;
        Ok(rx.await??)
    }

    async fn rollback(mut self: Box<Self>) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        self.conn
            .take()
            .unwrap()
            .tx
            .send(SQLiteCommand::Rollback { tx })
            .await?;
        Ok(rx.await??)
    }
}

pub struct SQLiteRows {
    rx: mpsc::Receiver<()>,
}

#[async_trait::async_trait]
impl Stream for SQLiteRows {
    type Item = ();

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<()>> {
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

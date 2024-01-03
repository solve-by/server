use deadpool_sqlite::rusqlite::params_from_iter;
use deadpool_sqlite::SyncGuard;
use tokio::sync::{mpsc, oneshot};

use super::query::{ExecuteCommand, QueryCommand, QueryHandle, QueryTask};
use super::{Result, Value};

enum TransactionCommand {
    Commit { tx: oneshot::Sender<Result<()>> },
    Rollback { tx: oneshot::Sender<Result<()>> },
    Execute(ExecuteCommand),
    Query(QueryCommand),
}

pub(super) struct TransactionHandle(mpsc::Sender<TransactionCommand>);

impl TransactionHandle {
    pub async fn commit(&mut self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.0.send(TransactionCommand::Commit { tx }).await?;
        rx.await?
    }

    pub async fn rollback(&mut self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.0.send(TransactionCommand::Rollback { tx }).await?;
        rx.await?
    }

    pub async fn execute(&mut self, statement: String, arguments: Vec<Value>) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.0
            .send(TransactionCommand::Execute(ExecuteCommand {
                statement,
                arguments,
                tx,
            }))
            .await?;
        rx.await?
    }

    pub async fn query(&mut self, statement: String, arguments: Vec<Value>) -> Result<QueryHandle> {
        let (tx, rx) = oneshot::channel();
        self.0
            .send(TransactionCommand::Query(QueryCommand {
                statement,
                arguments,
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

pub(super) struct TransactionTask<'a, 'b> {
    conn: &'a mut SyncGuard<'b, deadpool_sqlite::rusqlite::Connection>,
}

impl<'a, 'b> TransactionTask<'a, 'b> {
    pub fn new(conn: &'a mut SyncGuard<'b, deadpool_sqlite::rusqlite::Connection>) -> Self {
        Self { conn }
    }

    pub fn blocking_run(self, handle_rx: oneshot::Sender<Result<TransactionHandle>>) {
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
                            .execute(&cmd.statement, params_from_iter(cmd.arguments.into_iter()))
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
                    let task = QueryTask::new(stmt, cmd.arguments);
                    task.blocking_run(cmd.tx);
                }
            }
        }
    }
}

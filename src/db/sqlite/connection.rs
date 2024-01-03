use deadpool_sqlite::rusqlite::params_from_iter;
use tokio::sync::{mpsc, oneshot};

use super::query::{ExecuteCommand, QueryCommand, QueryHandle, QueryTask};
use super::transaction::{TransactionHandle, TransactionTask};
use super::{Result, Value};

enum ConnectionCommand {
    Transaction {
        tx: oneshot::Sender<Result<TransactionHandle>>,
    },
    Execute(ExecuteCommand),
    Query(QueryCommand),
    Shutdown,
}

pub(super) struct ConnectionHandle(mpsc::Sender<ConnectionCommand>);

impl ConnectionHandle {
    pub async fn transaction(&mut self) -> Result<TransactionHandle> {
        let (tx, rx) = oneshot::channel();
        self.0.send(ConnectionCommand::Transaction { tx }).await?;
        rx.await?
    }

    pub async fn execute(&mut self, statement: String, arguments: Vec<Value>) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.0
            .send(ConnectionCommand::Execute(ExecuteCommand {
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
            .send(ConnectionCommand::Query(QueryCommand {
                statement,
                arguments,
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

pub(super) struct ConnectionTask {
    conn: deadpool_sqlite::Connection,
}

impl ConnectionTask {
    pub fn new(conn: deadpool_sqlite::Connection) -> Self {
        Self { conn }
    }

    pub fn blocking_run(self, handle_rx: oneshot::Sender<Result<ConnectionHandle>>) {
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
                    task.blocking_run(tx);
                    continue;
                }
                ConnectionCommand::Execute(cmd) => {
                    let _ = cmd.tx.send(
                        conn.execute(&cmd.statement, params_from_iter(cmd.arguments.into_iter()))
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
                    let task = QueryTask::new(stmt, cmd.arguments);
                    task.blocking_run(cmd.tx);
                }
                ConnectionCommand::Shutdown => return,
            }
        }
    }
}

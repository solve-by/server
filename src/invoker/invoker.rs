use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::core::Core;
use crate::models::Error;

struct Task {}

pub struct Invoker {
    core: Arc<Core>,
}

impl Invoker {
    pub fn new(core: Arc<Core>) -> Self {
        Self { core }
    }

    pub async fn run(self, shutdown: CancellationToken) -> Result<(), Error> {
        slog::info!(self.core.logger(), "Running invoker loop");
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    return Ok(());
                }
                task = self.take_task() => {
                    let task = match task {
                        Ok(Some(task)) => task,
                        Ok(None) => {
                            let sleep = tokio::time::timeout(Duration::from_secs(1), shutdown.cancelled());
                            if let Ok(()) = sleep.await {
                                return Ok(());
                            }
                            continue;
                        }
                        Err(err) => {
                            println!("{}", err);
                            continue;
                        }
                    };
                    if let Err(err) = self.run_task(task).await {
                        println!("{}", err);
                    }
                }
            }
        }
    }

    async fn take_task(&self) -> Result<Option<Task>, Error> {
        let tasks = self.core.tasks().expect("task store should be initialized");
        todo!()
    }

    async fn run_task(&self, _task: Task) -> Result<(), Error> {
        todo!()
    }
}

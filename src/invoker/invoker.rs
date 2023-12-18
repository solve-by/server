use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::models::Error;

struct Task {}

pub struct Invoker {}

impl Invoker {
    pub fn new() -> Self {
        Self {}
    }

    pub async fn run(self, shutdown: CancellationToken) -> Result<(), Error> {
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
        todo!()
    }

    async fn run_task(&self, _task: Task) -> Result<(), Error> {
        todo!()
    }
}

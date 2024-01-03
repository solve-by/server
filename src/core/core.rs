use std::sync::Arc;

use slog::Drain;

use crate::config::Config;
use crate::db::any::{new_database, Database};
use crate::models::{FileStore, TaskStore};

pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;

pub struct Core {
    db: Arc<Database>,
    logger: slog::Logger,
    files: Option<Arc<FileStore>>,
    tasks: Option<Arc<TaskStore>>,
}

impl Core {
    pub fn new(config: &Config) -> Result<Self> {
        let db = Arc::new(new_database(&config.db)?);
        let decorator = slog_term::TermDecorator::new().build();
        let drain = slog_term::FullFormat::new(decorator)
            .use_file_location()
            .build()
            .fuse();
        let drain = slog_async::Async::new(drain)
            .chan_size(4096)
            .overflow_strategy(slog_async::OverflowStrategy::DropAndReport)
            .build()
            .fuse();
        let logger = slog::Logger::root(drain, slog::o!());
        Ok(Self {
            db,
            logger,
            files: None,
            tasks: None,
        })
    }

    pub fn db(&self) -> Arc<Database> {
        self.db.clone()
    }

    pub fn files(&self) -> Option<Arc<FileStore>> {
        self.files.clone()
    }

    pub fn tasks(&self) -> Option<Arc<TaskStore>> {
        self.tasks.clone()
    }

    pub fn logger(&self) -> &slog::Logger {
        &self.logger
    }

    pub async fn init_server(&mut self) -> Result<()> {
        self.files = Some(Arc::new(FileStore::new(
            self.db(),
            "solve_file",
            "solve_file_event",
        )));
        self.tasks = Some(Arc::new(TaskStore::new(
            self.db(),
            "solve_task",
            "solve_task_event",
        )));
        Ok(())
    }

    pub async fn init_invoker(&mut self) -> Result<()> {
        self.files = Some(Arc::new(FileStore::new(
            self.db(),
            "solve_file",
            "solve_file_event",
        )));
        self.tasks = Some(Arc::new(TaskStore::new(
            self.db(),
            "solve_task",
            "solve_task_event",
        )));
        Ok(())
    }
}

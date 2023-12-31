use crate::db::any::{Error, FromRow, Result, Row, Value};

use super::types::Instant;
use super::{BaseEvent, Object, PersistentStore};

#[derive(Clone, Copy, Default)]
pub enum TaskKind {
    #[default]
    Unknown,
    JudgeSolution,
    UpdateProblemPackage,
}

impl TryFrom<Value> for TaskKind {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self> {
        match value.try_into()? {
            1 => Ok(TaskKind::JudgeSolution),
            2 => Ok(TaskKind::UpdateProblemPackage),
            v => Err(format!("unknown task kind: {}", v).into()),
        }
    }
}

#[derive(Clone, Copy, Default)]
pub enum TaskStatus {
    #[default]
    Queued,
    Running,
    Succeeded,
    Failed,
}

impl TryFrom<Value> for TaskStatus {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self> {
        match value.try_into()? {
            0 => Ok(TaskStatus::Queued),
            1 => Ok(TaskStatus::Running),
            2 => Ok(TaskStatus::Succeeded),
            3 => Ok(TaskStatus::Failed),
            v => Err(format!("unknown task kind: {}", v).into()),
        }
    }
}

#[derive(Clone, Default)]
pub struct Task {
    pub id: i64,
    pub kind: TaskKind,
    pub config: serde_json::Value,
    pub status: TaskStatus,
    pub state: serde_json::Value,
    pub expire_time: Option<Instant>,
}

impl FromRow for Task {
    fn from_row(row: &Row) -> Result<Self> {
        Ok(Self {
            id: row.get("id")?.try_into()?,
            kind: row.get("kind")?.try_into()?,
            config: row.get("config")?.try_into()?,
            status: row.get("status")?.try_into()?,
            state: row.get("state")?.try_into()?,
            expire_time: row.get("expire_time")?.try_into()?,
        })
    }
}

impl Object for Task {
    type Id = i64;

    fn get_id(&self) -> Self::Id {
        self.id
    }

    fn set_id(&mut self, id: Self::Id) {
        self.id = id;
    }
}

pub type TaskEvent = BaseEvent<Task>;

pub type TaskStore = PersistentStore<Task>;

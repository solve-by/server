use crate::db::any::{Error, FromRow, Result, Row, Value};

use super::types::{now, Instant};
use super::{BaseEvent, Object, PersistentStore};

#[derive(Clone, Copy, Default)]
pub enum FileStatus {
    #[default]
    Pending,
    Available,
}

impl TryFrom<Value> for FileStatus {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self> {
        match value.try_into()? {
            0 => Ok(FileStatus::Pending),
            1 => Ok(FileStatus::Available),
            v => Err(format!("unknown file status: {}", v).into()),
        }
    }
}

#[derive(Clone)]
pub struct File {
    pub id: i64,
    pub status: FileStatus,
    pub expire_time: Instant,
    pub path: String,
    pub meta: serde_json::Value,
}

impl Default for File {
    fn default() -> Self {
        Self {
            id: Default::default(),
            status: Default::default(),
            expire_time: now(),
            path: Default::default(),
            meta: Default::default(),
        }
    }
}

impl FromRow for File {
    fn from_row(row: &Row) -> Result<Self> {
        Ok(Self {
            id: row.get("id")?.try_into()?,
            status: row.get("status")?.try_into()?,
            expire_time: row.get("expire_time")?.try_into()?,
            path: row.get("path")?.try_into()?,
            meta: row.get("meta")?.try_into()?,
        })
    }
}

impl Object for File {
    type Id = i64;

    fn get_id(&self) -> Self::Id {
        self.id
    }

    fn set_id(&mut self, id: Self::Id) {
        self.id = id;
    }
}

pub type FileEvent = BaseEvent<File>;

pub type FileStore = PersistentStore<File>;

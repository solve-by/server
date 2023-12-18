use std::time::Instant;

use super::{BaseEvent, Object, PersistentStore};

#[derive(Clone, Copy)]
pub enum FileStatus {
    Pending,
    Available,
}

#[derive(Clone)]
pub struct File {
    pub id: i64,
    pub status: FileStatus,
    pub expire_time: Instant,
    pub path: String,
    pub meta: serde_json::Value,
}

pub type FileEvent = BaseEvent<File>;

impl Default for File {
    fn default() -> Self {
        Self {
            id: Default::default(),
            status: FileStatus::Pending,
            expire_time: Instant::now(),
            path: Default::default(),
            meta: Default::default(),
        }
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

pub type FileStore = PersistentStore<File>;

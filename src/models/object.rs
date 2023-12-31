use crate::db::any::{Error, FromRow, Result, Row, Value};

use super::types::{now, Instant};

pub trait Object: FromRow + Default + Clone + Send + Sync + 'static {
    type Id: Default + Copy + Send + Sync + 'static;

    fn get_id(&self) -> Self::Id;

    fn set_id(&mut self, id: Self::Id);
}

pub trait Event: FromRow + Default + Clone + Send + Sync + 'static {
    type Object: Object;

    fn get_id(&self) -> i64;

    fn set_id(&mut self, id: i64);

    fn get_kind(&self) -> EventKind;

    fn set_kind(&mut self, kind: EventKind);

    fn get_time(&self) -> Instant;

    fn set_time(&mut self, time: Instant);

    fn get_account_id(&self) -> Option<i64>;

    fn set_account_id(&mut self, id: Option<i64>);

    fn get_object(&self) -> &Self::Object;

    fn get_mut_object(&mut self) -> &mut Self::Object;

    fn set_object(&mut self, object: Self::Object);
}

#[derive(Clone, Copy)]
pub enum EventKind {
    Create,
    Delete,
    Update,
}

impl TryFrom<Value> for EventKind {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self> {
        match value.try_into()? {
            1 => Ok(EventKind::Create),
            2 => Ok(EventKind::Delete),
            3 => Ok(EventKind::Update),
            v => Err(format!("unknown event kind: {}", v).into()),
        }
    }
}

#[derive(Clone)]
pub struct BaseEvent<O> {
    id: i64,
    time: Instant,
    account_id: Option<i64>,
    kind: EventKind,
    object: O,
}

impl<O: Object> BaseEvent<O> {
    pub fn create(object: O) -> Self {
        Self {
            kind: EventKind::Create,
            object,
            ..Default::default()
        }
    }

    pub fn update(object: O) -> Self {
        Self {
            kind: EventKind::Update,
            object,
            ..Default::default()
        }
    }

    pub fn delete(id: O::Id) -> Self {
        let mut value = Self {
            kind: EventKind::Delete,
            ..Default::default()
        };
        value.get_mut_object().set_id(id);
        value
    }
}

impl<O: Object> Default for BaseEvent<O> {
    fn default() -> Self {
        Self {
            id: Default::default(),
            time: now(),
            account_id: Default::default(),
            kind: EventKind::Create,
            object: Default::default(),
        }
    }
}

impl<O: Object> FromRow for BaseEvent<O> {
    fn from_row(row: &Row) -> Result<Self> {
        Ok(Self {
            id: row.get("event_id")?.try_into()?,
            time: row.get("event_time")?.try_into()?,
            account_id: row.get("event_account_id")?.try_into()?,
            kind: row.get("event_kind")?.try_into()?,
            object: FromRow::from_row(row)?,
        })
    }
}

impl<O: Object<Id = I>, I> Event for BaseEvent<O> {
    type Object = O;

    fn get_id(&self) -> i64 {
        self.id
    }

    fn set_id(&mut self, id: i64) {
        self.id = id
    }

    fn get_kind(&self) -> EventKind {
        self.kind
    }

    fn set_kind(&mut self, kind: EventKind) {
        self.kind = kind
    }

    fn get_time(&self) -> Instant {
        self.time
    }

    fn set_time(&mut self, time: Instant) {
        self.time = time
    }

    fn get_account_id(&self) -> Option<i64> {
        self.account_id
    }

    fn set_account_id(&mut self, id: Option<i64>) {
        self.account_id = id
    }

    fn get_object(&self) -> &O {
        &self.object
    }

    fn get_mut_object(&mut self) -> &mut O {
        &mut self.object
    }

    fn set_object(&mut self, object: O) {
        self.object = object
    }
}

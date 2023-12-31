use chrono::{DateTime, Utc};

use crate::db::any::{Error, Result, Value};

pub type Instant = DateTime<Utc>;

pub fn now() -> Instant {
    Utc::now()
}

impl TryFrom<Value> for Instant {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self> {
        DateTime::from_timestamp(value.try_into()?, 0).ok_or("cannot parse timestamp".into())
    }
}

impl TryFrom<Value> for Option<Instant> {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self> {
        Ok(match value {
            Value::Null => None,
            _ => Some(value.try_into()?),
        })
    }
}

impl TryFrom<Value> for serde_json::Value {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self> {
        Ok(match value {
            Value::String(v) => serde_json::from_str(&v)?,
            Value::Bytes(v) => serde_json::from_slice(&v)?,
            _ => return Err("cannot parse json".into()),
        })
    }
}

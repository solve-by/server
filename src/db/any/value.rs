use super::{Error, Result};

#[derive(Clone, Debug, PartialEq, Default)]
pub enum Value {
    #[default]
    Null,
    Bool(bool),
    Int64(i64),
    Float64(f64),
    String(String),
    Bytes(Vec<u8>),
}

impl TryFrom<Value> for bool {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self> {
        match value {
            Value::Bool(v) => Ok(v),
            _ => Err("cannot parse bool".into()),
        }
    }
}

impl TryFrom<Value> for Option<bool> {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self> {
        Ok(match value {
            Value::Null => None,
            _ => Some(value.try_into()?),
        })
    }
}

impl TryFrom<Value> for i64 {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self> {
        match value {
            Value::Int64(v) => Ok(v),
            _ => Err("cannot parse i64".into()),
        }
    }
}

impl TryFrom<Value> for Option<i64> {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self> {
        Ok(match value {
            Value::Null => None,
            _ => Some(value.try_into()?),
        })
    }
}

impl TryFrom<Value> for f64 {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self> {
        match value {
            Value::Float64(v) => Ok(v),
            _ => Err("cannot parse f64".into()),
        }
    }
}

impl TryFrom<Value> for Option<f64> {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self> {
        Ok(match value {
            Value::Null => None,
            _ => Some(value.try_into()?),
        })
    }
}

impl TryFrom<Value> for String {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self> {
        match value {
            Value::String(v) => Ok(v),
            _ => Err("cannot parse String".into()),
        }
    }
}

impl TryFrom<Value> for Option<String> {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self> {
        Ok(match value {
            Value::Null => None,
            _ => Some(value.try_into()?),
        })
    }
}

impl TryFrom<Value> for Vec<u8> {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self> {
        match value {
            Value::Bytes(v) => Ok(v),
            _ => Err("cannot parse Vec<u8>".into()),
        }
    }
}

impl TryFrom<Value> for Option<Vec<u8>> {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self> {
        Ok(match value {
            Value::Null => None,
            _ => Some(value.try_into()?),
        })
    }
}

impl From<bool> for Value {
    fn from(value: bool) -> Self {
        Value::Bool(value)
    }
}

impl From<Option<bool>> for Value {
    fn from(value: Option<bool>) -> Self {
        match value {
            Some(v) => v.into(),
            None => Value::Null,
        }
    }
}

impl From<i64> for Value {
    fn from(value: i64) -> Self {
        Value::Int64(value)
    }
}

impl From<Option<i64>> for Value {
    fn from(value: Option<i64>) -> Self {
        match value {
            Some(v) => v.into(),
            None => Value::Null,
        }
    }
}

impl From<f64> for Value {
    fn from(value: f64) -> Self {
        Value::Float64(value)
    }
}

impl From<Option<f64>> for Value {
    fn from(value: Option<f64>) -> Self {
        match value {
            Some(v) => v.into(),
            None => Value::Null,
        }
    }
}

impl From<String> for Value {
    fn from(value: String) -> Self {
        Value::String(value)
    }
}

impl From<Option<String>> for Value {
    fn from(value: Option<String>) -> Self {
        match value {
            Some(v) => v.into(),
            None => Value::Null,
        }
    }
}

impl From<Vec<u8>> for Value {
    fn from(value: Vec<u8>) -> Self {
        Value::Bytes(value)
    }
}

impl From<Option<Vec<u8>>> for Value {
    fn from(value: Option<Vec<u8>>) -> Self {
        match value {
            Some(v) => v.into(),
            None => Value::Null,
        }
    }
}

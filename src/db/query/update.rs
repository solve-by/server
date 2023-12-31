use crate::db::any::{QueryBuilder, Value};

pub struct Update {
    table: String,
    columns: Vec<String>,
    values: Vec<Value>,
}

impl Update {
    pub fn new() -> Self {
        Self {
            table: Default::default(),
            columns: Default::default(),
            values: Default::default(),
        }
    }

    pub fn table<T: Into<String>>(mut self, table: T) -> Self {
        self.table = table.into();
        self
    }

    pub fn columns(mut self, columns: Vec<String>) -> Self {
        self.columns = columns;
        self
    }

    pub fn values(mut self, values: Vec<Value>) -> Self {
        self.values = values;
        self
    }

    pub fn query(self, mut builder: QueryBuilder) -> QueryBuilder {
        assert_eq!(self.columns.len(), self.values.len());
        builder.push_str("UPDATE ");
        builder.push_name(&self.table);
        builder.push_str(" SET ");
        for ((i, column), value) in self
            .columns
            .into_iter()
            .enumerate()
            .zip(self.values.into_iter())
        {
            if i > 0 {
                builder.push_str(", ");
            }
            builder.push_name(&column);
            builder.push_str(" = ");
            builder.push_value(value);
        }
        builder
    }
}

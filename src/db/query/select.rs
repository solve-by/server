use crate::db::any::{Query, QueryBuilder, Value};

pub struct Select {
    table: String,
    names: Vec<String>,
}

impl Select {
    pub fn new() -> Self {
        Self {
            table: Default::default(),
            names: Default::default(),
        }
    }

    pub fn table<T: Into<String>>(mut self, table: T) -> Self {
        self.table = table.into();
        self
    }

    pub fn columns(mut self, names: Vec<String>) -> Self {
        self.names = names;
        self
    }

    pub fn query(self, mut builder: QueryBuilder) -> QueryBuilder {
        builder.push_str("SELECT ");
        for (i, name) in self.names.into_iter().enumerate() {
            if i > 0 {
                builder.push_str(", ");
            }
            builder.push_name(&name);
        }
        builder.push_str(" FROM ");
        builder.push_name(&self.table);
        builder.push_str(" WHERE ");
        builder.push_str("true");
        builder
    }
}

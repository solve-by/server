use super::Value;

pub trait Query: Send + Sync {
    fn build(&self) -> (&str, &[Value]);
}

pub trait QueryBuilderBackend: Send + Sync {
    fn push(&mut self, ch: char);

    fn push_str(&mut self, string: &str);

    fn push_name(&mut self, name: &str);

    fn push_value(&mut self, value: Value);

    fn build(&self) -> (&str, &[Value]);
}

pub struct QueryBuilder {
    inner: Box<dyn QueryBuilderBackend>,
}

impl QueryBuilder {
    pub fn new<T: QueryBuilderBackend + 'static>(builder: T) -> Self {
        let inner = Box::new(builder);
        Self { inner }
    }

    pub fn push(&mut self, ch: char) {
        self.inner.push(ch);
    }

    pub fn push_str(&mut self, string: &str) {
        self.inner.push_str(string);
    }

    pub fn push_name(&mut self, name: &str) {
        self.inner.push_name(name);
    }

    pub fn push_value<T: Into<Value>>(&mut self, value: T) {
        self.inner.push_value(value.into());
    }
}

impl Query for QueryBuilder {
    fn build(&self) -> (&str, &[Value]) {
        self.inner.build()
    }
}

impl Query for &str {
    fn build(&self) -> (&str, &[Value]) {
        (self, &[])
    }
}

impl Query for (&str, &[Value]) {
    fn build(&self) -> (&str, &[Value]) {
        (self.0, self.1)
    }
}

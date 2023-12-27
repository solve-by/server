use std::{marker::PhantomData, sync::Arc};

use crate::db::any::{Database, Executor, Transaction};

use super::{BaseEvent, Event, Object};

pub type Error = Box<dyn std::error::Error + Send + Sync>;

pub struct Context<'a> {
    pub tx: Option<Transaction<'a>>,
    pub account_id: Option<i64>,
}

#[async_trait::async_trait]
pub trait ObjectStore: Send {
    type Id;
    type Object: Object<Id = Self::Id>;
    type Event: Event<Object = Self::Object>;

    async fn create(&self, ctx: Context<'_>, object: Self::Object) -> Result<Self::Event, Error>;
    async fn update(&self, ctx: Context<'_>, object: Self::Object) -> Result<Self::Event, Error>;
    async fn delete(&self, ctx: Context<'_>, id: Self::Id) -> Result<Self::Event, Error>;
}

pub struct PersistentStore<O: Object> {
    db: Arc<Database>,
    _phantom: PhantomData<O>,
}

impl<O: Object> PersistentStore<O> {
    pub fn new(db: Arc<Database>) -> Self {
        Self {
            db,
            _phantom: PhantomData,
        }
    }

    #[allow(unused)]
    async fn create_object(&self, tx: &mut impl Executor<'_>, object: O) -> Result<O, Error> {
        todo!()
    }

    #[allow(unused)]
    async fn update_object(&self, tx: &mut impl Executor<'_>, object: O) -> Result<O, Error> {
        todo!()
    }

    #[allow(unused)]
    async fn delete_object(&self, tx: &mut impl Executor<'_>, object: O::Id) -> Result<(), Error> {
        todo!()
    }

    #[allow(unused)]
    async fn create_event(
        &self,
        tx: &mut impl Executor<'_>,
        event: BaseEvent<O>,
    ) -> Result<BaseEvent<O>, Error> {
        todo!()
    }
}

#[async_trait::async_trait]
impl<O: Object> ObjectStore for PersistentStore<O> {
    type Id = O::Id;
    type Object = O;
    type Event = BaseEvent<O>;

    async fn create(&self, mut ctx: Context<'_>, object: O) -> Result<Self::Event, Error> {
        if let Some(mut tx) = ctx.tx.take() {
            let object = self.create_object(&mut tx, object).await?;
            let event = self
                .create_event(&mut tx, BaseEvent::create(object))
                .await?;
            return Ok(event);
        }
        let mut conn = self.db.connection(Default::default()).await?;
        let tx = conn.transaction(Default::default()).await?;
        let ctx = Context {
            tx: Some(tx),
            ..ctx
        };
        self.create(ctx, object).await
    }

    async fn update(&self, mut ctx: Context<'_>, object: O) -> Result<Self::Event, Error> {
        if let Some(mut tx) = ctx.tx.take() {
            let object = self.update_object(&mut tx, object).await?;
            let event = self
                .create_event(&mut tx, BaseEvent::update(object))
                .await?;
            return Ok(event);
        }
        let mut conn = self.db.connection(Default::default()).await?;
        let tx = conn.transaction(Default::default()).await?;
        let ctx = Context {
            tx: Some(tx),
            ..ctx
        };
        self.update(ctx, object).await
    }

    async fn delete(&self, mut ctx: Context<'_>, id: O::Id) -> Result<Self::Event, Error> {
        if let Some(mut tx) = ctx.tx.take() {
            self.delete_object(&mut tx, id).await?;
            let event = self.create_event(&mut tx, BaseEvent::delete(id)).await?;
            return Ok(event);
        }
        let mut conn = self.db.connection(Default::default()).await?;
        let tx = conn.transaction(Default::default()).await?;
        let ctx = Context {
            tx: Some(tx),
            ..ctx
        };
        self.delete(ctx, id).await
    }
}

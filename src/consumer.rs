use std::mem;

use crate::{Message, MessageBus};

use self::{
    handler::{AsyncFnFallbackHandler, AsyncFnMessageHandler},
    router::Router,
    worker::{WorkerContext, WorkerPool, WorkerPoolConfig},
};

mod context;
mod extension;
mod extract;
mod handler;
mod router;
mod sentinel;
pub(crate) mod task_local;
mod worker;

pub use extension::Extension;
pub use extract::TryExtract;
pub use handler::{Handler, MessageHandler};
pub use hook::Hook;
pub use sentinel::Sentinel;

pub(crate) use extension::Extensions;
pub(crate) use hook::Hooks;

pub mod hook;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum MessageConsumerError {
    #[error("Failed sentinels: {0:?}")]
    SentinelError(Vec<Box<dyn Sentinel>>),
}

#[derive(Default)]
pub struct MessageConsumer {
    router: Router,
    extensions: Extensions,
    hooks: Hooks,
}

impl MessageConsumer {
    pub fn message_handler<H, Fun, Args, M>(self, handler: H) -> Self
    where
        Fun: 'static,
        Args: 'static,
        M: Message,
        H: Into<AsyncFnMessageHandler<Fun, Args, M>>,
        AsyncFnMessageHandler<Fun, Args, M>: MessageHandler,
    {
        Self {
            router: self.router.message_handler(handler),
            ..self
        }
    }

    pub fn fallback_handler<H, Fun, Args>(self, handler: H) -> Self
    where
        Fun: 'static,
        Args: 'static,
        H: Into<AsyncFnFallbackHandler<Fun, Args>>,
        AsyncFnFallbackHandler<Fun, Args>: Handler,
    {
        Self {
            router: self.router.fallback_handler(handler),
            ..self
        }
    }

    pub fn extension<T>(self, extension: T) -> Self
    where
        T: Clone + Send + Sync + 'static,
    {
        Self {
            extensions: self.extensions.insert(extension),
            ..self
        }
    }

    pub fn hook<T>(self, hook: T) -> Self
    where
        T: Hook,
    {
        Self {
            hooks: self.hooks.push(hook),
            ..self
        }
    }

    pub async fn listen<B: MessageBus>(
        mut self,
        config: WorkerPoolConfig,
        bus: B,
    ) -> Result<(), MessageConsumerError> {
        let sentinels = mem::take(&mut self.router.sentinels);

        let mut abortable = sentinels
            .into_iter()
            .filter(|x| x.abort(&self))
            .collect::<Vec<_>>();

        if !abortable.is_empty() {
            abortable.sort_by_key(|s| s.cause());
            abortable.dedup_by_key(|s| s.cause());

            return Err(MessageConsumerError::SentinelError(abortable));
        }

        let ctx = WorkerContext::new(self, bus);

        let mut worker_pool: WorkerPool<B> = WorkerPool::new(config, ctx.clone());

        while let Ok(msg) = ctx.bus().recv().await {
            worker_pool.dispatch(msg).await;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Confirmation {
    Ack,
    Nack,
    Reject,
}

impl From<()> for Confirmation {
    fn from(_: ()) -> Self {
        Self::Ack
    }
}

impl<T: Into<Confirmation>, E: Into<Confirmation>> From<Result<T, E>> for Confirmation {
    fn from(result: Result<T, E>) -> Self {
        match result {
            Ok(ok) => ok.into(),
            Err(err) => err.into(),
        }
    }
}

#[cfg(test)]
mod test {
    use crate::{
        test::{TestMessage, TestMessageBus},
        RawMessage,
    };

    use super::*;

    #[tokio::test]
    async fn detect_missing_extensions() {
        async fn fallback_handler_missing_states(
            _msg: RawMessage,
            _s1: Extension<()>,
            _s2: Extension<((), ())>,
        ) {
        }

        async fn message_handler_missing_states(
            _msg: TestMessage,
            _s1: Extension<()>,
            _s2: Extension<((), ())>,
        ) {
        }

        let consumer = MessageConsumer::default()
            .message_handler(message_handler_missing_states)
            .fallback_handler(fallback_handler_missing_states);

        let _error = consumer
            .listen(WorkerPoolConfig::KeyRoutedPoolConfig, TestMessageBus)
            .await
            .unwrap_err();
    }
}

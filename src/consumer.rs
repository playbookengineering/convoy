use std::{
    convert::Infallible,
    error::Error,
    fmt::Display,
    future::Future,
    mem,
    pin::Pin,
    task::{Context, Poll},
    time::Instant,
};

use self::{
    router::Router,
    worker::{WorkerContext, WorkerPool},
};

mod context;
mod extension;
mod extract;
mod handler;
mod message_bus;
mod router;
mod sentinel;
mod task_local;
mod worker;

use futures_lite::{Stream, StreamExt};

pub use context::ProcessContext;
pub use extension::Extension;
pub use extract::TryExtract;
pub use handler::{Handler, RoutableHandler};
pub use hook::Hook;
pub use message_bus::{IncomingMessage, MessageBus};
pub use sentinel::Sentinel;
#[cfg(feature = "unstable-features")]
pub use task_local::TaskLocal;
pub use worker::{FixedPoolConfig, KeyRoutedPoolConfig, WorkerPoolConfig};

pub(crate) use extension::Extensions;
pub(crate) use hook::Hooks;

pub mod hook;

use thiserror::Error;
use tokio::time::Interval;

#[derive(Debug, Error)]
pub enum MessageConsumerError<B> {
    #[error("Failed sentinels: {0:?}")]
    SentinelError(Vec<Box<dyn Sentinel<B>>>),

    #[error("Message bus error: {0}")]
    MessageBusError(Box<dyn Error + Send + Sync>),

    #[error("Message bus EOF")]
    MessageBusEOF,
}

pub struct MessageConsumer<B: MessageBus> {
    router: Router<B>,
    extensions: Extensions,
    hooks: Hooks<B>,
    bus: B,
}

impl<B: MessageBus> MessageConsumer<B> {
    pub fn new(bus: B) -> Self {
        Self {
            router: Router::default(),
            extensions: Extensions::default(),
            hooks: Hooks::default(),
            bus,
        }
    }

    pub fn message_handler<Fun, Args>(self, handler: Fun) -> Self
    where
        Fun: RoutableHandler<B, Args>
            + Handler<B, Args, Future = Pin<Box<dyn Future<Output = Confirmation> + Send>>>
            + 'static,
        Args: Send + Sync + 'static,
    {
        Self {
            router: self.router.message_handler(handler),
            ..self
        }
    }

    pub fn fallback_handler<Fun, Args>(self, handler: Fun) -> Self
    where
        Fun:
            Handler<B, Args, Future = Pin<Box<dyn Future<Output = Confirmation> + Send>>> + 'static,
        Args: Send + Sync + 'static,
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
        T: Hook<B>,
    {
        Self {
            hooks: self.hooks.push(hook),
            ..self
        }
    }

    pub async fn listen(
        mut self,
        config: WorkerPoolConfig,
    ) -> Result<Infallible, MessageConsumerError<B>> {
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

        let Self {
            router,
            extensions,
            hooks,
            bus,
        } = self;

        let ctx = WorkerContext::new(router, extensions, hooks);

        let cleanup_timer = config.timer();
        let mut cleanup_tick_stream = TickStream(cleanup_timer);
        let mut worker_pool: WorkerPool<B> = WorkerPool::new(config, ctx);
        let mut stream = bus
            .into_stream()
            .await
            .map_err(|err| MessageConsumerError::MessageBusError(err.into()))?;

        loop {
            tokio::select! {
                biased;
                msg = stream.next() => {
                    let msg = msg.ok_or(MessageConsumerError::MessageBusEOF)?.map_err(|err| MessageConsumerError::MessageBusError(err.into()))?;
                    worker_pool.dispatch(msg).await;
                }
                Some(tick) = cleanup_tick_stream.next() => {
                    worker_pool.do_cleanup(tick);
                }
            }
        }
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

impl Display for Confirmation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Confirmation::Ack => "ack",
            Confirmation::Nack => "nack",
            Confirmation::Reject => "reject",
        };

        f.write_str(s)
    }
}

struct TickStream(Option<Interval>);

impl Stream for TickStream {
    type Item = Instant;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(interval) = &mut self.0 {
            return match interval.poll_tick(cx) {
                Poll::Ready(i) => Poll::Ready(Some(i.into_std())),
                Poll::Pending => Poll::Pending,
            };
        }

        Poll::Ready(None)
    }
}

#[cfg(test)]
mod test {
    use crate::{
        consumer::worker::FixedPoolConfig,
        message::RawMessage,
        test::{TestMessage, TestMessageBus},
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

        let consumer = MessageConsumer::new(TestMessageBus)
            .message_handler(message_handler_missing_states)
            .fallback_handler(fallback_handler_missing_states);

        let _error = consumer
            .listen(WorkerPoolConfig::Fixed(FixedPoolConfig {
                count: 10,
                queue_size: 128,
            }))
            .await
            .unwrap_err();
    }
}

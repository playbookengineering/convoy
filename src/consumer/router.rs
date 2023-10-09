use std::{collections::HashMap, future::Future, pin::Pin};

use crate::message::RawMessage;

use super::{
    context::ProcessContext,
    handler::{BoxedHandler, ErasedHandler, Handler, HandlerError, RoutableHandler},
    sentinel::Sentinel,
    Confirmation, MessageBus,
};

pub struct Router<B: MessageBus> {
    handlers: HashMap<&'static str, BoxedHandler<B, ()>>,
    fallback_handler: BoxedHandler<B, ()>,
    pub(crate) sentinels: Vec<Box<dyn Sentinel<B>>>,
}

impl<B: MessageBus> Default for Router<B> {
    fn default() -> Self {
        Self {
            handlers: Default::default(),
            fallback_handler: Box::new(ErasedHandler::new(|raw: RawMessage| async move {
                match raw.kind() {
                    Some(kind) => tracing::warn!("Rejecting unrouted message with key: {kind}"),
                    None => tracing::warn!("Rejecting unrouted message (no key)"),
                };

                Confirmation::Reject
            })),
            sentinels: Default::default(),
        }
    }
}

impl<Bus: MessageBus> Router<Bus> {
    pub fn message_handler<Fun, Args>(mut self, handler: Fun) -> Self
    where
        Fun: RoutableHandler<Bus, Args>
            + Handler<Bus, Args, Future = Pin<Box<dyn Future<Output = Confirmation> + Send>>>
            + 'static,
        Args: Send + Sync + 'static,
    {
        self.sentinels.extend(handler.sentinels());
        self.handlers
            .insert(handler.kind(), Box::new(ErasedHandler::new(handler)));

        self
    }

    pub fn fallback_handler<Fun, Args>(mut self, handler: Fun) -> Self
    where
        Fun: Handler<Bus, Args, Future = Pin<Box<dyn Future<Output = Confirmation> + Send>>>
            + 'static,
        Args: Send + Sync + 'static,
    {
        self.sentinels.extend(handler.sentinels());
        self.fallback_handler = Box::new(ErasedHandler::new(handler));
        self
    }

    pub async fn route(&self, ctx: &ProcessContext<'_, Bus>) -> Result<Confirmation, HandlerError> {
        let handler = ctx
            .kind()
            .and_then(|kind| self.handlers.get(kind))
            .unwrap_or(&self.fallback_handler);

        Ok(handler.call(ctx)?.await)
    }
}

#[cfg(test)]
mod test {
    use std::fmt::Display;

    use crate::{
        consumer::extension::Extension,
        test::{TestMessage, TestMessageBus},
    };

    use super::*;

    struct A;
    #[derive(Debug)]
    struct B;

    impl From<A> for Confirmation {
        fn from(_: A) -> Self {
            Confirmation::Reject
        }
    }

    impl From<B> for Confirmation {
        fn from(_: B) -> Self {
            Confirmation::Reject
        }
    }

    impl Display for B {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("B")
        }
    }

    impl std::error::Error for B {}

    #[test]
    fn accept_handler() {
        async fn handler(_: TestMessage) {}

        async fn handler_with_ext(_msg: TestMessage, _: Extension<i32>) {}

        let _ = Router::<TestMessageBus>::default()
            .message_handler(handler)
            .message_handler(handler_with_ext)
            .message_handler(|message: TestMessage| async move {
                let _ = message;
            })
            .message_handler(|message: TestMessage| async move {
                let _ = message;
                A
            })
            .message_handler(|message: TestMessage| async move {
                let _ = message;
                B
            })
            .message_handler(|message: TestMessage| async move {
                let _ = message;
                Ok::<A, B>(A)
            })
            .message_handler(|message: TestMessage| async move {
                let _ = message;
                Err::<A, B>(B)
            });
    }

    #[test]
    fn accept_fallback_handler() {
        async fn default_handler(_: RawMessage) {}
        async fn default_handler_with_ext(_: RawMessage, _: Extension<i32>) {}

        let _ = Router::<TestMessageBus>::default()
            .fallback_handler(default_handler)
            .fallback_handler(default_handler_with_ext)
            .fallback_handler(|message: RawMessage| async move {
                let _ = message;
            })
            .fallback_handler(|message: RawMessage| async move {
                let _ = message;
                A
            })
            .fallback_handler(|message: RawMessage| async move {
                let _ = message;
                B
            })
            .fallback_handler(|message: RawMessage| async move {
                let _ = message;
                Ok::<A, B>(A)
            })
            .fallback_handler(|message: RawMessage| async move {
                let _ = message;
                Err::<A, B>(B)
            });
    }
}

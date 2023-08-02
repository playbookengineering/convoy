use std::collections::HashMap;

use crate::{Message, RawMessage};

use super::{
    context::ProcessContext,
    handler::{
        AsyncFnFallbackHandler, AsyncFnMessageHandler, Handler, HandlerError, MessageHandler,
    },
    sentinel::Sentinel,
    Confirmation,
};

pub struct Router {
    handlers: HashMap<&'static str, Box<dyn Handler>>,
    fallback_handler: Box<dyn Handler>,
    pub(crate) sentinels: Vec<Box<dyn Sentinel>>,
}

impl Default for Router {
    fn default() -> Self {
        Self {
            handlers: Default::default(),
            fallback_handler: Box::new(AsyncFnFallbackHandler::from(default_fallback_handler)),
            // default_fallback_handler does not carry any sentinels
            sentinels: Default::default(),
        }
    }
}

impl Router {
    pub fn message_handler<H, Fun, Args, M>(mut self, handler: H) -> Self
    where
        Fun: 'static,
        Args: 'static,
        M: Message,
        H: Into<AsyncFnMessageHandler<Fun, Args, M>>,
        AsyncFnMessageHandler<Fun, Args, M>: MessageHandler,
    {
        let mut handler = handler.into();
        let kind = handler.kind();

        self.sentinels.extend(handler.sentinels());
        self.handlers.insert(kind, Box::new(handler));

        self
    }

    pub fn fallback_handler<H, Fun, Args>(mut self, handler: H) -> Self
    where
        Fun: 'static,
        Args: 'static,
        H: Into<AsyncFnFallbackHandler<Fun, Args>>,
        AsyncFnFallbackHandler<Fun, Args>: Handler,
    {
        let mut handler = handler.into();

        self.sentinels.extend(handler.sentinels());
        self.fallback_handler = Box::new(handler);

        self
    }

    pub async fn route(&self, ctx: &ProcessContext<'_>) -> Result<Confirmation, HandlerError> {
        let handler = ctx
            .kind()
            .and_then(|kind| self.handlers.get(kind))
            .unwrap_or(&self.fallback_handler);

        Ok(handler.call(ctx)?.await)
    }
}

async fn default_fallback_handler(_: RawMessage) {}

#[cfg(test)]
mod test {
    use crate::{consumer::extension::Extension, test::TestMessage, RawMessage};

    use super::*;

    struct A;
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

    #[test]
    fn accept_handler() {
        async fn handler(_: TestMessage) {}

        async fn handler_with_ext(_msg: TestMessage, _: Extension<i32>) {}

        let _ = Router::default()
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

        let _ = Router::default()
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

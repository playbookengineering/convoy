use std::{error::Error, pin::Pin};

use std::future::Future;

use crate::codec::Codec;

use super::context::ProcessContext;
use super::MessageBus;
use super::{Confirmation, Sentinel, TryExtract};
use crate::consumer::message_bus::extract_message;
use crate::message::{Message, RawMessage};

#[derive(thiserror::Error, Debug)]
pub enum HandlerError {
    #[error("extract error: {0}")]
    ExtractError(Box<dyn Error + Send + Sync>),

    #[error("decode error: {0}")]
    DecodeError(Box<dyn Error + Send + Sync>),
}

pub trait Handler<Bus: MessageBus, C: Codec, Args>: Send + Sync + 'static {
    type Future: Future<Output = Confirmation> + Send;

    fn call(&self, msg: &ProcessContext<'_, Bus, C>) -> Result<Self::Future, HandlerError>;

    fn sentinels(&self) -> Vec<Box<dyn Sentinel<Bus, C>>>;
}

pub trait RoutableHandler<Bus: MessageBus, C: Codec, Args>: Handler<Bus, C, Args> {
    fn kind(&self) -> &'static str;
}

pub type BoxedHandler<Bus, C, Args> =
    Box<dyn Handler<Bus, C, Args, Future = Pin<Box<dyn Future<Output = Confirmation> + Send>>>>;

pub(crate) struct ErasedHandler<
    Bus: Send + Sync + 'static,
    C: Send + Sync + 'static,
    Args: Send + Sync + 'static,
> {
    handler: BoxedHandler<Bus, C, Args>,
}

impl<Bus: MessageBus, C: Codec + Send + Sync + 'static, Args: Send + Sync + 'static>
    ErasedHandler<Bus, C, Args>
{
    pub(crate) fn new<Func>(handler: Func) -> Self
    where
        Func: Handler<Bus, C, Args, Future = Pin<Box<dyn Future<Output = Confirmation> + Send>>>
            + 'static,
    {
        Self {
            handler: Box::new(handler),
        }
    }
}

impl<Bus: MessageBus, C: Codec + Send + Sync + 'static, Args: Send + Sync + 'static, A>
    Handler<Bus, C, A> for ErasedHandler<Bus, C, Args>
{
    type Future = Pin<Box<dyn Future<Output = Confirmation> + Send>>;

    fn call(&self, msg: &ProcessContext<'_, Bus, C>) -> Result<Self::Future, HandlerError> {
        self.handler.call(msg)
    }

    fn sentinels(&self) -> Vec<Box<dyn Sentinel<Bus, C>>> {
        self.handler.sentinels()
    }
}

macro_rules! impl_async_message_handler (($($extract:ident),* ) => {
    #[allow(non_snake_case)]
    impl<Bus, C, Func, Fut, M, $($extract,)*> Handler<Bus, C, (M, $($extract,)*)> for Func
    where
        Bus: MessageBus,
        Func: Fn(M, $($extract),*) -> Fut + Send + Sync + Clone + 'static,
        Fut: Future + Send + 'static,
        Fut::Output: Into<Confirmation>,
        M: Message,
        C: Codec,
        $(
            $extract: TryExtract<Bus, C>,
        )*
    {
        type Future = Pin<Box<dyn Future<Output = Confirmation> + Send>>;

        fn call(&self, msg: &ProcessContext<'_, Bus, C>) -> Result<Self::Future, HandlerError> {
            // consider making extractors async
            $(
                let $extract = $extract::try_extract(msg).map_err(|err| HandlerError::ExtractError(Box::new(err)))?;
            )*

            let handler = self.clone();
            let message = msg.raw().clone();
            let codec = msg.codec().clone();

            Ok(Box::pin(async move {
                // message decoding has to be done asynchronously
                match extract_message::<C, Bus::IncomingMessage, M>(&message, codec).await {
                    Ok(data) => {
                        let handler = (handler)(data, $($extract,)*);
                        handler.await.into()
                    }
                    Err(err) => {
                        tracing::error!("message extraction error: {err}");

                        Confirmation::Reject
                    }
                }
            }))
        }

       #[allow(unused)]
       fn sentinels(&self) -> Vec<Box<dyn Sentinel<Bus, C>>> {
          let mut sentinels = vec![];

          $(
               sentinels.extend($extract::sentinel());
          )*

          sentinels
        }
    }

    #[allow(non_snake_case)]
    impl<Bus, C, Func, Fut, M, $($extract,)*> RoutableHandler<Bus, C, (M, $($extract,)*)> for Func
    where
        Bus: MessageBus,
        Func: Fn(M, $($extract),*) -> Fut + Send + Sync + Clone + 'static,
        Fut: Future + Send + 'static,
        Fut::Output: Into<Confirmation>,
        M: Message,
        C: Codec,
        $(
            $extract: TryExtract<Bus, C>,
        )*
    {
        fn kind(&self) -> &'static str {
            M::KIND
        }
    }
});

impl_async_message_handler! {}
impl_async_message_handler! {A}
impl_async_message_handler! {A, B}
impl_async_message_handler! {A, B, Co}
impl_async_message_handler! {A, B, Co, D}
impl_async_message_handler! {A, B, Co, D, E}
impl_async_message_handler! {A, B, Co, D, E, F}
impl_async_message_handler! {A, B, Co, D, E, F, G}
impl_async_message_handler! {A, B, Co, D, E, F, G, H}

macro_rules! impl_async_fallback_handler (($($extract:ident),* ) => {
    #[allow(non_snake_case)]
    impl<Bus, C, Func, Fut, $($extract,)*> Handler<Bus, C, (RawMessage, $($extract,)*)> for Func
    where
        Bus: MessageBus,
        Func: Fn(RawMessage, $($extract),*) -> Fut + Send + Sync + 'static,
        Fut: Future + Send + 'static,
        Fut::Output: Into<Confirmation>,
        C: Codec,
        $(
            $extract: TryExtract<Bus, C>,
        )*
    {
        type Future = Pin<Box<dyn Future<Output = Confirmation> + Send>>;

        fn call(&self, msg: &ProcessContext<'_, Bus, C>) -> Result<Self::Future, HandlerError> {
            $(
                let $extract = $extract::try_extract(msg).map_err(|err| HandlerError::ExtractError(Box::new(err)))?;
            )*

            let handler = (self)(msg.to_owned(), $($extract,)*);

            Ok(Box::pin(async move {
                handler.await.into()
            }))
        }

        #[allow(unused)]
        fn sentinels(&self) -> Vec<Box<dyn Sentinel<Bus, C>>> {
            let mut sentinels = vec![];

            $(
                sentinels.extend($extract::sentinel());
            )*

            sentinels
        }
    }
});

impl_async_fallback_handler! {}
impl_async_fallback_handler! {A}
impl_async_fallback_handler! {A, B}
impl_async_fallback_handler! {A, B, Co}
impl_async_fallback_handler! {A, B, Co, D}
impl_async_fallback_handler! {A, B, Co, D, E}
impl_async_fallback_handler! {A, B, Co, D, E, F}
impl_async_fallback_handler! {A, B, Co, D, E, F, G}
impl_async_fallback_handler! {A, B, Co, D, E, F, G, H}

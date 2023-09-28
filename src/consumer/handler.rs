use std::{error::Error, pin::Pin};

use std::future::Future;

use crate::codec::Json;

use super::context::ProcessContext;
use super::MessageBus;
use super::{Confirmation, Sentinel, TryExtract};
use crate::message::{Message, RawMessage};

#[derive(thiserror::Error, Debug)]
pub enum HandlerError {
    #[error("extract error: {0}")]
    ExtractError(Box<dyn Error + Send + Sync>),

    #[error("decode error: {0}")]
    DecodeError(Box<dyn Error + Send + Sync>),
}

pub trait Handler<Bus: MessageBus, Args>: Send + Sync + 'static {
    type Future: Future<Output = Confirmation> + Send;

    fn call(&self, msg: &ProcessContext<'_, Bus>) -> Result<Self::Future, HandlerError>;

    fn sentinels(&self) -> Vec<Box<dyn Sentinel<Bus>>>;
}

pub trait RoutableHandler<Bus: MessageBus, Args>: Handler<Bus, Args> {
    fn kind(&self) -> &'static str;
}

pub type BoxedHandler<Bus, Args> =
    Box<dyn Handler<Bus, Args, Future = Pin<Box<dyn Future<Output = Confirmation> + Send>>>>;

pub(crate) struct ErasedHandler<Bus: Send + Sync + 'static, Args: Send + Sync + 'static> {
    handler: BoxedHandler<Bus, Args>,
}

impl<Bus: MessageBus, Args: Send + Sync + 'static> ErasedHandler<Bus, Args> {
    pub(crate) fn new<Func>(handler: Func) -> Self
    where
        Func: Handler<Bus, Args, Future = Pin<Box<dyn Future<Output = Confirmation> + Send>>>
            + 'static,
    {
        Self {
            handler: Box::new(handler),
        }
    }
}

impl<Bus: MessageBus, Args: Send + Sync + 'static, A> Handler<Bus, A> for ErasedHandler<Bus, Args> {
    type Future = Pin<Box<dyn Future<Output = Confirmation> + Send>>;

    fn call(&self, msg: &ProcessContext<'_, Bus>) -> Result<Self::Future, HandlerError> {
        self.handler.call(msg)
    }

    fn sentinels(&self) -> Vec<Box<dyn Sentinel<Bus>>> {
        self.handler.sentinels()
    }
}

macro_rules! impl_async_message_handler (($($extract:ident),* ) => {
    #[allow(non_snake_case)]
    impl<Bus, Func, Fut, M, $($extract,)*> Handler<Bus, (M, $($extract,)*)> for Func
    where
        Bus: MessageBus,
        Func: Fn(M, $($extract),*) -> Fut + Send + Sync + 'static,
        Fut: Future + Send + 'static,
        Fut::Output: Into<Confirmation>,
        M: Message,
        $(
            $extract: TryExtract<Bus>,
        )*
    {
        type Future = Pin<Box<dyn Future<Output = Confirmation> + Send>>;

        fn call(&self, msg: &ProcessContext<'_, Bus>) -> Result<Self::Future, HandlerError> {
            $(
                let $extract = $extract::try_extract(msg).map_err(|err| HandlerError::ExtractError(Box::new(err)))?;
            )*

            let message: M = msg.extract_message(Json).map_err(|err| HandlerError::DecodeError(Box::new(err)))?;
            let handler = (self)(message, $($extract,)*);

            Ok(Box::pin(async move {
                handler.await.into()
            }))
        }

       #[allow(unused)]
       fn sentinels(&self) -> Vec<Box<dyn Sentinel<Bus>>> {
          let mut sentinels = vec![];

          $(
               sentinels.extend($extract::sentinel());
          )*

          sentinels
        }
    }

    #[allow(non_snake_case)]
    impl<Bus, Func, Fut, M, $($extract,)*> RoutableHandler<Bus, (M, $($extract,)*)> for Func
    where
        Bus: MessageBus,
        Func: Fn(M, $($extract),*) -> Fut + Send + Sync + 'static,
        Fut: Future + Send + 'static,
        Fut::Output: Into<Confirmation>,
        M: Message,
        $(
            $extract: TryExtract<Bus>,
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
impl_async_message_handler! {A, B, C}
impl_async_message_handler! {A, B, C, D}
impl_async_message_handler! {A, B, C, D, E}
impl_async_message_handler! {A, B, C, D, E, F}
impl_async_message_handler! {A, B, C, D, E, F, G}
impl_async_message_handler! {A, B, C, D, E, F, G, H}

macro_rules! impl_async_fallback_handler (($($extract:ident),* ) => {
    #[allow(non_snake_case)]
    impl<Bus, Func, Fut, $($extract,)*> Handler<Bus, (RawMessage, $($extract,)*)> for Func
    where
        Bus: MessageBus,
        Func: Fn(RawMessage, $($extract),*) -> Fut + Send + Sync + 'static,
        Fut: Future + Send + 'static,
        Fut::Output: Into<Confirmation>,
        $(
            $extract: TryExtract<Bus>,
        )*
    {
        type Future = Pin<Box<dyn Future<Output = Confirmation> + Send>>;

        fn call(&self, msg: &ProcessContext<'_, Bus>) -> Result<Self::Future, HandlerError> {
            $(
                let $extract = $extract::try_extract(msg).map_err(|err| HandlerError::ExtractError(Box::new(err)))?;
            )*

            let handler = (self)(msg.to_owned(), $($extract,)*);

            Ok(Box::pin(async move {
                handler.await.into()
            }))
        }

        #[allow(unused)]
        fn sentinels(&self) -> Vec<Box<dyn Sentinel<Bus>>> {
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
impl_async_fallback_handler! {A, B, C}
impl_async_fallback_handler! {A, B, C, D}
impl_async_fallback_handler! {A, B, C, D, E}
impl_async_fallback_handler! {A, B, C, D, E, F}
impl_async_fallback_handler! {A, B, C, D, E, F, G}
impl_async_fallback_handler! {A, B, C, D, E, F, G, H}

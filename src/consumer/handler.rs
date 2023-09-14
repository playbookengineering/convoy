use std::{error::Error, pin::Pin};

use serde::de::DeserializeOwned;
use std::future::Future;

use crate::codec::Json;

use super::context::ProcessContext;
use super::{Confirmation, Sentinel, TryExtract};
use crate::message::{FromMessageParts, Message, RawMessage, TryFromRawHeaders};

#[derive(thiserror::Error, Debug)]
pub enum HandlerError {
    #[error("extract error: {0}")]
    ExtractError(Box<dyn Error + Send + Sync>),

    #[error("decode error: {0}")]
    DecodeError(Box<dyn Error + Send + Sync>),
}

pub trait Handler<Args>: Send + Sync + 'static {
    type Future: Future<Output = Confirmation> + Send;

    fn call(&self, msg: &ProcessContext<'_>) -> Result<Self::Future, HandlerError>;

    fn sentinels(&self) -> Vec<Box<dyn Sentinel>>;
}

pub trait RoutableHandler<Args>: Handler<Args> {
    fn kind(&self) -> &'static str;
}

pub type BoxedHandler<Args> =
    Box<dyn Handler<Args, Future = Pin<Box<dyn Future<Output = Confirmation> + Send>>>>;

pub(crate) struct ErasedHandler<Args: Send + Sync + 'static> {
    handler: BoxedHandler<Args>,
}

impl<Args: Send + Sync + 'static> ErasedHandler<Args> {
    pub(crate) fn new<Func>(handler: Func) -> Self
    where
        Func: Handler<Args, Future = Pin<Box<dyn Future<Output = Confirmation> + Send>>> + 'static,
    {
        Self {
            handler: Box::new(handler),
        }
    }
}

impl<Args: Send + Sync + 'static, A> Handler<A> for ErasedHandler<Args> {
    type Future = Pin<Box<dyn Future<Output = Confirmation> + Send>>;

    fn call(&self, msg: &ProcessContext<'_>) -> Result<Self::Future, HandlerError> {
        self.handler.call(msg)
    }

    fn sentinels(&self) -> Vec<Box<dyn Sentinel>> {
        self.handler.sentinels()
    }
}

macro_rules! impl_async_message_handler (($($extract:ident),* ) => {
    #[allow(non_snake_case)]
    impl<Func, Fut, M, $($extract,)*> Handler<(M, $($extract,)*)> for Func
    where
        Func: Fn(M, $($extract),*) -> Fut + Send + Sync + 'static,
        Fut: Future + Send + 'static,
        Fut::Output: Into<Confirmation>,
        M: Message + FromMessageParts,
        M::Body: DeserializeOwned,
        M::Headers: TryFromRawHeaders,
        $(
            $extract: TryExtract,
        )*
    {
        type Future = Pin<Box<dyn Future<Output = Confirmation> + Send>>;

        fn call(&self, msg: &ProcessContext<'_>) -> Result<Self::Future, HandlerError> {
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
        fn sentinels(&self) -> Vec<Box<dyn Sentinel>> {
            let mut sentinels = vec![];

            $(
                sentinels.extend($extract::sentinel());
            )*

            sentinels
        }
    }

    #[allow(non_snake_case)]
    impl<Func, Fut, M, $($extract,)*> RoutableHandler<(M, $($extract,)*)> for Func
    where
        Func: Fn(M, $($extract),*) -> Fut + Send + Sync + 'static,
        Fut: Future + Send + 'static,
        Fut::Output: Into<Confirmation>,
        M: Message + FromMessageParts,
        M::Body: DeserializeOwned,
        M::Headers: TryFromRawHeaders,
        $(
            $extract: TryExtract,
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
    impl<Func, Fut, $($extract,)*> Handler<(RawMessage, $($extract,)*)> for Func
    where
        Func: Fn(RawMessage, $($extract),*) -> Fut + Send + Sync + 'static,
        Fut: Future + Send + 'static,
        Fut::Output: Into<Confirmation>,
        $(
            $extract: TryExtract,
        )*
    {
        type Future = Pin<Box<dyn Future<Output = Confirmation> + Send>>;

        fn call(&self, msg: &ProcessContext<'_>) -> Result<Self::Future, HandlerError> {
            $(
                let $extract = $extract::try_extract(msg).map_err(|err| HandlerError::ExtractError(Box::new(err)))?;
            )*

            let handler = (self)(msg.to_owned(), $($extract,)*);

            Ok(Box::pin(async move {
                handler.await.into()
            }))
        }

        #[allow(unused)]
        fn sentinels(&self) -> Vec<Box<dyn Sentinel>> {
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

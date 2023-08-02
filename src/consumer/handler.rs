use std::error::Error;
use std::{future::Future, marker::PhantomData, pin::Pin};

use super::context::ProcessContext;
use super::{extract::TryExtract, sentinel::Sentinel, Confirmation};

use crate::codec::Json;
use crate::{Message, RawMessage};

pub type HandlerFuture = Pin<Box<dyn Future<Output = Confirmation> + Send + 'static>>;

#[derive(Debug, thiserror::Error)]
pub enum HandlerError {
    #[error("extract error: {0}")]
    ExtractError(Box<dyn Error + Send + Sync>),

    #[error("decode error: {0}")]
    DecodeError(Box<dyn Error + Send + Sync>),
}

pub trait Handler: Send + Sync + 'static {
    fn call(&self, msg: &ProcessContext<'_>) -> Result<HandlerFuture, HandlerError>;

    fn sentinels(&mut self) -> Vec<Box<dyn Sentinel>>;
}

pub trait MessageHandler: Handler {
    fn kind(&self) -> &'static str;
}

pub struct AsyncFnMessageHandler<Fun, Args, M> {
    fun: Fun,
    message: PhantomData<M>,
    args: PhantomData<Args>,
    sentinels: Vec<Box<dyn Sentinel>>,
}

pub struct AsyncFnFallbackHandler<Fun, Args> {
    fun: Fun,
    args: PhantomData<Args>,
    sentinels: Vec<Box<dyn Sentinel>>,
}

macro_rules! impl_async_message_handler {
    ($($extract: ident),*) => {
        #[allow(non_snake_case)]
        impl<Fun, Fut, M, $($extract,)*> Handler for AsyncFnMessageHandler<Fun, (Fut, $($extract,)*), M>
        where
            M: Message,
            Fun: Fn(M, $($extract,)*) -> Fut + Send + Sync + 'static,
            Fut: Future + Send + Sync + 'static,
            Fut::Output: Into<Confirmation>,
            $(
                $extract: TryExtract + Send + Sync + 'static,
                <$extract as TryExtract>::Error: Send + Sync + 'static,
            )*
        {
            fn call(&self, msg: &ProcessContext<'_>) -> Result<HandlerFuture, HandlerError> {
                $(
                    let $extract = $extract::try_extract(&msg).map_err(|err| HandlerError::ExtractError(Box::new(err)))?;
                )*

                let message: M = msg.extract_message(Json).map_err(|err| HandlerError::DecodeError(Box::new(err)))?;

                let future = (self.fun)(message, $($extract,)*);

                Ok(Box::pin(async move { future.await.into() }))
            }

            fn sentinels(&mut self) -> Vec<Box<dyn Sentinel>> {
                std::mem::take(&mut self.sentinels)
            }
        }

        impl<Fun, Fut, M, $($extract,)*> From<Fun> for AsyncFnMessageHandler<Fun, (Fut, $($extract,)*), M>
        where
            M: Message,
            Fun: Fn(M, $($extract,)*) -> Fut + Send + Sync + 'static,
            Fut: Future + Send + Sync + 'static,
            Fut::Output: Into<Confirmation>,
            $(
                $extract: TryExtract,
            )*
        {

            #[allow(unused)]
            fn from(fun: Fun) -> Self {
                let mut sentinels: Vec<Box<dyn Sentinel>> = vec![];

                $(
                    sentinels.extend($extract::sentinel());
                )*

                Self {
                    fun,
                    args: PhantomData,
                    message: PhantomData,
                    sentinels,
                }
            }
        }

        impl<Fun, Fut, M, $($extract,)*> MessageHandler for AsyncFnMessageHandler<Fun, (Fut, $($extract,)*), M>
        where
            M: Message,
            Fun: Fn(M, $($extract,)*) -> Fut + Send + Sync + 'static,
            Fut: Future + Send + Sync + 'static,
            Fut::Output: Into<Confirmation>,
            $(
                $extract: TryExtract + Send + Sync + 'static,
                <$extract as TryExtract>::Error: Send + Sync + 'static,
            )*
        {
            fn kind(&self) -> &'static str {
                M::KIND
            }
        }
    };
}

impl_async_message_handler!();
impl_async_message_handler!(E1);
impl_async_message_handler!(E1, E2);
impl_async_message_handler!(E1, E2, E3);
impl_async_message_handler!(E1, E2, E3, E4);
impl_async_message_handler!(E1, E2, E3, E4, E5);
impl_async_message_handler!(E1, E2, E3, E4, E5, E6);
impl_async_message_handler!(E1, E2, E3, E4, E5, E6, E7);
impl_async_message_handler!(E1, E2, E3, E4, E5, E6, E7, E8);
impl_async_message_handler!(E1, E2, E3, E4, E5, E6, E7, E8, E9);

macro_rules! impl_async_fallback_handler {
    ($($extract: ident),*) => {
        #[allow(non_snake_case)]
        impl<Fun, Fut, $($extract,)*> Handler for AsyncFnFallbackHandler<Fun, (Fut, $($extract,)*)>
        where
            Fun: Fn(RawMessage, $($extract,)*) -> Fut + Send + Sync + 'static,
            Fut: Future + Send + Sync + 'static,
            Fut::Output: Into<Confirmation>,
            $(
                $extract: TryExtract + Send + Sync + 'static,
                <$extract as TryExtract>::Error: Send + Sync + 'static,
            )*
        {
            fn call(&self, msg: &ProcessContext<'_>) -> Result<HandlerFuture, HandlerError> {
                $(
                    let $extract = $extract::try_extract(&msg).map_err(|err| HandlerError::ExtractError(Box::new(err)))?;
                )*

                let msg = msg.to_owned();
                let future = (self.fun)(msg, $($extract,)*);

                Ok(Box::pin(async move { future.await.into() }))
            }

            fn sentinels(&mut self) -> Vec<Box<dyn Sentinel>> {
                std::mem::take(&mut self.sentinels)
            }
        }

        impl<Fun, Fut, $($extract,)*> From<Fun> for AsyncFnFallbackHandler<Fun, (Fut, $($extract,)*)>
        where
            Fun: Fn(RawMessage, $($extract,)*) -> Fut + Send + Sync + 'static,
            Fut: Future + Send + 'static,
            Fut::Output: Into<Confirmation>,
            $(
                $extract: TryExtract,
            )*
        {
            #[allow(unused)]
            fn from(fun: Fun) -> Self {
                let mut sentinels: Vec<Box<dyn Sentinel>> = vec![];

                $(
                    sentinels.extend($extract::sentinel());
                )*

                Self {
                    fun,
                    args: PhantomData,
                    sentinels,
                }
            }
        }
    };
}

impl_async_fallback_handler!();
impl_async_fallback_handler!(E1);
impl_async_fallback_handler!(E1, E2);
impl_async_fallback_handler!(E1, E2, E3);
impl_async_fallback_handler!(E1, E2, E3, E4);
impl_async_fallback_handler!(E1, E2, E3, E4, E5);
impl_async_fallback_handler!(E1, E2, E3, E4, E5, E6);
impl_async_fallback_handler!(E1, E2, E3, E4, E5, E6, E7);
impl_async_fallback_handler!(E1, E2, E3, E4, E5, E6, E7, E8);
impl_async_fallback_handler!(E1, E2, E3, E4, E5, E6, E7, E8, E9);

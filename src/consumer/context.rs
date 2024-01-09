use std::{
    any::{Any, TypeId},
    collections::HashMap,
    sync::Arc,
};

use crate::{
    codec::Codec,
    message::{RawHeaders, RawMessage, KIND_HEADER},
};

use super::{Extensions, MessageBus};
use crate::consumer::message_bus::IncomingMessage;

use thiserror::Error;

pub struct ProcessContext<'a, B: MessageBus, C: Codec> {
    message: Arc<B::IncomingMessage>,
    extensions: &'a Extensions,
    cache: &'a mut LocalCache,
    codec: &'a C,
}

#[derive(Debug, Error)]
pub enum MessageConvertError {
    #[error("Codec error: {0}")]
    Codec(Box<dyn std::error::Error + Send + Sync>),

    #[error("Headers decode error: {0}")]
    Headers(Box<dyn std::error::Error + Send + Sync>),
}

impl<'a, B: MessageBus, C: Codec> ProcessContext<'a, B, C> {
    pub fn new(
        message: Arc<B::IncomingMessage>,
        extensions: &'a Extensions,
        cache: &'a mut LocalCache,
        codec: &'a C,
    ) -> Self {
        Self {
            message,
            extensions,
            cache,
            codec,
        }
    }

    pub fn kind(&self) -> Option<&str> {
        self.message
            .headers()
            .get(KIND_HEADER)
            .map(|hdr| hdr.as_str())
    }

    pub fn headers(&self) -> &RawHeaders {
        self.message.headers()
    }

    pub fn payload(&self) -> &[u8] {
        self.message.payload()
    }

    pub fn key(&self) -> Option<&[u8]> {
        self.message.key()
    }

    pub fn codec(&self) -> &C {
        self.codec
    }

    pub fn to_owned(&self) -> RawMessage {
        RawMessage {
            payload: self.message.payload().to_vec(),
            headers: self.message.headers().clone(),
            key: self.message.key().map(|k| k.to_vec()),
        }
    }

    pub fn raw(&self) -> Arc<B::IncomingMessage> {
        self.message.clone()
    }

    pub(crate) fn extensions(&self) -> &Extensions {
        self.extensions
    }

    pub fn cache_mut(&mut self) -> &mut LocalCache {
        self.cache
    }

    pub fn cache(&self) -> &LocalCache {
        self.cache
    }
}

#[derive(Default)]
pub struct LocalCache(HashMap<TypeId, Box<dyn Any + Send + Sync>>);

impl LocalCache {
    pub fn get<T: 'static>(&self) -> Option<&T> {
        self.0
            .get(&TypeId::of::<T>())
            .and_then(|t| t.downcast_ref())
    }

    pub fn get_mut<T: 'static>(&mut self) -> Option<&mut T> {
        self.0
            .get_mut(&TypeId::of::<T>())
            .and_then(|t| t.downcast_mut())
    }

    pub fn set<T: Send + Sync + 'static>(&mut self, value: T) {
        self.0.insert(value.type_id(), Box::new(value));
    }
}

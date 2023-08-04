use std::{
    any::{Any, TypeId},
    collections::HashMap,
};

use crate::{
    codec::Codec,
    message::{
        Message, RawHeaders, RawMessage, TryFromRawHeaders, CONTENT_TYPE_HEADER, KIND_HEADER,
    },
};

use super::Extensions;

use thiserror::Error;

pub struct ProcessContext<'a> {
    payload: &'a [u8],
    headers: RawHeaders,
    extensions: &'a Extensions,
    cache: &'a mut LocalCache,
}

#[derive(Debug, Error)]
pub enum MessageConvertError {
    #[error("Codec error: {0}")]
    Codec(Box<dyn std::error::Error + Send + Sync>),

    #[error("Headers decode error: {0}")]
    Headers(Box<dyn std::error::Error + Send + Sync>),
}

impl<'a> ProcessContext<'a> {
    pub fn new(
        payload: &'a [u8],
        headers: RawHeaders,
        extensions: &'a Extensions,
        cache: &'a mut LocalCache,
    ) -> Self {
        Self {
            payload,
            headers,
            extensions,
            cache,
        }
    }

    pub fn kind(&self) -> Option<&str> {
        self.headers.get(KIND_HEADER).map(|hdr| hdr.as_str())
    }

    pub fn content_type(&self) -> Option<&str> {
        self.headers
            .get(CONTENT_TYPE_HEADER)
            .map(|hdr| hdr.as_str())
    }

    pub fn headers(&self) -> &RawHeaders {
        &self.headers
    }

    pub fn payload(&self) -> &[u8] {
        self.payload
    }

    pub fn to_owned(&self) -> RawMessage {
        RawMessage {
            payload: self.payload.to_vec(),
            headers: self.headers.clone(),
        }
    }

    pub(crate) fn extract_message<M, C>(&self, codec: C) -> Result<M, MessageConvertError>
    where
        M: Message,
        C: Codec,
    {
        let Self {
            payload,
            headers,
            extensions: _,
            cache: _,
        } = self;

        let headers: M::Headers = TryFromRawHeaders::try_from_raw_headers(headers.clone())
            .map_err(|err| MessageConvertError::Headers(Box::new(err)))?;

        let body: M::Body = codec
            .decode(payload)
            .map_err(|err| MessageConvertError::Codec(Box::new(err)))?;

        Ok(Message::from_body_and_headers(body, headers))
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

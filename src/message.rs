use std::{collections::HashMap, error::Error};

use serde::{de::DeserializeOwned, Serialize};

pub type RawHeaders = HashMap<String, String>;

pub const CONTENT_TYPE_HEADER: &str = "x-convoy-content-type";
pub const KIND_HEADER: &str = "x-convoy-kind";

pub trait Message: Send + Sync + 'static {
    /// Message discriminator
    /// `KIND` is passed along headers by producer.
    /// This makes routing possible.
    const KIND: &'static str;

    /// Message body
    type Body: Serialize + DeserializeOwned;

    /// Additional message metadata
    type Headers: TryFromRawHeaders + Into<RawHeaders> + Send + Sync;

    /// Instructs how to construct a message from parts
    fn from_body_and_headers(body: Self::Body, headers: Self::Headers) -> Self;

    /// Instructs how to destruct a message
    fn into_body_and_headers(self) -> (Self::Body, Self::Headers);

    /// Identity key
    fn key(&self) -> String;
}

pub trait TryFromRawHeaders: Sized {
    type Error: std::error::Error + Send + Sync;

    fn try_from_raw_headers(headers: RawHeaders) -> Result<Self, Self::Error>;
}

impl<T> TryFromRawHeaders for T
where
    T: TryFrom<RawHeaders>,
    <T as TryFrom<RawHeaders>>::Error: Error + Send + Sync,
{
    type Error = T::Error;

    fn try_from_raw_headers(headers: RawHeaders) -> Result<Self, Self::Error> {
        T::try_from(headers)
    }
}

/// Represents untyped message, that can be used in fallback handler
pub struct RawMessage {
    pub payload: Vec<u8>,
    pub headers: RawHeaders,
    pub key: Option<Vec<u8>>,
}

impl RawMessage {
    /// Returns message kind if present
    pub fn kind(&self) -> Option<&str> {
        self.headers.get(KIND_HEADER).map(|x| x.as_str())
    }
}

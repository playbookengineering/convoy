use std::{collections::HashMap, error::Error};

use serde::{de::DeserializeOwned, Serialize};

pub type RawHeaders = HashMap<String, String>;

pub const CONTENT_TYPE_HEADER: &str = "x-convoy-content-type";
pub const KIND_HEADER: &str = "x-convoy-kind";

pub trait Message: Send + Sync + 'static {
    const KIND: &'static str;

    type Body: Serialize + DeserializeOwned;
    type Headers: TryFromRawHeaders + Into<RawHeaders> + Send + Sync;

    fn from_body_and_headers(body: Self::Body, headers: Self::Headers) -> Self;
    fn into_body_and_headers(self) -> (Self::Body, Self::Headers);

    fn key(&self) -> &str;
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

pub struct RawMessage {
    pub payload: Vec<u8>,
    pub headers: RawHeaders,
}

impl RawMessage {
    pub fn kind(&self) -> Option<&str> {
        self.headers.get(KIND_HEADER).map(|x| x.as_str())
    }
}

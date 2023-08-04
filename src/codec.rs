use std::error::Error;
use std::fmt::Debug;

use serde::{Deserialize, Serialize};

pub trait Codec: Debug + Send + Sync + 'static {
    type EncodeError: Error + Send + Sync + 'static;
    type DecodeError: Error + Send + Sync + 'static;

    const CONTENT_TYPE: &'static str;

    fn encode<S: Serialize>(&self, ser: S) -> Result<Vec<u8>, Self::EncodeError>;

    fn decode<'a, T>(&self, data: &'a [u8]) -> Result<T, Self::DecodeError>
    where
        T: Deserialize<'a>;
}

#[derive(Debug)]
pub struct Json;

impl Codec for Json {
    type EncodeError = serde_json::Error;
    type DecodeError = serde_json::Error;

    const CONTENT_TYPE: &'static str = "application/json";

    fn encode<S: Serialize>(&self, ser: S) -> Result<Vec<u8>, Self::EncodeError> {
        let bytes = serde_json::to_vec(&ser)?;
        Ok(bytes)
    }

    fn decode<'a, T>(&self, data: &'a [u8]) -> Result<T, Self::DecodeError>
    where
        T: Deserialize<'a>,
    {
        let value = serde_json::from_slice(data)?;

        Ok(value)
    }
}

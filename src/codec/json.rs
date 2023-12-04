use std::fmt::Debug;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use super::Codec;

#[derive(Debug, Clone)]
pub struct Json;

#[async_trait]
impl Codec for Json {
    type EncodeError = serde_json::Error;
    type DecodeError = serde_json::Error;

    async fn encode<S: Serialize + Send + Sync>(
        &self,
        ser: S,
    ) -> Result<Vec<u8>, Self::EncodeError> {
        let bytes = serde_json::to_vec(&ser)?;
        Ok(bytes)
    }

    async fn decode<'a, T>(&self, data: &'a [u8]) -> Result<T, Self::DecodeError>
    where
        T: Deserialize<'a>,
    {
        let value = serde_json::from_slice(data)?;
        Ok(value)
    }
}

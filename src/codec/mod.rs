use std::error::Error;
use std::fmt::Debug;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

mod json;
pub use json::Json;

#[cfg(feature = "avro")]
mod avro;
#[cfg(feature = "avro")]
pub use avro::{Avro, AvroRegistry};

#[async_trait]
pub trait Codec: Clone + Debug + Send + Sync + 'static {
    type EncodeError: Error + Send + Sync + 'static;
    type DecodeError: Error + Send + Sync + 'static;

    // consider spliting encode/decode

    async fn encode<S: Serialize + Send + Sync + 'static>(
        &self,
        ser: S,
    ) -> Result<Vec<u8>, Self::EncodeError>;

    async fn decode<'a, T>(&self, data: &'a [u8]) -> Result<T, Self::DecodeError>
    where
        T: Deserialize<'a> + Serialize + Send + Sync + 'static;
}

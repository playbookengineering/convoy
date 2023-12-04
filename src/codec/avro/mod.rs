use std::error::Error;
use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_avro_fast::ser::SerializerConfig;
use serde_avro_fast::Schema;

mod registry;
pub use registry::AvroRegistry;

use super::Codec;

#[derive(Debug)]
pub struct SchemaDefinition {
    pub schema: Schema,
    pub id: u32,
}

#[async_trait]
pub trait SchemaRegistry {
    type Error: Error + Send + Sync + 'static;

    async fn schema(&self, topic: &str, id: u32) -> Result<Arc<SchemaDefinition>, Self::Error>;

    async fn default_schema(&self, topic: &str) -> Result<Arc<SchemaDefinition>, Self::Error>;
}

#[derive(Debug)]
pub struct Avro<R: SchemaRegistry> {
    registry: Arc<R>,
    topic: Arc<String>,
}

impl<R> Clone for Avro<R>
where
    R: SchemaRegistry,
{
    fn clone(&self) -> Self {
        Self {
            registry: Arc::clone(&self.registry),
            topic: Arc::clone(&self.topic),
        }
    }
}

impl<R> Avro<R>
where
    R: SchemaRegistry,
{
    pub fn new(registry: R, topic: impl Into<String>) -> Self {
        Self {
            registry: Arc::new(registry),
            topic: Arc::new(topic.into()),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum AvroCodecError {
    #[error("serde error")]
    Serde(#[from] serde_json::Error),

    #[error("avro serializing error")]
    AvroSerializing(#[from] serde_avro_fast::ser::SerError),

    #[error("avro deserializing error")]
    AvroDeserializing(#[from] serde_avro_fast::de::DeError),

    #[error("registry error")]
    Registry(Box<dyn Error + Send + Sync + 'static>),

    #[error("missing avro schema id error")]
    MissingAvroSchemaId,
}

#[async_trait]
impl<R> Codec for Avro<R>
where
    R: SchemaRegistry + Debug + Send + Sync + 'static,
{
    type EncodeError = AvroCodecError;
    type DecodeError = AvroCodecError;

    async fn encode<S: Serialize + Send + Sync + 'static>(
        &self,
        ser: S,
    ) -> Result<Vec<u8>, Self::EncodeError> {
        let fetched = self
            .registry
            .default_schema(&self.topic)
            .await
            .map_err(|e| AvroCodecError::Registry(Box::new(e)))?;

        // add avro and schema version
        let mut data = vec![];
        data.push(0);
        data.extend(fetched.id.to_be_bytes());

        let mut serializer_config = SerializerConfig::new(&fetched.schema);
        serde_avro_fast::to_datum(&ser, &mut data, &mut serializer_config)?;

        Ok(data)
    }

    async fn decode<'a, T>(&self, data: &'a [u8]) -> Result<T, Self::DecodeError>
    where
        T: Deserialize<'a>,
    {
        // get schema version
        let schema_id: [u8; 4] = data[1..5]
            .try_into()
            .map_err(|_| AvroCodecError::MissingAvroSchemaId)?;
        let schema_id = u32::from_be_bytes(schema_id);

        let fetched = self
            .registry
            .schema(&self.topic, schema_id)
            .await
            .map_err(|e| AvroCodecError::Registry(Box::new(e)))?;

        serde_avro_fast::from_datum_slice::<T>(&data[5..], &fetched.schema).map_err(Into::into)
    }
}

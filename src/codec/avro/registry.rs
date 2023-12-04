use std::{str::FromStr, sync::Arc};

use ahash::HashMap;
use async_trait::async_trait;
use schema_registry_converter::{
    async_impl::schema_registry, error::SRCError, schema_registry_common::SubjectNameStrategy,
};
use serde_avro_fast::schema::ParseSchemaError;
use tokio::{sync::mpsc::Sender, task::JoinHandle};

use super::{SchemaDefinition, SchemaRegistry};

struct AvroFetch {
    topic: String,
    id: Option<u32>,
    sender: tokio::sync::oneshot::Sender<Result<Arc<SchemaDefinition>, AvroRegistryError>>,
}

#[derive(thiserror::Error, Debug)]
pub enum AvroRegistryError {
    #[error("channel error")]
    Channel,

    #[error("registry error")]
    SRC(#[from] SRCError),

    #[error("parse schema error")]
    ParseSchema(#[from] ParseSchemaError),
}

#[derive(Debug)]
pub struct AvroRegistry {
    sender: Sender<AvroFetch>,
    _handle: JoinHandle<()>,
}

impl AvroRegistry {
    pub fn new(registry: impl Into<String>) -> Self {
        let config = schema_registry::SrSettings::new(registry.into());

        let (sender, mut receiver) = tokio::sync::mpsc::channel::<AvroFetch>(10);
        let handle = tokio::task::spawn(async move {
            // todo: consider some kind of cleanup
            let mut defaults: HashMap<String, Arc<SchemaDefinition>> = HashMap::default();
            let mut versioned: HashMap<u32, Arc<SchemaDefinition>> = HashMap::default();

            tracing::debug!("schema registry worker started");

            while let Some(AvroFetch { topic, id, sender }) = receiver.recv().await {
                match id {
                    Some(id) => {
                        tracing::debug!("fetching schema id: {id}");

                        let reply = if let Some(definition) = versioned.get(&id) {
                            Ok(definition.clone())
                        } else {
                            schema_registry_converter::async_impl::schema_registry::get_schema_by_id(
                                id,
                                &config
                            )
                            .await
                            .map_err(AvroRegistryError::from)
                            .and_then(|rs| serde_avro_fast::Schema::from_str(&rs.schema).map(|schema| SchemaDefinition {
                                schema,
                                id: rs.id
                            }).map_err(AvroRegistryError::from))
                            .map(|sd| {
                                let id = sd.id;
                                let r = Arc::new(sd);
                                versioned.insert(id, r.clone());
                                r
                            })
                        };

                        let _ = sender.send(reply);
                    }
                    None => {
                        tracing::debug!("fetching default schema for: {topic}");

                        let reply: Result<Arc<SchemaDefinition>, AvroRegistryError> = if let Some(
                            default,
                        ) =
                            defaults.get(&topic)
                        {
                            Ok(default.clone())
                        } else {
                            schema_registry_converter::async_impl::schema_registry::get_schema_by_subject(
                                &config,
                                &SubjectNameStrategy::TopicNameStrategy(topic.clone(), false),
                            )
                            .await
                            .map_err(AvroRegistryError::from)
                            .and_then(|rs| serde_avro_fast::Schema::from_str(&rs.schema).map(|schema| SchemaDefinition {
                                schema,
                                id: rs.id
                            }).map_err(AvroRegistryError::from))
                            .map(|sd| {
                                let r = Arc::new(sd);
                                defaults.insert(topic, r.clone());
                                r
                            })
                        };

                        let _ = sender.send(reply);
                    }
                }
            }
        });

        Self {
            sender,
            _handle: handle,
        }
    }

    pub async fn fetch(
        &self,
        topic: &str,
        id: Option<u32>,
    ) -> Result<Arc<SchemaDefinition>, AvroRegistryError> {
        let (sender, receiver) =
            tokio::sync::oneshot::channel::<Result<Arc<SchemaDefinition>, AvroRegistryError>>();

        // request new schema
        self.sender
            .send(AvroFetch {
                topic: topic.to_string(),
                id,
                sender,
            })
            .await
            .map_err(|_| AvroRegistryError::Channel)?;

        // wait for fetched schema
        receiver.await.map_err(|_| AvroRegistryError::Channel)?
    }
}

#[async_trait]
impl SchemaRegistry for AvroRegistry {
    type Error = AvroRegistryError;

    async fn schema(&self, topic: &str, id: u32) -> Result<Arc<SchemaDefinition>, Self::Error> {
        self.fetch(topic, Some(id)).await
    }

    async fn default_schema(&self, topic: &str) -> Result<Arc<SchemaDefinition>, Self::Error> {
        self.fetch(topic, None).await
    }
}

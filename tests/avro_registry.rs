#![cfg(feature = "avro")]

use convoy::{
    codec::{Avro, AvroRegistry},
    consumer::{Confirmation, Extension, MessageConsumer, WorkerPoolConfig},
    producer::MessageProducer,
};
use fake::{Fake, Faker};
use httpmock::prelude::*;
use schema::ModelContainer;

use crate::schema::Headers;

mod in_memory;
mod schema;

async fn proxy(
    message: ModelContainer,
    sender: Extension<tokio::sync::mpsc::Sender<ModelContainer>>,
) -> Confirmation {
    sender.send(message).await.unwrap();
    Confirmation::Ack
}

#[tokio::test]
async fn message_is_serialized_and_deserialized() {
    let server = MockServer::start_async().await;

    let raw_schema = serde_json::json!({
        "type": "record",
        "name": "Transaction",
        "fields": [
          {
            "name": "id",
            "type": "string"
          },
          {
            "name": "name",
            "type": "string"
          },
          {
            "name": "production_year",
            "type": "int"
          }
        ]
    });

    let latest_fetch = server.mock(|when, then| {
        when.path("/subjects/testing-value/versions/latest");
        then.status(200).body(
            serde_json::json!({
                "subject": "testing-value",
                "version": 2,
                "id": 5,
                "schema": raw_schema.to_string()
            })
            .to_string(),
        );
    });

    let by_id_fetch: httpmock::Mock<'_> = server.mock(|when, then| {
        when.path_contains("/schemas/ids/5");
        then.status(200).body(
            serde_json::json!({
                "schema": raw_schema.to_string()
            })
            .to_string(),
        );
    });

    let registry = AvroRegistry::new(server.base_url());
    let avro = Avro::new(registry, "testing");

    let (producer1, bus1) = in_memory::make_queue();
    let producer1 = MessageProducer::builder(producer1, avro.clone()).build();

    let (sender, mut receiver) = tokio::sync::mpsc::channel::<ModelContainer>(1);

    let consumer1 = MessageConsumer::new(avro.clone())
        .message_handler(proxy)
        .extension(sender)
        .listen(bus1, WorkerPoolConfig::fixed(1));

    tokio::spawn(async move {
        consumer1.await.expect("cannot start consumer");
    });

    let model = ModelContainer(Faker.fake(), Headers);
    let expected_id = model.0.id.clone();

    producer1
        .produce(model, Default::default())
        .await
        .expect("cannot send message");

    // assert
    let data = receiver.recv().await.unwrap();
    assert_eq!(data.0.id, expected_id);
    latest_fetch.assert_hits(1);
    by_id_fetch.assert_hits(1);
}

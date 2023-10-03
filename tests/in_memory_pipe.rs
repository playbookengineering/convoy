use convoy::{
    codec::{Codec, Json},
    consumer::{Confirmation, Extension, MessageConsumer, WorkerPoolConfig},
    message::{Message, CONTENT_TYPE_HEADER, KIND_HEADER},
    producer::{MessageProducer, Producer},
};
use fake::{Fake, Faker};
use schema::ModelContainer;

use crate::{in_memory::InMemoryProducer, schema::Model};

mod in_memory;
mod schema;

async fn proxy<P: Producer, C: Codec>(
    message: ModelContainer,
    dst: Extension<MessageProducer<P, C>>,
) -> Confirmation {
    if let Err(err) = dst.produce(message, Default::default()).await {
        tracing::error!("Failed to produce message: {err}");
        Confirmation::Reject
    } else {
        Confirmation::Ack
    }
}

#[tokio::test]
async fn message_is_passed_through_pipeline() {
    // entry -> [ bus1 -> producer1 ] -> [ bus2 -> producer2 ] -> out

    let (entry, bus1) = in_memory::make_queue();
    let (producer1, bus2) = in_memory::make_queue();

    let producer1 = MessageProducer::builder(producer1, Json).build();

    // [ bus1 -> producer2 ]
    let consumer1 = MessageConsumer::new()
        .extension(producer1)
        .message_handler(proxy::<InMemoryProducer, Json>)
        .listen(bus1, WorkerPoolConfig::fixed(3));

    let (producer2, out) = in_memory::make_queue();

    let producer2 = MessageProducer::builder(producer2.clone(), Json).build();

    // [ bus2 -> producer3 ]
    let consumer2 = MessageConsumer::new()
        .extension(producer2)
        .message_handler(proxy::<InMemoryProducer, Json>)
        .listen(bus2, WorkerPoolConfig::fixed(3));

    let entry = entry.into_sender();
    let mut out = out.into_receiver();

    // spin them up (pun intended)
    tokio::spawn(consumer1);
    tokio::spawn(consumer2);

    let model_in: Model = Faker.fake();
    let raw_msg = model_in.marshal();

    entry.send(raw_msg).await.unwrap();
    let recv = out.recv().await.unwrap();

    let model_out: Model = serde_json::from_slice(&recv.payload).unwrap();

    assert_eq!(model_in, model_out);
    assert_eq!(
        recv.headers.get(KIND_HEADER).map(|x| x.as_str()),
        Some(ModelContainer::KIND)
    );
    assert_eq!(
        recv.headers.get(CONTENT_TYPE_HEADER).map(|x| x.as_str()),
        Some(Json::CONTENT_TYPE)
    );
}

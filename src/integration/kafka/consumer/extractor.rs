use std::convert::Infallible;

use rdkafka::{consumer::ConsumerContext, Message};

use crate::consumer::{ProcessContext, TryExtract};

use super::KafkaConsumer;

/// Extracts message topic
pub struct Topic(pub String);

impl<C: ConsumerContext> TryExtract<KafkaConsumer<C>> for Topic {
    type Error = Infallible;

    fn try_extract(ctx: &ProcessContext<'_, KafkaConsumer<C>>) -> Result<Self, Self::Error> {
        let topic = ctx.raw_message().message().topic().to_owned();

        Ok(Self(topic))
    }
}

/// Extracts message partition
pub struct Partition(pub i32);

impl<C: ConsumerContext> TryExtract<KafkaConsumer<C>> for Partition {
    type Error = Infallible;

    fn try_extract(ctx: &ProcessContext<'_, KafkaConsumer<C>>) -> Result<Self, Self::Error> {
        let partition = ctx.raw_message().message().partition();

        Ok(Self(partition))
    }
}

/// Extracts message offset
pub struct Offset(pub i64);

impl<C: ConsumerContext> TryExtract<KafkaConsumer<C>> for Offset {
    type Error = Infallible;

    fn try_extract(ctx: &ProcessContext<'_, KafkaConsumer<C>>) -> Result<Self, Self::Error> {
        let offset = ctx.raw_message().message().offset();

        Ok(Self(offset))
    }
}

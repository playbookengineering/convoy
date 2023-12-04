use std::convert::Infallible;

use rdkafka::{consumer::ConsumerContext, Message};

use crate::{
    codec::Codec,
    consumer::{ProcessContext, TryExtract},
};

use super::KafkaConsumer;

/// Extracts message topic
pub struct Topic(pub String);

impl<C: ConsumerContext, Co: Codec> TryExtract<KafkaConsumer<C>, Co> for Topic {
    type Error = Infallible;

    fn try_extract(ctx: &ProcessContext<'_, KafkaConsumer<C>, Co>) -> Result<Self, Self::Error> {
        let topic = ctx.raw().message().topic().to_owned();

        Ok(Self(topic))
    }
}

/// Extracts message partition
pub struct Partition(pub i32);

impl<C: ConsumerContext, Co: Codec> TryExtract<KafkaConsumer<C>, Co> for Partition {
    type Error = Infallible;

    fn try_extract(ctx: &ProcessContext<'_, KafkaConsumer<C>, Co>) -> Result<Self, Self::Error> {
        let partition = ctx.raw().message().partition();

        Ok(Self(partition))
    }
}

/// Extracts message offset
pub struct Offset(pub i64);

impl<C: ConsumerContext, Co: Codec> TryExtract<KafkaConsumer<C>, Co> for Offset {
    type Error = Infallible;

    fn try_extract(ctx: &ProcessContext<'_, KafkaConsumer<C>, Co>) -> Result<Self, Self::Error> {
        let offset = ctx.raw().message().offset();

        Ok(Self(offset))
    }
}

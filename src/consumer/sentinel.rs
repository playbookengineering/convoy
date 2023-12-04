use crate::codec::Codec;

use super::{MessageBus, MessageConsumer};

use std::fmt::Debug;

pub trait Sentinel<B: MessageBus, C: Codec>: Debug + Send + Sync + 'static {
    fn abort(&self, consumer: &MessageConsumer<B, C>) -> bool;
    fn cause(&self) -> String;
}

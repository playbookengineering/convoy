use super::{MessageBus, MessageConsumer};

use std::fmt::Debug;

pub trait Sentinel<B: MessageBus>: Debug + Send + Sync + 'static {
    fn abort(&self, consumer: &MessageConsumer<B>) -> bool;
    fn cause(&self) -> String;
}

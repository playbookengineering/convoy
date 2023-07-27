use super::MessageConsumerBuilder;

use std::fmt::Debug;

pub trait Sentinel: Debug + Send + Sync + 'static {
    fn abort(&self, consumer: &MessageConsumerBuilder) -> bool;
    fn cause(&self) -> String;
}

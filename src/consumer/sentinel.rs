use super::MessageConsumer;

use std::fmt::Debug;

pub trait Sentinel: Debug + Send + Sync + 'static {
    fn abort(&self, consumer: &MessageConsumer) -> bool;
    fn cause(&self) -> String;
}

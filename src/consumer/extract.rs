use std::error::Error;

use super::{context::ProcessContext, sentinel::Sentinel, MessageBus};

pub trait TryExtract<B: MessageBus>: Send + Sync + 'static + Sized {
    type Error: Error + Send + Sync + 'static;

    fn try_extract(message: &ProcessContext<'_, B>) -> Result<Self, Self::Error>;

    fn sentinel() -> Option<Box<dyn Sentinel<B>>> {
        None
    }
}

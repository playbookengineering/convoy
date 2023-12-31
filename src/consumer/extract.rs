use std::error::Error;

use crate::codec::Codec;

use super::{context::ProcessContext, sentinel::Sentinel, MessageBus};

pub trait TryExtract<B: MessageBus, C: Codec>: Send + Sync + 'static + Sized {
    type Error: Error + Send + Sync + 'static;

    fn try_extract(ctx: &ProcessContext<'_, B, C>) -> Result<Self, Self::Error>;

    fn sentinel() -> Option<Box<dyn Sentinel<B, C>>> {
        None
    }
}

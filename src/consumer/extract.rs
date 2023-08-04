use std::error::Error;

use super::{context::ProcessContext, sentinel::Sentinel};

pub trait TryExtract: Send + Sync + 'static + Sized {
    type Error: Error + Send + Sync + 'static;

    fn try_extract(message: &ProcessContext<'_>) -> Result<Self, Self::Error>;

    fn sentinel() -> Option<Box<dyn Sentinel>> {
        None
    }
}

use std::error::Error;

use super::{context::ProcessContext, sentinel::Sentinel};

pub trait TryExtract: Sized + 'static {
    type Error: Error;

    fn try_extract(message: &ProcessContext<'_>) -> Result<Self, Self::Error>;

    fn sentinel() -> Option<Box<dyn Sentinel>> {
        None
    }
}

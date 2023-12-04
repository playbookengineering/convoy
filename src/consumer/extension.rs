use std::{
    any::{Any, TypeId},
    collections::HashMap,
    error::Error,
    fmt::{Debug, Display},
    marker::PhantomData,
    ops::Deref,
};

use crate::codec::Codec;

use super::{extract::TryExtract, sentinel::Sentinel, MessageBus, MessageConsumer};

use super::context::ProcessContext;

pub struct Extension<T>(pub T);

impl<T> Deref for Extension<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Default)]
pub struct Extensions(HashMap<TypeId, Box<dyn Any + Send + Sync + 'static>>);

impl Extensions {
    pub fn insert<T: Send + Sync + 'static>(mut self, extension: T) -> Self {
        let typeid = extension.type_id();
        self.0.insert(typeid, Box::new(extension));
        self
    }

    pub fn get<T: 'static>(&self) -> Option<&T> {
        self.0
            .get(&TypeId::of::<T>())
            .and_then(|boxed| boxed.downcast_ref())
    }
}

#[derive(Debug)]
pub struct ExtensionExtractError(&'static str);

impl Display for ExtensionExtractError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Failed to extract extension of type {}", self.0)
    }
}

impl Error for ExtensionExtractError {}

impl<B: MessageBus, C: Codec, T: Clone + Send + Sync + 'static> TryExtract<B, C> for Extension<T> {
    type Error = ExtensionExtractError;

    fn try_extract(ctx: &ProcessContext<'_, B, C>) -> Result<Self, Self::Error> {
        let extension = ctx
            .extensions()
            .get::<T>()
            .ok_or_else(|| ExtensionExtractError(std::any::type_name::<T>()))?;

        Ok(Self(extension.clone()))
    }

    fn sentinel() -> Option<Box<dyn Sentinel<B, C>>> {
        Some(Box::new(MissingExtension::<T>(PhantomData)))
    }
}

pub struct MissingExtension<T>(PhantomData<T>);

impl<T> Debug for MissingExtension<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MissingExtension({})", std::any::type_name::<T>())
    }
}

impl<B: MessageBus, C: Codec, T: Send + Sync + 'static> Sentinel<B, C> for MissingExtension<T> {
    fn abort(&self, consumer: &MessageConsumer<B, C>) -> bool {
        consumer.extensions.get::<T>().is_none()
    }

    fn cause(&self) -> String {
        format!(
            "Type {} is not registered within extensions",
            std::any::type_name::<T>()
        )
    }
}

use std::sync::Arc;

use crate::codec::Codec;

use super::{context::ProcessContext, Confirmation, MessageBus};

pub struct Hooks<B: MessageBus, C: Codec>(Vec<Box<dyn Hook<B, C>>>);

impl<B: MessageBus, C: Codec> Default for Hooks<B, C> {
    fn default() -> Self {
        Self(Vec::default())
    }
}

impl<B: MessageBus, C: Codec> Hooks<B, C> {
    pub fn push<H: Hook<B, C>>(mut self, hook: H) -> Self {
        self.0.push(Box::new(hook));
        self
    }

    pub fn before_processing(&self, ctx: &mut ProcessContext<'_, B, C>) {
        self.0.iter().for_each(|hook| hook.before_processing(ctx));
    }

    pub fn after_processing(&self, ctx: &ProcessContext<'_, B, C>, confirmation: Confirmation) {
        self.0
            .iter()
            .for_each(|hook| hook.after_processing(ctx, confirmation));
    }
}

/// Hook is called at following phases:
///
/// - before processing
/// - after processing
pub trait Hook<B: MessageBus, C: Codec>: Send + Sync + 'static {
    fn before_processing(&self, _ctx: &mut ProcessContext<'_, B, C>) {}

    fn after_processing(&self, _ctx: &ProcessContext<'_, B, C>, _: Confirmation) {}
}

impl<T: Hook<B, C>, B: MessageBus, C: Codec> Hook<B, C> for Box<T> {
    fn before_processing(&self, ctx: &mut ProcessContext<'_, B, C>) {
        (**self).before_processing(ctx)
    }

    fn after_processing(&self, ctx: &ProcessContext<'_, B, C>, confirmation: Confirmation) {
        (**self).after_processing(ctx, confirmation)
    }
}

impl<T: Hook<B, C>, B: MessageBus, C: Codec> Hook<B, C> for Arc<T> {
    fn before_processing(&self, ctx: &mut ProcessContext<'_, B, C>) {
        (**self).before_processing(ctx)
    }

    fn after_processing(&self, ctx: &ProcessContext<'_, B, C>, confirmation: Confirmation) {
        (**self).after_processing(ctx, confirmation)
    }
}

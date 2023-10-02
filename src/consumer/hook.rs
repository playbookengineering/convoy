use std::sync::Arc;

use super::{context::ProcessContext, Confirmation, MessageBus};

pub struct Hooks<B: MessageBus>(Vec<Box<dyn Hook<B>>>);

impl<B: MessageBus> Default for Hooks<B> {
    fn default() -> Self {
        Self(Vec::default())
    }
}

impl<B: MessageBus> Hooks<B> {
    pub fn push<H: Hook<B>>(mut self, hook: H) -> Self {
        self.0.push(Box::new(hook));
        self
    }

    pub fn before_processing(&self, ctx: &mut ProcessContext<'_, B>) {
        self.0.iter().for_each(|hook| hook.before_processing(ctx));
    }

    pub fn after_processing(&self, ctx: &ProcessContext<'_, B>, confirmation: Confirmation) {
        self.0
            .iter()
            .for_each(|hook| hook.after_processing(ctx, confirmation));
    }
}

/// Hook is called at following phases:
///
/// - before processing
/// - after processing
pub trait Hook<B: MessageBus>: Send + Sync + 'static {
    fn before_processing(&self, _ctx: &mut ProcessContext<'_, B>) {}

    fn after_processing(&self, _ctx: &ProcessContext<'_, B>, _: Confirmation) {}
}

impl<T: Hook<B>, B: MessageBus> Hook<B> for Box<T> {
    fn before_processing(&self, ctx: &mut ProcessContext<'_, B>) {
        (**self).before_processing(ctx)
    }

    fn after_processing(&self, ctx: &ProcessContext<'_, B>, confirmation: Confirmation) {
        (**self).after_processing(ctx, confirmation)
    }
}

impl<T: Hook<B>, B: MessageBus> Hook<B> for Arc<T> {
    fn before_processing(&self, ctx: &mut ProcessContext<'_, B>) {
        (**self).before_processing(ctx)
    }

    fn after_processing(&self, ctx: &ProcessContext<'_, B>, confirmation: Confirmation) {
        (**self).after_processing(ctx, confirmation)
    }
}

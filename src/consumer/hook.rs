use std::sync::Arc;

use super::{context::ProcessContext, Confirmation};

#[derive(Default)]
pub struct Hooks(Vec<Box<dyn Hook>>);

impl Hooks {
    pub fn push<H: Hook>(mut self, hook: H) -> Self {
        self.0.push(Box::new(hook));
        self
    }

    pub fn before_processing(&self, ctx: &mut ProcessContext) {
        self.0.iter().for_each(|hook| hook.before_processing(ctx));
    }

    pub fn after_processing(&self, ctx: &ProcessContext, confirmation: Confirmation) {
        self.0
            .iter()
            .for_each(|hook| hook.after_processing(ctx, confirmation));
    }
}

pub trait Hook: Send + Sync + 'static {
    fn before_processing(&self, _ctx: &mut ProcessContext<'_>) {}

    fn after_processing(&self, _ctx: &ProcessContext<'_>, _: Confirmation) {}
}

impl<T: Hook> Hook for Box<T> {
    fn before_processing(&self, ctx: &mut ProcessContext<'_>) {
        (**self).before_processing(ctx)
    }

    fn after_processing(&self, ctx: &ProcessContext<'_>, confirmation: Confirmation) {
        (**self).after_processing(ctx, confirmation)
    }
}

impl<T: Hook> Hook for Arc<T> {
    fn before_processing(&self, ctx: &mut ProcessContext<'_>) {
        (**self).before_processing(ctx)
    }

    fn after_processing(&self, ctx: &ProcessContext<'_>, confirmation: Confirmation) {
        (**self).after_processing(ctx, confirmation)
    }
}

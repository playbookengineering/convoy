use super::{context::ProcessContext, Confirmation};

#[derive(Default)]
pub struct Hooks(Vec<Box<dyn Hook>>);

impl Hooks {
    pub fn push<H: Hook>(mut self, hook: H) -> Self {
        self.0.push(Box::new(hook));
        self
    }

    pub fn on_processing_start(&self, ctx: &mut ProcessContext) {
        for hook in &self.0 {
            hook.on_processing_start(ctx);
        }
    }

    pub fn on_processing_end(&self, ctx: &ProcessContext, confirmation: Confirmation) {
        for hook in &self.0 {
            hook.on_processing_end(ctx, confirmation);
        }
    }
}

pub trait Hook: Send + Sync + 'static {
    fn on_processing_start(&self, _ctx: &mut ProcessContext<'_>) {}

    fn on_processing_end(&self, _ctx: &ProcessContext<'_>, _: Confirmation) {}
}

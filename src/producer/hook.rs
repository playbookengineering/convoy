use std::sync::Arc;

use super::ProducerError;

#[derive(Default)]
pub struct Hooks(Vec<Box<dyn Hook>>);

impl Hooks {
    pub fn push<H: Hook>(mut self, hook: H) -> Self {
        self.0.push(Box::new(hook));
        self
    }

    pub fn before_send(&self) {
        self.0.iter().for_each(|hook| hook.before_send());
    }

    pub fn after_send(&self, result: &Result<(), ProducerError>) {
        self.0.iter().for_each(|hook| hook.after_send(result));
    }
}

pub trait Hook: Send + Sync + 'static {
    fn before_send(&self) {}
    fn after_send(&self, _result: &Result<(), ProducerError>) {}
}

impl<T: Hook> Hook for Box<T> {
    fn before_send(&self) {
        (**self).before_send()
    }

    fn after_send(&self, result: &Result<(), ProducerError>) {
        (**self).after_send(result)
    }
}

impl<T: Hook> Hook for Arc<T> {
    fn before_send(&self) {
        (**self).before_send()
    }

    fn after_send(&self, result: &Result<(), ProducerError>) {
        (**self).after_send(result)
    }
}

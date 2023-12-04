use std::{
    any::{Any, TypeId},
    cell::RefCell,
    collections::HashMap,
    convert::Infallible,
    marker::PhantomData,
};

use crate::codec::Codec;

use super::{context::ProcessContext, MessageBus, TryExtract};

tokio::task_local! {
    pub(crate) static TASK_LOCALS: TaskLocals;
}

#[derive(Default)]
pub struct TaskLocals(RefCell<HashMap<TypeId, Box<dyn Any + Send + 'static>>>);

pub struct TaskLocal<T>(PhantomData<T>);

impl<T: Default + Send + 'static> TaskLocal<T> {
    #[allow(unused)]
    pub fn with<F: FnOnce(&T) -> R, R>(&self, f: F) -> R {
        TASK_LOCALS.with(|task_locals| {
            let mut tls = task_locals.0.borrow_mut();

            let value = tls
                .entry(TypeId::of::<T>())
                .or_insert_with(|| Box::<T>::default());

            let value: &T = value.downcast_ref().expect("erased value must not panic");

            f(value)
        })
    }

    #[allow(unused)]
    pub fn set(&mut self, value: T) {
        Self::set_internal(value);
    }

    pub(crate) fn set_internal(value: T) {
        TASK_LOCALS.with(|task_locals| {
            let mut tls = task_locals.0.borrow_mut();

            tls.insert(TypeId::of::<T>(), Box::new(value));
        })
    }
}

impl<T: Default + Copy + Send + 'static> TaskLocal<T> {
    #[allow(unused)]
    pub fn get(&self) -> T {
        self.with(|t| *t)
    }
}

impl<B: MessageBus, C: Codec, T: Default + Send + Sync + Sized + 'static> TryExtract<B, C>
    for TaskLocal<T>
{
    type Error = Infallible;

    fn try_extract(_: &ProcessContext<'_, B, C>) -> Result<Self, Self::Error> {
        Ok(TaskLocal(PhantomData))
    }
}

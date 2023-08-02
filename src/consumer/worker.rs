use murmur2::KAFKA_SEED;
use rand::{rngs::ThreadRng, Rng};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};

use crate::{
    message_bus::{IncomingMessage, MessageBus},
    MessageConsumer,
};

use super::{
    context::{LocalCache, ProcessContext},
    extension::Extensions,
    router::Router,
    task_local::{TaskLocal, TASK_LOCALS},
    Hooks,
};

pub struct WorkerPool<B: MessageBus> {
    workers: Flavour<B>,
}

pub struct WorkerContext<B>(Arc<WorkerContextInternal<B>>);

struct WorkerContextInternal<B> {
    consumer: MessageConsumer,
    bus: B,
}

#[derive(Debug)]
pub enum WorkerPoolConfig {
    FixedPoolConfig(FixedPoolConfig),
    KeyRoutedPoolConfig,
}

#[derive(Debug)]
pub struct FixedPoolConfig {
    pub count: usize,
}

#[derive(Debug)]
pub struct KeyRoutedPoolConfig {
    pub inactivity_duration: Duration,
}

enum Flavour<B: MessageBus> {
    Fixed(Fixed<B>),
    KeyRouted(KeyRouted<B>),
}

struct Fixed<B: MessageBus> {
    workers: Vec<WorkerState<B>>,
    rng: ThreadRng,
}
impl<B: MessageBus> WorkerContext<B> {
    pub fn new(consumer: MessageConsumer, bus: B) -> Self {
        let internal = WorkerContextInternal { consumer, bus };

        Self(Arc::new(internal))
    }

    pub fn router(&self) -> &Router {
        &self.0.consumer.router
    }

    pub fn extensions(&self) -> &Extensions {
        &self.0.consumer.extensions
    }

    pub fn hooks(&self) -> &Hooks {
        &self.0.consumer.hooks
    }

    pub fn bus(&self) -> &B {
        &self.0.bus
    }
}

impl<B: MessageBus> Clone for WorkerContext<B> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl<B: MessageBus> WorkerPool<B> {
    pub fn new(config: WorkerPoolConfig, context: WorkerContext<B>) -> Self {
        match config {
            WorkerPoolConfig::FixedPoolConfig(cfg) => Self::fixed(cfg, context),
            WorkerPoolConfig::KeyRoutedPoolConfig => Self::key_routed(context),
        }
    }

    fn fixed(config: FixedPoolConfig, context: WorkerContext<B>) -> Self {
        let FixedPoolConfig { count } = config;

        assert!(count > 0, "Count must be greater than zero!");

        let workers = (0..count)
            .map(|idx| launch_worker::<B>(context.clone(), WorkerId(idx.to_string())))
            .collect();

        Self {
            workers: Flavour::Fixed(Fixed::new(workers)),
        }
    }

    fn key_routed(context: WorkerContext<B>) -> Self {
        Self {
            workers: Flavour::KeyRouted(KeyRouted::new(context)),
        }
    }

    pub fn dispatch(&mut self, message: B::IncomingMessage) {
        match &mut self.workers {
            Flavour::Fixed(f) => f.dispatch(message),
            Flavour::KeyRouted(kr) => kr.dispatch(message),
        }
    }
}

impl<B: MessageBus> Fixed<B> {
    fn new(workers: Vec<WorkerState<B>>) -> Self {
        Self {
            workers,
            rng: rand::thread_rng(),
        }
    }

    fn dispatch(&mut self, msg: B::IncomingMessage) {
        let worker_idx = match msg.key() {
            Some(key) => {
                let hash = murmur2::murmur2(key.as_bytes(), KAFKA_SEED) as usize;
                hash % self.workers.len()
            }
            None => self.rng.gen_range(0..self.workers.len()),
        };

        self.workers[worker_idx].dispatch(msg);
    }
}

pub struct KeyRouted<B: MessageBus> {
    workers: HashMap<String, WorkerState<B>>,
    fallback: WorkerState<B>,
    context: WorkerContext<B>,
}

impl<B: MessageBus> KeyRouted<B> {
    fn new(context: WorkerContext<B>) -> Self {
        let fallback = launch_worker(context.clone(), WorkerId("fallback".to_owned()));

        Self {
            workers: Default::default(),
            fallback,
            context,
        }
    }

    fn dispatch(&mut self, msg: B::IncomingMessage) {
        match msg.key() {
            Some(key) => {
                let key = key.to_string();
                let worker = self
                    .workers
                    .entry(key.clone())
                    .or_insert_with(|| launch_worker(self.context.clone(), WorkerId(key)));

                worker.dispatch(msg)
            }
            None => self.fallback.dispatch(msg),
        }
    }
}

pub struct WorkerState<B: MessageBus> {
    sender: UnboundedSender<WorkerEvent<B>>,
}

impl<B: MessageBus> WorkerState<B> {
    fn dispatch(&self, message: B::IncomingMessage) {
        self.sender
            .send(WorkerEvent::IncomingMessage(message))
            .expect("failed to send, this is a bug");
    }
}

impl<B: MessageBus> Drop for WorkerState<B> {
    fn drop(&mut self) {
        let _ = self.sender.send(WorkerEvent::Termination);
    }
}

fn launch_worker<B: MessageBus>(context: WorkerContext<B>, id: WorkerId) -> WorkerState<B> {
    let (tx, rx) = mpsc::unbounded_channel::<WorkerEvent<B>>();

    tokio::spawn(TASK_LOCALS.scope(Default::default(), worker::<B>(context, rx, id)));

    WorkerState { sender: tx }
}

async fn worker<B: MessageBus>(
    worker_context: WorkerContext<B>,
    mut receiver: UnboundedReceiver<WorkerEvent<B>>,
    id: WorkerId,
) {
    TaskLocal::<WorkerId>::set_internal(id);

    while let Some(message) = receiver.recv().await {
        let message = match message {
            WorkerEvent::IncomingMessage(msg) => msg,
            WorkerEvent::Termination => return,
        };

        let bus = worker_context.bus();
        let extensions = worker_context.extensions();
        let router = worker_context.router();

        let payload = message.payload();
        let headers = message.headers();

        let mut cache = LocalCache::default();

        let mut process_context = ProcessContext::new(payload, headers, extensions, &mut cache);

        worker_context
            .hooks()
            .on_processing_start(&mut process_context);

        let confirmation = match router.route(&process_context).await {
            Ok(confirmation) => confirmation,
            Err(err) => {
                tracing::error!("Handler error occurred: {err}");
                continue;
            }
        };

        let confirmation_store_result = match confirmation {
            super::Confirmation::Ack => bus.ack(&message).await,
            super::Confirmation::Nack => bus.nack(&message).await,
            super::Confirmation::Reject => bus.reject(&message).await,
        };

        if let Err(err) = confirmation_store_result {
            tracing::error!("Failed to store confirmation result: {err}");
        }

        worker_context
            .hooks()
            .on_processing_end(&process_context, confirmation);
    }
}

enum WorkerEvent<B: MessageBus> {
    IncomingMessage(B::IncomingMessage),
    Termination,
}

#[derive(Debug, Clone, Default)]
struct WorkerId(String);

impl WorkerId {
    #[allow(unused)]
    fn get(&self) -> &str {
        self.0.as_str()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::consumer::{Confirmation, Hook};
    use crate::test::{TestIncomingMessage, TestMessage, TestMessageBus};
    use crate::{consumer::Extension, RawHeaders, RawMessage};

    use tokio::sync::mpsc::unbounded_channel;

    async fn handler(
        message: TestMessage,
        sender: Extension<UnboundedSender<(TestMessage, WorkerId, usize)>>,
        worker_id: TaskLocal<WorkerId>,
        mut call_counter: TaskLocal<usize>,
    ) {
        let worker_id = worker_id.with(|x| x.clone());
        let counter = call_counter.get();

        sender.send((message, worker_id, counter)).unwrap();

        call_counter.set(counter + 1);
    }

    async fn fallback_handler(
        message: RawMessage,
        sender: Extension<UnboundedSender<(RawMessage, WorkerId)>>,
        worker_id: TaskLocal<WorkerId>,
    ) {
        let worker_id = worker_id.with(|x| x.clone());
        sender.send((message, worker_id)).unwrap();
    }

    fn fixed_config() -> FixedPoolConfig {
        FixedPoolConfig { count: 3 }
    }

    #[tokio::test]
    async fn fixed_pool_dispatch() {
        let (tx, mut rx) = unbounded_channel::<(TestMessage, WorkerId, usize)>();
        let consumer = MessageConsumer::default()
            .message_handler(handler)
            .extension(tx);

        let context = WorkerContext::new(consumer, TestMessageBus);

        let mut workers = WorkerPool::<TestMessageBus>::fixed(fixed_config(), context);

        let message = TestMessage::new(0);

        let incoming = TestIncomingMessage::create_raw_json(message.clone());

        workers.dispatch(incoming);
        let (processed, worker_id, _) = rx.recv().await.unwrap();

        assert_eq!(message, processed);
        assert_eq!(worker_id.get(), "0");

        let message = TestMessage::new(12);

        let incoming = TestIncomingMessage::create_raw_json(message.clone());
        workers.dispatch(incoming);
        let (processed, worker_id, _) = rx.recv().await.unwrap();

        assert_eq!(message, processed);
        assert_eq!(worker_id.get(), "1");

        let message = TestMessage::new(9);

        let incoming = TestIncomingMessage::create_raw_json(message.clone());
        workers.dispatch(incoming);
        let (processed, worker_id, _) = rx.recv().await.unwrap();

        assert_eq!(message, processed);
        assert_eq!(worker_id.get(), "2");
    }

    #[tokio::test]
    async fn fixed_pool_fallback() {
        let (tx, mut rx) = unbounded_channel::<(RawMessage, WorkerId)>();

        let consumer = MessageConsumer::default()
            .fallback_handler(fallback_handler)
            .extension(tx);

        let context = WorkerContext::new(consumer, TestMessageBus);

        let mut workers = WorkerPool::<TestMessageBus>::fixed(fixed_config(), context);

        let incoming = TestIncomingMessage {
            key: None,
            headers: Default::default(),
            payload: vec![42],
        };

        workers.dispatch(incoming);
        let (processed_raw, _) = rx.recv().await.unwrap();

        assert_eq!(processed_raw.payload, [42]);
        assert_eq!(processed_raw.headers, RawHeaders::default());
    }

    #[tokio::test]
    async fn key_routed_pool_dispatch() {
        let (tx, mut rx) = unbounded_channel::<(TestMessage, WorkerId, usize)>();

        let consumer = MessageConsumer::default()
            .message_handler(handler)
            .extension(tx);

        let context = WorkerContext::new(consumer, TestMessageBus);

        let mut workers = WorkerPool::<TestMessageBus>::key_routed(context);

        for i in 0..100 {
            let message = TestMessage::new(0);

            let incoming = TestIncomingMessage::create_raw_json(message.clone());
            workers.dispatch(incoming);
            let (processed, worker_id, call_counter) = rx.recv().await.unwrap();

            assert_eq!(message, processed);
            assert_eq!(worker_id.get(), "test0");
            assert_eq!(call_counter, i);

            let message = TestMessage::new(1);

            let incoming = TestIncomingMessage::create_raw_json(message.clone());
            workers.dispatch(incoming);
            let (processed, worker_id, call_counter) = rx.recv().await.unwrap();

            assert_eq!(message, processed);
            assert_eq!(worker_id.get(), "test1");
            assert_eq!(call_counter, i);

            let message = TestMessage::new(2);

            let incoming = TestIncomingMessage::create_raw_json(message.clone());
            workers.dispatch(incoming);
            let (processed, worker_id, call_counter) = rx.recv().await.unwrap();

            assert_eq!(message, processed);
            assert_eq!(worker_id.get(), "test2");
            assert_eq!(call_counter, i);
        }
    }

    #[tokio::test]
    async fn key_routed_pool_fallback() {
        let (tx, mut rx) = unbounded_channel::<(RawMessage, WorkerId)>();

        let consumer = MessageConsumer::default()
            .fallback_handler(fallback_handler)
            .extension(tx);

        let context = WorkerContext::new(consumer, TestMessageBus);

        let mut workers = WorkerPool::<TestMessageBus>::key_routed(context);

        let incoming = TestIncomingMessage {
            key: None,
            headers: Default::default(),
            payload: vec![42],
        };

        workers.dispatch(incoming);
        let (processed_raw, worker_id) = rx.recv().await.unwrap();

        assert_eq!(processed_raw.payload, [42]);
        assert_eq!(processed_raw.headers, RawHeaders::default());
        assert_eq!(worker_id.get(), "fallback");
    }

    #[tokio::test]
    async fn hooks_are_executed() {
        #[derive(Debug, PartialEq, Eq)]
        enum Event {
            ProcessStart,
            ProcessEnd(Confirmation),
        }

        struct TestHook(UnboundedSender<Event>);

        impl Hook for TestHook {
            fn on_processing_start(&self, _: &mut ProcessContext<'_>) {
                self.0.send(Event::ProcessStart).unwrap();
            }

            fn on_processing_end(&self, _: &ProcessContext<'_>, confirmation: Confirmation) {
                self.0.send(Event::ProcessEnd(confirmation)).unwrap();
            }
        }

        let (tx, mut rx) = unbounded_channel();

        let consumer = MessageConsumer::default().hook(TestHook(tx));

        let context = WorkerContext::new(consumer, TestMessageBus);
        let mut workers = WorkerPool::<TestMessageBus>::fixed(fixed_config(), context);

        let incoming = TestIncomingMessage {
            key: None,
            headers: Default::default(),
            payload: vec![42],
        };

        workers.dispatch(incoming);

        let event1 = rx.recv().await.unwrap();
        let event2 = rx.recv().await.unwrap();

        assert_eq!(event1, Event::ProcessStart);
        assert_eq!(event2, Event::ProcessEnd(Confirmation::Ack));
    }

    #[tokio::test]
    async fn hooks_with_cache() {
        struct TestHook(UnboundedSender<Option<Num>>);

        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        struct Num(i32);

        impl Hook for TestHook {
            fn on_processing_start(&self, req: &mut ProcessContext<'_>) {
                req.cache_mut().set(Num(42));
            }

            fn on_processing_end(&self, req: &ProcessContext<'_>, _: Confirmation) {
                let num: Option<Num> = req.cache().get().cloned();
                self.0.send(num).unwrap();
            }
        }

        let (tx, mut rx) = unbounded_channel();
        let consumer = MessageConsumer::default().hook(TestHook(tx));

        let context = WorkerContext::new(consumer, TestMessageBus);
        let mut workers = WorkerPool::<TestMessageBus>::fixed(fixed_config(), context);

        let incoming = TestIncomingMessage {
            key: None,
            headers: Default::default(),
            payload: vec![42],
        };

        workers.dispatch(incoming);

        let num = rx.recv().await.unwrap();

        assert_eq!(num, Some(Num(42)));
    }
}

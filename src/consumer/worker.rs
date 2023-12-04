use rand::{thread_rng, Rng};
use std::{
    collections::HashMap,
    fmt::Debug,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::mpsc::{self, Receiver, Sender};
use tracing::{instrument, Instrument, Span};

use crate::{codec::Codec, consumer::Confirmation};

use super::{
    context::{LocalCache, ProcessContext},
    extension::Extensions,
    router::Router,
    task_local::{TaskLocal, TASK_LOCALS},
    Hooks, IncomingMessage, MessageBus,
};

const DEFAULT_QUEUE_SIZE: usize = 128;

pub struct WorkerPool<B: MessageBus, C: Codec> {
    pool: Flavour<B, C>,
}

pub struct WorkerContext<B: MessageBus, C: Codec> {
    extensions: Arc<Extensions>,
    router: Arc<Router<B, C>>,
    hooks: Arc<Hooks<B, C>>,
    codec: Arc<C>,
}

impl<B: MessageBus, C: Codec> Clone for WorkerContext<B, C> {
    fn clone(&self) -> Self {
        Self {
            extensions: Arc::clone(&self.extensions),
            router: Arc::clone(&self.router),
            hooks: Arc::clone(&self.hooks),
            codec: Arc::clone(&self.codec),
        }
    }
}

#[derive(Debug, Clone)]
pub enum WorkerPoolConfig {
    Fixed(FixedPoolConfig),
    KeyRouted(KeyRoutedPoolConfig),
}

impl WorkerPoolConfig {
    pub fn fixed(count: usize) -> Self {
        Self::Fixed(FixedPoolConfig::new(count))
    }

    pub fn key_routed(inactivity_duration: Duration) -> Self {
        Self::KeyRouted(KeyRoutedPoolConfig {
            inactivity_duration,
            queue_size: DEFAULT_QUEUE_SIZE,
        })
    }

    pub(crate) fn timer(&self) -> Option<tokio::time::Interval> {
        match self {
            WorkerPoolConfig::Fixed(_) => None,
            WorkerPoolConfig::KeyRouted(kr_config) => {
                let duration = kr_config.inactivity_duration;

                Some(tokio::time::interval_at(
                    tokio::time::Instant::now() + duration,
                    duration,
                ))
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct FixedPoolConfig {
    pub count: usize,
    pub queue_size: usize,
}

impl FixedPoolConfig {
    pub fn new(workers_count: usize) -> Self {
        Self {
            count: workers_count,
            queue_size: DEFAULT_QUEUE_SIZE,
        }
    }

    pub fn queue_size(self, size: usize) -> Self {
        Self {
            queue_size: size,
            ..self
        }
    }
}

#[derive(Debug, Clone)]
pub struct KeyRoutedPoolConfig {
    pub inactivity_duration: Duration,
    pub queue_size: usize,
}

enum Flavour<B: MessageBus, C: Codec> {
    Fixed(Fixed<B>),
    KeyRouted(KeyRouted<B, C>),
}

struct Fixed<B: MessageBus> {
    workers: Vec<WorkerState<B>>,
    hasher: ahash::RandomState,
}

impl<B: MessageBus, C: Codec> WorkerContext<B, C> {
    pub fn new(router: Router<B, C>, extensions: Extensions, hooks: Hooks<B, C>, codec: C) -> Self {
        Self {
            extensions: Arc::new(extensions),
            router: Arc::new(router),
            hooks: Arc::new(hooks),
            codec: Arc::new(codec),
        }
    }

    pub fn router(&self) -> &Router<B, C> {
        &self.router
    }

    pub fn extensions(&self) -> &Extensions {
        &self.extensions
    }

    pub fn hooks(&self) -> &Hooks<B, C> {
        &self.hooks
    }
}

impl<B: MessageBus, C: Codec> WorkerPool<B, C> {
    pub fn new(config: WorkerPoolConfig, context: WorkerContext<B, C>) -> Self {
        let worker = match config.clone() {
            WorkerPoolConfig::Fixed(cfg) => Self::fixed(cfg, context),
            WorkerPoolConfig::KeyRouted(cfg) => Self::key_routed(cfg, context),
        };

        tracing::info!("Initialized worker with config: {config:?}");

        worker
    }

    fn fixed(config: FixedPoolConfig, context: WorkerContext<B, C>) -> Self {
        let FixedPoolConfig { count, queue_size } = config;

        assert!(count > 0, "Count must be greater than zero!");

        let workers = (0..count)
            .map(|idx| {
                launch_worker::<B, C>(context.clone(), WorkerId(idx.to_string()), queue_size)
            })
            .collect();

        Self {
            pool: Flavour::Fixed(Fixed::new(workers)),
        }
    }

    fn key_routed(cfg: KeyRoutedPoolConfig, context: WorkerContext<B, C>) -> Self {
        Self {
            pool: Flavour::KeyRouted(KeyRouted::new(cfg, context)),
        }
    }

    pub async fn dispatch(&mut self, message: B::IncomingMessage) {
        match &mut self.pool {
            Flavour::Fixed(f) => f.dispatch(message).await,
            Flavour::KeyRouted(kr) => kr.dispatch(message).await,
        }
    }

    pub fn do_cleanup(&mut self, now: Instant) {
        if let Flavour::KeyRouted(pool) = &mut self.pool {
            pool.do_cleanup(now);
        }
    }

    #[cfg(test)]
    fn set_stable_seed(&mut self) {
        match &mut self.pool {
            Flavour::Fixed(f) => f.set_stable_seed(),
            Flavour::KeyRouted(_) => unimplemented!(),
        }
    }
}

impl<B: MessageBus> Fixed<B> {
    fn new(workers: Vec<WorkerState<B>>) -> Self {
        let hasher = ahash::RandomState::default();

        Self { workers, hasher }
    }

    #[cfg(test)]
    fn set_stable_seed(&mut self) {
        self.hasher = ahash::RandomState::with_seeds(0x3038, 0x3039, 0x9394, 0x1234);
    }

    async fn dispatch(&mut self, msg: B::IncomingMessage) {
        let worker_idx = match msg.key() {
            Some(key) => {
                tracing::debug!("message key: {key:?}");
                let hash = self.hasher.hash_one(key) as usize;
                hash % self.workers.len()
            }
            None => {
                tracing::info!("message does not contain a key, fallback to rand");
                thread_rng().gen_range(0..self.workers.len())
            }
        };

        self.workers[worker_idx].dispatch(msg).await
    }
}

pub struct KeyRouted<B: MessageBus, C: Codec> {
    workers: HashMap<WorkerId, WorkerState<B>>,
    fallback: WorkerState<B>,
    context: WorkerContext<B, C>,
    cfg: KeyRoutedPoolConfig,
}

impl<B: MessageBus, C: Codec> KeyRouted<B, C> {
    fn new(cfg: KeyRoutedPoolConfig, context: WorkerContext<B, C>) -> Self {
        let fallback = launch_worker(
            context.clone(),
            WorkerId("fallback".to_owned()),
            cfg.queue_size,
        );

        Self {
            workers: Default::default(),
            fallback,
            context,
            cfg,
        }
    }

    async fn dispatch(&mut self, msg: B::IncomingMessage) {
        match msg.key() {
            Some(key) => {
                let worker_id = std::str::from_utf8(key)
                    .map(ToString::to_string)
                    .map(WorkerId)
                    .unwrap_or_else(|_| WorkerId(hex::encode(key)));

                let worker = self.workers.entry(worker_id.clone()).or_insert_with(|| {
                    launch_worker(self.context.clone(), worker_id, self.cfg.queue_size)
                });

                worker.dispatch(msg).await
            }
            None => self.fallback.dispatch(msg).await,
        }
    }

    fn do_cleanup(&mut self, now: Instant) {
        let limit = self.cfg.inactivity_duration;

        let to_remove = self
            .workers
            .iter()
            .filter_map(|(key, worker)| {
                let elapsed = now.duration_since(worker.last_received);

                (elapsed > limit).then(|| key.clone())
            })
            .collect::<Vec<_>>();

        for key in to_remove {
            if let Some(worker) = self.workers.remove(&key) {
                // dispose without further blocking
                tokio::spawn(async move { worker.dispose().await });
            }
        }
    }
}

pub struct WorkerState<B: MessageBus> {
    sender: Sender<WorkerEvent<B>>,
    last_received: Instant,
}

impl<B: MessageBus> WorkerState<B> {
    async fn dispatch(&mut self, message: B::IncomingMessage) {
        self.last_received = Instant::now();

        self.sender
            .send(WorkerEvent::IncomingMessage(message))
            .await
            .expect("failed to send, worker receiver should be alive");
    }

    async fn dispose(self) {
        self.sender
            .send(WorkerEvent::Termination)
            .await
            .expect("failed to send, worker receiver should be alive");
    }
}

enum WorkerEvent<B: MessageBus> {
    IncomingMessage(B::IncomingMessage),
    Termination,
}

impl<B: MessageBus> Debug for WorkerEvent<B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WorkerEvent::IncomingMessage(m) => f
                .debug_struct("WorkerEvent::IncomingMessage")
                .field("payload_len", &m.payload().len())
                .finish(),
            WorkerEvent::Termination => f.debug_struct("WorkerEvent::Termination").finish(),
        }
    }
}

fn launch_worker<B: MessageBus, C: Codec>(
    context: WorkerContext<B, C>,
    id: WorkerId,
    queue_size: usize,
) -> WorkerState<B> {
    let (tx, rx) = mpsc::channel(queue_size);

    tokio::spawn(TASK_LOCALS.scope(Default::default(), worker::<B, C>(context, rx, id)));

    WorkerState {
        sender: tx,
        last_received: Instant::now(),
    }
}

#[instrument(skip_all, fields(id = id.0))]
async fn worker<B: MessageBus, C: Codec>(
    worker_context: WorkerContext<B, C>,
    mut receiver: Receiver<WorkerEvent<B>>,
    id: WorkerId,
) {
    TaskLocal::<WorkerId>::set_internal(id);

    tracing::info!("Start listening");

    while let Some(event) = receiver.recv().await {
        tracing::debug!("Received event: {event:?}");

        let message = Arc::new(match event {
            WorkerEvent::IncomingMessage(m) => m,
            WorkerEvent::Termination => return,
        });

        let extensions = worker_context.extensions();
        let router = worker_context.router();

        let span = message.make_span();
        let mut cache = LocalCache::default();

        if cfg!(feature = "opentelemetry") {
            extract_otel_context(&span, message.headers());
        }

        async {
            let mut process_context = ProcessContext::new(
                message.clone(),
                extensions,
                &mut cache,
                &*worker_context.codec,
            );

            if let Some(kind) = process_context.kind() {
                Span::current().record("convoy.kind", kind);
            }

            tracing::debug!("Message: begin processing");

            worker_context
                .hooks()
                .before_processing(&mut process_context);

            let confirmation = match router.route(&process_context).await {
                Ok(confirmation) => confirmation,
                Err(err) => {
                    tracing::error!("Handler error occurred: {err}");
                    Confirmation::Reject
                }
            };

            let confirmation_store_result = match confirmation {
                Confirmation::Ack => message.ack().await,
                Confirmation::Nack => message.nack().await,
                Confirmation::Reject => message.reject().await,
            };

            if let Err(err) = confirmation_store_result {
                tracing::error!("Failed to store confirmation result: {err}");
            }

            worker_context
                .hooks()
                .after_processing(&process_context, confirmation);

            tracing::info!(
                "Message {} processed, confirmation: {}",
                process_context.kind().unwrap_or("unknown"),
                confirmation,
            );
        }
        .instrument(span)
        .await
    }
}

#[cfg(not(feature = "opentelemetry"))]
#[allow(unused)]
#[inline(always)]
fn extract_otel_context(_: &tracing::Span, _: &crate::message::RawHeaders) {}

#[cfg(feature = "opentelemetry")]
#[inline(always)]
fn extract_otel_context(span: &tracing::Span, headers: &crate::message::RawHeaders) {
    use opentelemetry::global::get_text_map_propagator;
    use tracing_opentelemetry::OpenTelemetrySpanExt;

    let parent_context = get_text_map_propagator(|propagator| propagator.extract(headers));
    span.set_parent(parent_context);
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
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
    use crate::codec::Json;
    use crate::consumer::Extension;
    use crate::consumer::{Confirmation, Hook};
    use crate::message::{RawHeaders, RawMessage};
    use crate::test::{TestIncomingMessage, TestMessage, TestMessageBus};

    use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};

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

    fn fixed_config_default() -> FixedPoolConfig {
        FixedPoolConfig {
            count: 3,
            queue_size: 128,
        }
    }

    fn fixed_config(count: usize) -> FixedPoolConfig {
        FixedPoolConfig {
            count,
            queue_size: 128,
        }
    }

    fn kr_config_default() -> KeyRoutedPoolConfig {
        KeyRoutedPoolConfig {
            inactivity_duration: Duration::from_secs(10),
            queue_size: 128,
        }
    }

    fn kr_config(duration: Duration) -> KeyRoutedPoolConfig {
        KeyRoutedPoolConfig {
            inactivity_duration: duration,
            queue_size: 128,
        }
    }

    #[tokio::test]
    async fn fixed_pool_dispatch() {
        let (tx, mut rx) = unbounded_channel::<(TestMessage, WorkerId, usize)>();

        let router = Router::<TestMessageBus, _>::default().message_handler(handler);
        let extensions = Extensions::default().insert(tx);

        let context = WorkerContext::new(router, extensions, Default::default(), Json);

        let mut workers = WorkerPool::fixed(fixed_config_default(), context);
        workers.set_stable_seed();

        let message = TestMessage::new(0);

        let incoming = TestIncomingMessage::create_raw_json(message.clone());

        workers.dispatch(incoming).await;
        let (processed, worker_id, _) = rx.recv().await.unwrap();

        assert_eq!(message, processed);
        assert_eq!(worker_id.get(), "1");

        let message = TestMessage::new(12);

        let incoming = TestIncomingMessage::create_raw_json(message.clone());
        workers.dispatch(incoming).await;
        let (processed, worker_id, _) = rx.recv().await.unwrap();

        assert_eq!(message, processed);
        assert_eq!(worker_id.get(), "0");

        let message = TestMessage::new(9);

        let incoming = TestIncomingMessage::create_raw_json(message.clone());
        workers.dispatch(incoming).await;
        let (processed, worker_id, _) = rx.recv().await.unwrap();

        assert_eq!(message, processed);
        assert_eq!(worker_id.get(), "2");
    }

    #[tokio::test]
    async fn fixed_pool_fallback() {
        let (tx, mut rx) = unbounded_channel::<(RawMessage, WorkerId)>();

        let router = Router::<TestMessageBus, _>::default().fallback_handler(fallback_handler);
        let extensions = Extensions::default().insert(tx);

        let context = WorkerContext::new(router, extensions, Default::default(), Json);

        let mut workers = WorkerPool::<TestMessageBus, _>::fixed(fixed_config_default(), context);

        let incoming = TestIncomingMessage {
            key: None,
            headers: Default::default(),
            payload: vec![42],
        };

        workers.dispatch(incoming).await;
        let (processed_raw, _) = rx.recv().await.unwrap();

        assert_eq!(processed_raw.payload, [42]);
        assert_eq!(processed_raw.headers, RawHeaders::default());
    }

    #[tokio::test]
    async fn fixed_worker_workers_are_not_cleaned_up() {
        let (tx, mut _rx) = unbounded_channel::<(TestMessage, WorkerId, usize)>();

        let extensions = Extensions::default().insert(tx);
        let router = Router::<_, Json>::default().message_handler(handler);

        let context = WorkerContext::new(router, extensions, Default::default(), Json);
        let dur = Duration::from_millis(5);
        let count = 10;
        let mut workers = WorkerPool::<TestMessageBus, Json>::fixed(fixed_config(10), context);

        let message = TestMessage::new(0);
        let incoming = TestIncomingMessage::create_raw_json(message.clone());
        workers.dispatch(incoming).await;
        tokio::time::sleep(dur * 2).await;
        workers.do_cleanup(Instant::now());

        match workers.pool {
            Flavour::KeyRouted(_) => unreachable!("fixed worker pool is used"),
            Flavour::Fixed(f) => {
                assert_eq!(f.workers.len(), count);
            }
        }
    }

    #[tokio::test]
    async fn key_routed_pool_dispatch() {
        let (tx, mut rx) = unbounded_channel::<(TestMessage, WorkerId, usize)>();

        let extensions = Extensions::default().insert(tx);
        let router = Router::default().message_handler(handler);

        let context = WorkerContext::new(router, extensions, Default::default(), Json);

        let mut workers = WorkerPool::<TestMessageBus, _>::key_routed(kr_config_default(), context);

        for i in 0..100 {
            let message = TestMessage::new(0);

            let incoming = TestIncomingMessage::create_raw_json(message.clone());
            workers.dispatch(incoming).await;
            let (processed, worker_id, call_counter) = rx.recv().await.unwrap();

            assert_eq!(message, processed);
            assert_eq!(worker_id.get(), "test0");
            assert_eq!(call_counter, i);

            let message = TestMessage::new(1);

            let incoming = TestIncomingMessage::create_raw_json(message.clone());
            workers.dispatch(incoming).await;
            let (processed, worker_id, call_counter) = rx.recv().await.unwrap();

            assert_eq!(message, processed);
            assert_eq!(worker_id.get(), "test1");
            assert_eq!(call_counter, i);

            let message = TestMessage::new(2);

            let incoming = TestIncomingMessage::create_raw_json(message.clone());
            workers.dispatch(incoming).await;
            let (processed, worker_id, call_counter) = rx.recv().await.unwrap();

            assert_eq!(message, processed);
            assert_eq!(worker_id.get(), "test2");
            assert_eq!(call_counter, i);
        }
    }

    #[tokio::test]
    async fn key_routed_pool_delete_inactive_workers() {
        let (tx, mut _rx) = unbounded_channel::<(TestMessage, WorkerId, usize)>();

        let extensions = Extensions::default().insert(tx);
        let router = Router::default().message_handler(handler);

        let context = WorkerContext::new(router, extensions, Default::default(), Json);
        let dur = Duration::from_millis(5);
        let mut workers = WorkerPool::<TestMessageBus, _>::key_routed(kr_config(dur), context);

        let message = TestMessage::new(0);
        let incoming = TestIncomingMessage::create_raw_json(message.clone());
        workers.dispatch(incoming).await;
        tokio::time::sleep(dur * 2).await;
        workers.do_cleanup(Instant::now());

        match workers.pool {
            Flavour::Fixed(_) => unreachable!("key routed pool is used"),
            Flavour::KeyRouted(kr) => assert!(
                kr.workers.is_empty(),
                "workers count: {}, expected empty",
                kr.workers.len()
            ),
        }
    }

    #[tokio::test]
    async fn key_routed_pool_recreate_workers() {
        let (tx, mut rx) = unbounded_channel::<(TestMessage, WorkerId, usize)>();

        let extensions = Extensions::default().insert(tx);
        let router = Router::default().message_handler(handler);

        let context = WorkerContext::new(router, extensions, Default::default(), Json);
        let dur = Duration::from_millis(5);
        let mut workers = WorkerPool::<TestMessageBus, _>::key_routed(kr_config(dur), context);

        let message = TestMessage::new(0);
        let incoming = TestIncomingMessage::create_raw_json(message.clone());
        workers.dispatch(incoming.clone()).await;
        tokio::time::sleep(dur * 2).await;
        workers.do_cleanup(Instant::now());

        workers.dispatch(incoming.clone()).await;

        match workers.pool {
            Flavour::Fixed(_) => unreachable!("key routed pool is used"),
            Flavour::KeyRouted(kr) => {
                assert_eq!(kr.workers.len(), 1, "expected worker to be recreated")
            }
        };

        assert_eq!(rx.recv().await.unwrap().2, 0);
        assert_eq!(
            rx.recv().await.unwrap().2,
            0,
            "previous worker was not removed (old TLS)"
        );
    }

    #[tokio::test]
    async fn key_routed_pool_fallback() {
        let (tx, mut rx) = unbounded_channel::<(RawMessage, WorkerId)>();

        let extensions = Extensions::default().insert(tx);
        let router = Router::default().fallback_handler(fallback_handler);

        let context = WorkerContext::new(router, extensions, Default::default(), Json);

        let mut workers = WorkerPool::<TestMessageBus, _>::key_routed(kr_config_default(), context);

        let incoming = TestIncomingMessage {
            key: None,
            headers: Default::default(),
            payload: vec![42],
        };

        workers.dispatch(incoming).await;
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

        impl<B: MessageBus, C: Codec> Hook<B, C> for TestHook {
            fn before_processing(&self, _: &mut ProcessContext<'_, B, C>) {
                self.0.send(Event::ProcessStart).unwrap();
            }

            fn after_processing(&self, _: &ProcessContext<'_, B, C>, confirmation: Confirmation) {
                self.0.send(Event::ProcessEnd(confirmation)).unwrap();
            }
        }

        let (tx, mut rx) = unbounded_channel();

        let hooks = Hooks::default().push(TestHook(tx));

        let context = WorkerContext::new(Router::default(), Extensions::default(), hooks, Json);

        let mut workers =
            WorkerPool::<TestMessageBus, Json>::fixed(fixed_config_default(), context);

        let incoming = TestIncomingMessage {
            key: None,
            headers: Default::default(),
            payload: vec![42],
        };

        workers.dispatch(incoming).await;

        let event1 = rx.recv().await.unwrap();
        let event2 = rx.recv().await.unwrap();

        assert_eq!(event1, Event::ProcessStart);
        assert_eq!(event2, Event::ProcessEnd(Confirmation::Reject));
    }

    #[tokio::test]
    async fn hooks_with_cache() {
        struct TestHook(UnboundedSender<Option<Num>>);

        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        struct Num(i32);

        impl<B: MessageBus, C: Codec> Hook<B, C> for TestHook {
            fn before_processing(&self, req: &mut ProcessContext<'_, B, C>) {
                req.cache_mut().set(Num(42));
            }

            fn after_processing(&self, req: &ProcessContext<'_, B, C>, _: Confirmation) {
                let num: Option<Num> = req.cache().get().cloned();
                self.0.send(num).unwrap();
            }
        }

        let (tx, mut rx) = unbounded_channel();
        let hooks = Hooks::default().push(TestHook(tx));

        let context = WorkerContext::new(Router::default(), Extensions::default(), hooks, Json);
        let mut workers = WorkerPool::<TestMessageBus, _>::fixed(fixed_config_default(), context);

        let incoming = TestIncomingMessage {
            key: None,
            headers: Default::default(),
            payload: vec![42],
        };

        workers.dispatch(incoming).await;

        let num = rx.recv().await.unwrap();

        assert_eq!(num, Some(Num(42)));
    }
}

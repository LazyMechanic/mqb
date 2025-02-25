use std::{
    future::Future, hash::Hash, hint::black_box, sync::Arc, time::Duration,
};

use criterion::{Criterion, criterion_group, criterion_main};
use mqb::{MessageQueueBroker, Subscriber};
use rand::seq::SliceRandom;
use scc::Equivalent;
use tokio::{sync::Semaphore, time::Instant};

trait Broker<T, M> {
    type Subscriber: Receiver<T, M>;

    fn send<Q>(
        &self,
        tag: &Q,
        msg: M,
    ) -> impl Future<Output = Result<(), ()>> + Send
    where
        Q: Hash + Equivalent<T> + ?Sized + Send + Sync;

    fn subscribe(&self, tag: T) -> Self::Subscriber;
}

trait Receiver<T, M> {
    fn recv(&mut self) -> impl Future<Output = Result<M, ()>> + Send;
}

impl<T, M> Broker<T, M> for MessageQueueBroker<T, M>
where
    T: Hash + Eq + Clone + Send + Sync,
    M: Send + Sync,
{
    type Subscriber = Subscriber<T, M>;

    fn send<Q>(
        &self,
        tag: &Q,
        msg: M,
    ) -> impl Future<Output = Result<(), ()>> + Send
    where
        Q: Hash + Equivalent<T> + ?Sized + Sync,
    {
        async move {
            MessageQueueBroker::send(self, tag, msg)
                .await
                .map_err(|_| ())
        }
    }

    fn subscribe(&self, tag: T) -> Self::Subscriber {
        MessageQueueBroker::subscribe(self, tag)
    }
}

impl<T, M> Receiver<T, M> for Subscriber<T, M>
where
    T: Hash + Eq + Send + Sync,
    M: Send + Sync,
{
    fn recv(&mut self) -> impl Future<Output = Result<M, ()>> + Send {
        async move { Subscriber::recv(self).await.map_err(|_| ()) }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////

async fn send_impl<const COUNT: usize, F, B>(
    iters: u64,
    mqb_builder: F,
) -> Duration
where
    F: Fn() -> B,
    B: Broker<usize, usize>,
{
    let kvs = (0..COUNT).map(|v| (v, v)).collect::<Vec<_>>();

    let mut duration = Duration::ZERO;
    for _ in 0..iters {
        let mqb = mqb_builder();
        let mut subs = Vec::with_capacity(kvs.len());

        for (k, _v) in &kvs {
            subs.push(mqb.subscribe(*k));
        }

        let start = Instant::now();
        for (k, v) in &kvs {
            black_box(mqb.send(k, *v).await.unwrap());
        }
        duration += start.elapsed();
    }
    duration
}

fn unbounded_send<const COUNT: usize>(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(num_cpus::get())
        .build()
        .unwrap();

    c.bench_function(&format!("unbounded_send_{COUNT}"), |b| {
        b.to_async(&rt).iter_custom(|iters| {
            send_impl::<COUNT, _, _>(iters, || MessageQueueBroker::unbounded())
        });
    });
}

fn bounded_send<const COUNT: usize>(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(num_cpus::get())
        .build()
        .unwrap();

    c.bench_function(&format!("bounded_send_{COUNT}"), |b| {
        b.to_async(&rt).iter_custom(|iters| {
            send_impl::<COUNT, _, _>(iters, || {
                MessageQueueBroker::bounded(COUNT)
            })
        });
    });
}

////////////////////////////////////////////////////////////////////////////////////////////////

async fn recv_impl<const COUNT: usize, F, B>(
    iters: u64,
    mqb_builder: F,
) -> Duration
where
    F: Fn() -> B,
    B: Broker<usize, usize>,
{
    let kvs = (0..COUNT).map(|v| (v, v)).collect::<Vec<_>>();

    let mut duration = Duration::ZERO;
    for _ in 0..iters {
        let mqb = mqb_builder();
        let mut subs = Vec::with_capacity(kvs.len());

        for (k, _v) in &kvs {
            subs.push(mqb.subscribe(*k));
        }

        for (k, v) in &kvs {
            mqb.send(k, *v).await.unwrap();
        }

        let start = Instant::now();
        for (k, _v) in &kvs {
            black_box(subs[*k].recv().await.unwrap());
        }
        duration += start.elapsed();
    }
    duration
}

fn unbounded_recv<const COUNT: usize>(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(num_cpus::get())
        .build()
        .unwrap();

    c.bench_function(&format!("unbounded_recv_{COUNT}"), |b| {
        b.to_async(&rt).iter_custom(|iters| {
            recv_impl::<COUNT, _, _>(iters, || MessageQueueBroker::unbounded())
        });
    });
}

fn bounded_recv<const COUNT: usize>(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(num_cpus::get())
        .build()
        .unwrap();

    c.bench_function(&format!("bounded_recv_{COUNT}"), |b| {
        b.to_async(&rt).iter_custom(|iters| {
            recv_impl::<COUNT, _, _>(iters, || {
                MessageQueueBroker::bounded(COUNT)
            })
        });
    });
}

////////////////////////////////////////////////////////////////////////////////////////////////

async fn parallel_bench<
    const WRITER_THREADS: usize,
    const TAGS: usize,
    const MESSAGES_PER_TAG: usize,
    const READERS_PER_TAG: usize,
    F,
    B,
>(
    iters: u64,
    mqb_builder: F,
) -> Duration
where
    F: Fn() -> B,
    B: Broker<usize, usize> + Send + Sync + 'static,
    <B as Broker<usize, usize>>::Subscriber: Send + 'static,
{
    let all_threads = WRITER_THREADS + TAGS * READERS_PER_TAG;

    let mut gen = rand::rng();

    let mut duration = Duration::ZERO;
    for _ in 0..iters {
        let mqb = Arc::new(mqb_builder());
        let start_notify = Arc::new(Semaphore::new(0));

        let mut tasks = Vec::with_capacity(all_threads);
        // writers
        for thread_idx in 0..WRITER_THREADS {
            let mqb = mqb.clone();
            let start_notify = start_notify.clone();
            let messages = {
                let mut msgs = Vec::with_capacity(
                    MESSAGES_PER_TAG
                        * usize::max(1, TAGS.div_ceil(WRITER_THREADS)),
                );
                for tag in
                    (0..TAGS).filter(|tag| tag % WRITER_THREADS == thread_idx)
                {
                    for msg in 0..MESSAGES_PER_TAG {
                        msgs.push((tag, msg));
                    }
                }
                msgs.shuffle(&mut gen);
                msgs
            };
            let fut = async move {
                let _permit = start_notify.acquire().await.unwrap();
                for (tag, msg) in messages {
                    mqb.send(&tag, msg).await.unwrap();
                }
            };

            tasks.push(tokio::spawn(fut));
        }

        // readers

        let messages_per_readers = {
            let single = MESSAGES_PER_TAG / READERS_PER_TAG;
            let mut remainder = MESSAGES_PER_TAG % READERS_PER_TAG;
            std::iter::from_fn(|| Some(single))
                .take(READERS_PER_TAG)
                .map(|v| {
                    if remainder > 0 {
                        remainder -= 1;
                        v + 1
                    } else {
                        v
                    }
                })
                .collect::<Vec<_>>()
        };

        for tag in 0..TAGS {
            for thread_idx in 0..READERS_PER_TAG {
                let mut sub = mqb.subscribe(tag);
                let start_notify = start_notify.clone();
                let messages_per_reader = messages_per_readers[thread_idx];
                let fut = async move {
                    let _permit = start_notify.acquire().await.unwrap();

                    for _ in 0..messages_per_reader {
                        match sub.recv().await {
                            Ok(_) => {}
                            Err(_) => return,
                        }
                    }
                };

                tasks.push(tokio::spawn(fut));
            }
        }

        let start = Instant::now();
        start_notify.add_permits(all_threads);
        let _ = futures::future::join_all(tasks).await;
        duration += start.elapsed();
    }
    duration
}

fn unbounded_parallel<
    const WRITER_THREADS: usize,
    const TAGS: usize,
    const MESSAGES_PER_TAG: usize,
    const READERS_PER_TAG: usize,
>(
    c: &mut Criterion,
) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(num_cpus::get())
        .build()
        .unwrap();

    c.bench_function(&format!("unbounded_parallel_{WRITER_THREADS}_{TAGS}_{MESSAGES_PER_TAG}_{READERS_PER_TAG}"), |b| {
        b.to_async(&rt).iter_custom(|iters| {
            parallel_bench::<
                WRITER_THREADS,
                TAGS,
                MESSAGES_PER_TAG,
                READERS_PER_TAG,
                _,
                _,
            >(iters, || MessageQueueBroker::unbounded())
        });
    });
}

fn bounded_parallel<
    const WRITER_THREADS: usize,
    const TAGS: usize,
    const MESSAGES_PER_TAG: usize,
    const READERS_PER_TAG: usize,
    const CAPACITY: usize,
>(
    c: &mut Criterion,
) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(num_cpus::get())
        .build()
        .unwrap();

    c.bench_function(&format!("bounded_parallel_{WRITER_THREADS}_{TAGS}_{MESSAGES_PER_TAG}_{READERS_PER_TAG}_{CAPACITY}"), |b| {
        b.to_async(&rt).iter_custom(|iters| {
            parallel_bench::<
                WRITER_THREADS,
                TAGS,
                MESSAGES_PER_TAG,
                READERS_PER_TAG,
                _,
                _,
            >(iters, || MessageQueueBroker::bounded(CAPACITY))
        });
    });
}

criterion_group!(
    name = benches;
    config = Criterion::default();
    targets = unbounded_send::<1>
            , unbounded_send::<10>
            , unbounded_send::<100>
            , unbounded_send::<1000>
            , unbounded_send::<10000>

            , bounded_send::<1>
            , bounded_send::<10>
            , bounded_send::<100>
            , bounded_send::<1000>
            , bounded_send::<10000>

            , unbounded_recv::<1>
            , unbounded_recv::<10>
            , unbounded_recv::<100>
            , unbounded_recv::<1000>
            , unbounded_recv::<10000>

            , bounded_recv::<1>
            , bounded_recv::<10>
            , bounded_recv::<100>
            , bounded_recv::<1000>
            , bounded_recv::<10000>

            , unbounded_parallel::<20, 1000, 100, 1>
            , unbounded_parallel::<20, 1000, 100, 2>
            , unbounded_parallel::<20, 1000, 100, 10>

            , unbounded_parallel::<20, 10000, 100, 1>
            , unbounded_parallel::<20, 10000, 100, 2>
            , unbounded_parallel::<20, 10000, 100, 10>

            , bounded_parallel::<20, 1000, 100, 1, 1000>
            , bounded_parallel::<20, 1000, 100, 2, 1000>
            , bounded_parallel::<20, 1000, 100, 10, 1000>

            , bounded_parallel::<20, 10000, 100, 1, 1000>
            , bounded_parallel::<20, 10000, 100, 2, 1000>
            , bounded_parallel::<20, 10000, 100, 10, 1000>
);
criterion_main!(benches);

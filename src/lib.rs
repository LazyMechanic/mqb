//! # mqb
//!
//! This library provides lock free in memory message queue broker.
//!
//! Similar to mpmc channels with a single queue size.
//! There are two variants of broker: bounded and unbounded.
//! The bounded variant has a limit on the number of messages that the broker can store,
//! and if this limit is reached, trying to send another message will wait
//! until a message is received from the subscriber, the limit is global for all "sub-channels".
//! An unbounded channel has an infinite capacity, so the send method will always complete immediately.
//!
//! ### Unbounded example
//! ```
//! use mqb::MessageQueueBroker;
//! use futures::future::FutureExt;
//!
//! type Tag = i32;
//! type Message = i32;
//!
//! #[tokio::main]
//! async fn main() {
//!     let mqb = MessageQueueBroker::<Tag, Message>::unbounded();
//!     let sub1 = mqb.subscribe(1);
//!     let sub2 = mqb.subscribe(2);
//!     let sub2_another = mqb.subscribe(2);
//!     let sub2_clone = sub2.clone();
//!
//!     let mut tasks = Vec::new();
//!     tasks.push(async move { assert_eq!(sub1.recv().await.unwrap(), 1) }.boxed());
//!     tasks.push(async move { assert!(sub2.recv().await.is_ok()) }.boxed());
//!     tasks.push(async move { assert!(sub2_another.recv().await.is_ok()) }.boxed());
//!     tasks.push(async move { assert!(sub2_clone.recv().await.is_ok()) }.boxed());
//!
//!     mqb.send(&1, 1).await.unwrap();
//!     mqb.send(&2, 1).await.unwrap();
//!     mqb.send(&2, 2).await.unwrap();
//!     mqb.send(&2, 3).await.unwrap();
//!     assert!(mqb.send(&3, 1).await.is_err());
//!
//!     futures::future::join_all(tasks).await;
//! }
//! ```
//!
//! ### Bounded example
//! ```
//! use mqb::MessageQueueBroker;
//! use futures::future::FutureExt;
//!
//! type Tag = i32;
//! type Message = i32;
//!
//! #[tokio::main]
//! async fn main() {
//!     let mqb = MessageQueueBroker::<Tag, Message>::bounded(2);
//!     let sub1 = mqb.subscribe(1);
//!     let sub2 = mqb.subscribe(2);
//!
//!     let mut tasks = Vec::new();
//!     tasks.push(async move { assert_eq!(sub1.recv().await.unwrap(), 1) }.boxed());
//!     tasks.push(async move { assert!(sub2.recv().await.is_ok()) }.boxed());
//!
//!     mqb.send(&1, 1).await.unwrap();
//!     mqb.send(&2, 1).await.unwrap();
//!     assert!(mqb.try_send(&2, 2).unwrap_err().is_full());
//!     assert!(mqb.try_send(&3, 1).unwrap_err().is_closed());
//!
//!     futures::future::join_all(tasks).await;
//! }
//! ```

use std::{
    fmt::{Debug, Formatter},
    future::Future,
    hash::Hash,
    marker::PhantomPinned,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll, ready},
};

use crossbeam::queue::SegQueue;
use equivalent::Equivalent;
use event_listener::{Event, EventListener, IntoNotification};
use event_listener_strategy::{EventListenerFuture, FutureWrapper, Strategy};
use futures::Stream;
use pin_project::{pin_project, pinned_drop};
use scc::hash_map::Entry;

/// Lock free in memory message queue broker for sending values between asynchronous tasks by tags.
#[derive(Debug)]
pub struct MessageQueueBroker<T: Hash + Eq, M> {
    inner: Arc<MessageQueueBrokerInner<T, M>>,
}

impl<T, M> MessageQueueBroker<T, M>
where
    T: Hash + Eq + Clone,
{
    /// Creates unbounded broker.
    pub fn unbounded() -> Self {
        Self {
            inner: Arc::new(MessageQueueBrokerInner::Unbounded(Unbounded {
                buckets: Default::default(),
                is_closed: AtomicBool::new(false),
                len: AtomicUsize::new(0),
            })),
        }
    }

    /// Creates bounded broker.
    pub fn bounded(cap: usize) -> Self {
        Self {
            inner: Arc::new(MessageQueueBrokerInner::Bounded(Bounded {
                buckets: Default::default(),
                send_notify: Default::default(),
                is_closed: AtomicBool::new(false),
                len: AtomicUsize::new(0),
                cap,
            })),
        }
    }

    /// Subscribes to the tag.
    ///
    /// If a queue with provided tag already exists, a new subscriber to it will be created,
    /// otherwise a new queue will be created.
    ///
    /// Until the queue with provided tag has been created (no one has subscribed),
    /// all attempts to send a message will fail.
    pub fn subscribe(&self, tag: T) -> Subscriber<T, M> {
        MessageQueueBrokerInner::subscribe(&self.inner, tag)
    }
}

impl<T, M> MessageQueueBroker<T, M>
where
    T: Hash + Eq,
{
    /// Closes the broker. Sends and receives will fail.
    pub fn close(&self) {
        self.inner.close();
    }

    /// Checks if the broker is closed.
    pub fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }

    /// Gets the global queue length.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Checks if the global queue is empty.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Trying to send a message with the tag.
    ///
    /// If broker is closed, or there are no any subscriber to provided tag then returns `TrySendError::Closed(_)`.
    /// If the tagged queue is full then returns `TrySendError::Full(_)`.
    ///
    /// ### Ok
    /// ```
    /// use mqb::MessageQueueBroker;
    ///
    /// let mqb = MessageQueueBroker::unbounded();
    /// let sub = mqb.subscribe(1);
    ///
    /// assert!(mqb.try_send(&1, 1).is_ok());
    /// assert_eq!(sub.try_recv().unwrap(), 1);
    /// ```
    ///
    /// ### No subscribers
    /// ```
    /// use mqb::{MessageQueueBroker, TrySendError};
    ///
    /// let mqb = MessageQueueBroker::<i32, i32>::unbounded();
    ///
    /// assert_eq!(mqb.try_send(&1, 1).unwrap_err(), TrySendError::Closed(1));
    /// ```
    ///
    /// ### Broker closed
    /// ```
    /// use mqb::{MessageQueueBroker, TrySendError};
    ///
    /// let mqb = MessageQueueBroker::unbounded();
    /// let sub = mqb.subscribe(1);
    ///
    /// mqb.close();
    /// assert_eq!(mqb.try_send(&1, 1).unwrap_err(), TrySendError::Closed(1));
    /// ```
    ///
    /// ### Queue is full
    /// ```
    /// use mqb::{MessageQueueBroker, TrySendError};
    ///
    /// let mqb = MessageQueueBroker::bounded(1);
    /// let sub = mqb.subscribe(1);
    ///
    /// assert!(mqb.try_send(&1, 1).is_ok());
    /// assert_eq!(mqb.try_send(&1, 1).unwrap_err(), TrySendError::Full(1));
    /// ```
    pub fn try_send<Q>(&self, tag: &Q, msg: M) -> Result<(), TrySendError<M>>
    where
        Q: Hash + Equivalent<T> + ?Sized,
    {
        self.inner.try_send(tag, msg)
    }

    /// Sends a message with the tag.
    ///
    /// If broker is closed, or there are no any subscriber to provided tag then returns `TrySendError::Closed(_)`.
    /// If the tagged queue is full, it will wait until a slot becomes available.
    ///
    /// ### Ok
    /// ```
    /// use mqb::MessageQueueBroker;
    ///
    /// # futures::executor::block_on(async move {
    /// let mqb = MessageQueueBroker::unbounded();
    /// let sub = mqb.subscribe(1);
    ///
    /// assert!(mqb.send(&1, 1).await.is_ok());
    /// assert_eq!(sub.recv().await.unwrap(), 1);
    /// # })
    /// ```
    ///
    /// ### No subscribers
    /// ```
    /// use mqb::{MessageQueueBroker, SendError};
    ///
    /// # futures::executor::block_on(async move {
    /// let mqb = MessageQueueBroker::<i32, i32>::unbounded();
    ///
    /// assert_eq!(mqb.send(&1, 1).await.unwrap_err(), SendError(1));
    /// # });
    /// ```
    ///
    /// ### Broker closed
    /// ```
    /// use mqb::{MessageQueueBroker, SendError};
    ///
    /// # futures::executor::block_on(async move {
    /// let mqb = MessageQueueBroker::unbounded();
    /// let sub = mqb.subscribe(1);
    ///
    /// mqb.close();
    /// assert_eq!(mqb.send(&1, 1).await.unwrap_err(), SendError(1));
    /// # });
    /// ```
    ///
    /// ### Queue is full
    /// ```
    /// use mqb::{MessageQueueBroker, TrySendError};
    ///
    /// # futures::executor::block_on(async move {
    /// let mqb = MessageQueueBroker::bounded(1);
    /// let sub = mqb.subscribe(1);
    ///
    /// assert!(mqb.send(&1, 1).await.is_ok());
    /// # async {
    /// # let _ =
    /// // Wait endlessly...
    /// mqb.send(&1, 2).await;
    /// # };
    /// # });
    /// ```
    pub fn send<'a, Q>(&'a self, tag: &'a Q, msg: M) -> Send<'a, T, M, Q>
    where
        Q: Hash + Equivalent<T> + ?Sized,
    {
        Send::new(SendInner {
            broker: self,
            msg: Some(msg),
            tag: Some(tag),
            listener: None,
            _pin: Default::default(),
        })
    }

    /// Sends a message with the tag using the blocking strategy.
    ///
    /// If broker is closed, or there are no any subscriber to provided tag then returns `TrySendError::Closed(_)`.
    /// If the tagged queue is full, it will wait until a slot becomes available.
    ///
    /// ### Ok
    /// ```
    /// use mqb::MessageQueueBroker;
    ///
    /// let mqb = MessageQueueBroker::unbounded();
    /// let sub = mqb.subscribe(1);
    ///
    /// assert!(mqb.send_blocking(&1, 1).is_ok());
    /// assert_eq!(sub.recv_blocking().unwrap(), 1);
    /// ```
    ///
    /// ### No subscribers
    /// ```
    /// use mqb::{MessageQueueBroker, SendError};
    ///
    /// let mqb = MessageQueueBroker::<i32, i32>::unbounded();
    ///
    /// assert_eq!(mqb.send_blocking(&1, 1).unwrap_err(), SendError(1));
    /// ```
    ///
    /// ### Broker closed
    /// ```
    /// use mqb::{MessageQueueBroker, SendError};
    ///
    /// let mqb = MessageQueueBroker::unbounded();
    /// let sub = mqb.subscribe(1);
    ///
    /// mqb.close();
    /// assert_eq!(mqb.send_blocking(&1, 1).unwrap_err(), SendError(1));
    /// ```
    ///
    /// ### Queue is full
    /// ```
    /// use mqb::{MessageQueueBroker, TrySendError};
    ///
    /// let mqb = MessageQueueBroker::bounded(1);
    /// let sub = mqb.subscribe(1);
    ///
    /// assert!(mqb.send_blocking(&1, 1).is_ok());
    /// # stringify!(
    /// // Wait endlessly...
    /// mqb.send_blocking(&1, 2);
    /// # );
    /// ```
    pub fn send_blocking<'a, Q>(
        &'a self,
        tag: &'a Q,
        msg: M,
    ) -> Result<(), SendError<M>>
    where
        Q: Hash + Equivalent<T> + ?Sized,
    {
        self.send(tag, msg).wait()
    }
}

impl<T, M> Clone for MessageQueueBroker<T, M>
where
    T: Hash + Eq,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T, M> Drop for MessageQueueBroker<T, M>
where
    T: Hash + Eq,
{
    fn drop(&mut self) {
        self.close();
    }
}

#[derive(Debug)]
enum MessageQueueBrokerInner<T: Hash + Eq, M> {
    Bounded(Bounded<T, M>),
    Unbounded(Unbounded<T, M>),
}

impl<T, M> MessageQueueBrokerInner<T, M>
where
    T: Hash + Eq + Clone,
{
    fn subscribe(this: &Arc<Self>, tag: T) -> Subscriber<T, M> {
        let buckets = match &**this {
            MessageQueueBrokerInner::Bounded(b) => &b.buckets,
            MessageQueueBrokerInner::Unbounded(b) => &b.buckets,
        };

        match buckets.entry_sync(tag.clone()) {
            Entry::Occupied(e) => {
                let bucket = e.get().clone();
                bucket.subs.fetch_add(1, Ordering::Release);
                Subscriber {
                    tag,
                    bucket,
                    broker: Arc::clone(this),
                    listener: None,
                    _pin: Default::default(),
                }
            }
            Entry::Vacant(e) => {
                let bucket = Arc::new(Bucket {
                    queue: Default::default(),
                    subs: AtomicUsize::new(1),
                    recv_notify: Default::default(),
                });
                e.insert_entry(bucket.clone());

                Subscriber {
                    tag,
                    bucket,
                    broker: Arc::clone(this),
                    listener: None,
                    _pin: Default::default(),
                }
            }
        }
    }
}

impl<T, M> MessageQueueBrokerInner<T, M>
where
    T: Hash + Eq,
{
    fn close(&self) {
        match self {
            MessageQueueBrokerInner::Bounded(b) => b.close(),
            MessageQueueBrokerInner::Unbounded(b) => b.close(),
        }
    }

    fn is_closed(&self) -> bool {
        match self {
            MessageQueueBrokerInner::Bounded(b) => b.is_closed(),
            MessageQueueBrokerInner::Unbounded(b) => b.is_closed(),
        }
    }

    fn len(&self) -> usize {
        match self {
            MessageQueueBrokerInner::Bounded(b) => b.len(),
            MessageQueueBrokerInner::Unbounded(b) => b.len(),
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            MessageQueueBrokerInner::Bounded(b) => b.is_empty(),
            MessageQueueBrokerInner::Unbounded(b) => b.is_empty(),
        }
    }

    fn try_send<Q>(&self, tag: &Q, msg: M) -> Result<(), TrySendError<M>>
    where
        Q: Hash + Equivalent<T> + ?Sized,
    {
        match self {
            MessageQueueBrokerInner::Bounded(b) => b.try_send(tag, msg),
            MessageQueueBrokerInner::Unbounded(b) => b.try_send(tag, msg),
        }
    }

    fn unsubscribe<Q>(&self, tag: &Q)
    where
        Q: Hash + Equivalent<T> + ?Sized,
    {
        match self {
            MessageQueueBrokerInner::Bounded(b) => b.unsubscribe(tag),
            MessageQueueBrokerInner::Unbounded(b) => b.unsubscribe(tag),
        }
    }
}

#[derive(Debug)]
struct Bounded<T: Hash + Eq, M> {
    buckets: scc::HashMap<T, Arc<Bucket<M>>>,
    send_notify: Event,
    is_closed: AtomicBool,
    len: AtomicUsize,
    cap: usize,
}

impl<T, M> Bounded<T, M>
where
    T: Hash + Eq,
{
    fn close(&self) {
        self.is_closed.store(true, Ordering::Release);
        self.buckets.iter_sync(|_, v| {
            v.recv_notify.notify(usize::MAX.additional());
            true
        });
    }

    fn is_closed(&self) -> bool {
        self.is_closed.load(Ordering::Acquire)
    }

    fn len(&self) -> usize {
        self.len.load(Ordering::Acquire)
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn try_send<Q>(&self, tag: &Q, msg: M) -> Result<(), TrySendError<M>>
    where
        Q: Hash + Equivalent<T> + ?Sized,
    {
        if self.is_closed() {
            return Err(TrySendError::Closed(msg));
        }

        let Some(bucket) = self.buckets.get_sync(tag) else {
            return Err(TrySendError::Closed(msg));
        };

        match self.try_acquire_slot() {
            Ok(_) => {
                bucket.queue.push(msg);
                bucket.recv_notify.notify(1.additional());
                Ok(())
            }
            Err(_) => Err(TrySendError::Full(msg)),
        }
    }

    fn try_acquire_slot(&self) -> Result<(), ()> {
        self.len
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |x| {
                if x < self.cap { Some(x + 1) } else { None }
            })
            .map(|_| ())
            .map_err(|_| ())
    }

    fn unsubscribe<Q>(&self, tag: &Q)
    where
        Q: Hash + Equivalent<T> + ?Sized,
    {
        let Some((_tag, bucket)) = self.buckets.remove_sync(tag) else {
            return;
        };
        self.len.fetch_sub(bucket.queue.len(), Ordering::Release);
    }
}

#[derive(Debug)]
struct Unbounded<T: Hash + Eq, M> {
    buckets: scc::HashMap<T, Arc<Bucket<M>>>,
    is_closed: AtomicBool,
    len: AtomicUsize,
}

impl<T, M> Unbounded<T, M>
where
    T: Hash + Eq,
{
    fn close(&self) {
        self.is_closed.store(true, Ordering::Release);
        self.buckets.iter_sync(|_, v| {
            v.recv_notify.notify(usize::MAX.additional());
            true
        });
    }

    fn is_closed(&self) -> bool {
        self.is_closed.load(Ordering::Acquire)
    }

    fn len(&self) -> usize {
        self.len.load(Ordering::Acquire)
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn try_send<Q>(&self, tag: &Q, msg: M) -> Result<(), TrySendError<M>>
    where
        Q: Hash + Equivalent<T> + ?Sized,
    {
        if self.is_closed() {
            return Err(TrySendError::Closed(msg));
        }

        let Some(bucket) = self.buckets.get_sync(tag) else {
            return Err(TrySendError::Closed(msg));
        };

        self.len.fetch_add(1, Ordering::Release);
        bucket.queue.push(msg);
        bucket.recv_notify.notify(1.additional());
        Ok(())
    }

    fn unsubscribe<Q>(&self, tag: &Q)
    where
        Q: Hash + Equivalent<T> + ?Sized,
    {
        let Some((_tag, bucket)) = self.buckets.remove_sync(tag) else {
            return;
        };
        self.len.fetch_sub(bucket.queue.len(), Ordering::Release);
    }
}

#[derive(Debug)]
struct Bucket<M> {
    queue: SegQueue<M>,
    subs: AtomicUsize,
    recv_notify: Event,
}

/// Subscriber to the tagged queue created by [`subscribe()`] function.
///
/// [`subscribe()`]: crate::MessageQueueBroker::subscribe
#[pin_project(PinnedDrop)]
#[derive(Debug)]
pub struct Subscriber<T: Hash + Eq, M> {
    tag: T,
    bucket: Arc<Bucket<M>>,
    broker: Arc<MessageQueueBrokerInner<T, M>>,
    listener: Option<EventListener>,

    // Keeping this type `!Unpin` enables future optimizations.
    #[pin]
    _pin: PhantomPinned,
}

impl<T, M> Subscriber<T, M>
where
    T: Hash + Eq,
{
    /// Returns the number of subscribers of the same tag.
    pub fn subs_count(&self) -> usize {
        self.bucket.subs.load(Ordering::Acquire)
    }

    /// Returns length of tagged queue.
    ///
    /// ### Example
    /// ```
    /// use mqb::MessageQueueBroker;
    ///
    /// # futures::executor::block_on(async move {
    /// let mqb = MessageQueueBroker::unbounded();
    /// let sub1 = mqb.subscribe(1);
    /// let sub2 = mqb.subscribe(2);
    ///
    /// mqb.send(&2, 1).await.unwrap();
    /// assert_eq!(sub1.len(), 0);
    /// assert_eq!(sub2.len(), mqb.len());
    /// # });
    /// ```
    pub fn len(&self) -> usize {
        self.bucket.queue.len()
    }

    /// Checks if the tagged queue is empty.
    ///
    /// ### Example
    /// ```
    /// use mqb::MessageQueueBroker;
    ///
    /// # futures::executor::block_on(async move {
    /// let mqb = MessageQueueBroker::unbounded();
    /// let sub1 = mqb.subscribe(1);
    /// let sub2 = mqb.subscribe(2);
    ///
    /// mqb.send(&2, 1).await.unwrap();
    /// assert!(sub1.is_empty());
    /// assert_eq!(sub2.len(), mqb.len());
    /// # });
    /// ```
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Checks if the broker is closed.
    pub fn is_closed(&self) -> bool {
        match &*self.broker {
            MessageQueueBrokerInner::Bounded(b) => b.is_closed(),
            MessageQueueBrokerInner::Unbounded(b) => b.is_closed(),
        }
    }

    /// Trying to receive a message from the tagged queue.
    ///
    /// If broker is closed then returns `TryRecvError::Closed`.
    /// If the tagged queue is empty returns `TryRecvError::Empty`.
    ///
    /// ### Ok
    /// ```
    /// use mqb::MessageQueueBroker;
    ///
    /// let mqb = MessageQueueBroker::unbounded();
    /// let sub = mqb.subscribe(1);
    ///
    /// assert!(mqb.try_send(&1, 1).is_ok());
    /// assert_eq!(sub.try_recv().unwrap(), 1);
    /// ```
    ///
    /// ### Broker closed
    /// ```
    /// use mqb::{MessageQueueBroker, TryRecvError};
    ///
    /// let mqb = MessageQueueBroker::<i32, i32>::unbounded();
    /// let sub = mqb.subscribe(1);
    ///
    /// mqb.close();
    /// assert_eq!(sub.try_recv().unwrap_err(), TryRecvError::Closed);
    /// ```
    ///
    /// ### Queue is empty
    /// ```
    /// use mqb::{MessageQueueBroker, TryRecvError};
    ///
    /// let mqb = MessageQueueBroker::<i32, i32>::unbounded();
    /// let sub = mqb.subscribe(1);
    ///
    /// assert_eq!(sub.try_recv().unwrap_err(), TryRecvError::Empty);
    /// ```
    pub fn try_recv(&self) -> Result<M, TryRecvError> {
        match &*self.broker {
            MessageQueueBrokerInner::Bounded(b) => {
                if b.is_closed() {
                    return Err(TryRecvError::Closed);
                }

                let msg = self.bucket.queue.pop().ok_or(TryRecvError::Empty)?;
                b.len.fetch_sub(1, Ordering::Release);
                b.send_notify.notify(1.additional());
                Ok(msg)
            }
            MessageQueueBrokerInner::Unbounded(b) => {
                if b.is_closed() {
                    return Err(TryRecvError::Closed);
                }

                let msg = self.bucket.queue.pop().ok_or(TryRecvError::Empty)?;
                b.len.fetch_sub(1, Ordering::Release);
                Ok(msg)
            }
        }
    }

    /// Receives a message from the tagged queue.
    ///
    /// If broker is closed then returns `RecvError`.
    ///
    /// ### Ok
    /// ```
    /// use mqb::MessageQueueBroker;
    ///
    /// # futures::executor::block_on(async move {
    /// let mqb = MessageQueueBroker::unbounded();
    /// let sub = mqb.subscribe(1);
    ///
    /// assert!(mqb.send(&1, 1).await.is_ok());
    /// assert_eq!(sub.recv().await.unwrap(), 1);
    /// # });
    /// ```
    ///
    /// ### Broker closed
    /// ```
    /// use mqb::{MessageQueueBroker, RecvError};
    ///
    /// # futures::executor::block_on(async move {
    /// let mqb = MessageQueueBroker::<i32, i32>::unbounded();
    /// let sub = mqb.subscribe(1);
    ///
    /// mqb.close();
    /// assert_eq!(sub.recv().await.unwrap_err(), RecvError);
    /// # });
    /// ```
    ///
    /// ### Queue is empty
    /// ```
    /// use mqb::MessageQueueBroker;
    ///
    /// # futures::executor::block_on(async move {
    /// let mqb = MessageQueueBroker::<i32, i32>::bounded(1);
    /// let sub = mqb.subscribe(1);
    ///
    /// # async {
    /// # let _ =
    /// // Wait endlessly...
    /// sub.recv().await;
    /// # };
    /// # });
    /// ```
    pub fn recv(&self) -> Recv<'_, T, M> {
        Recv::new(RecvInner {
            sub: self,
            listener: None,
            _pin: Default::default(),
        })
    }

    /// Receives a message from the tagged queue using the blocking strategy.
    ///
    /// If broker is closed then returns `RecvError`.
    ///
    /// ### Ok
    /// ```
    /// use mqb::MessageQueueBroker;
    ///
    /// let mqb = MessageQueueBroker::unbounded();
    /// let sub = mqb.subscribe(1);
    ///
    /// assert!(mqb.try_send(&1, 1).is_ok());
    /// assert_eq!(sub.recv_blocking().unwrap(), 1);
    /// ```
    ///
    /// ### Broker closed
    /// ```
    /// use mqb::{MessageQueueBroker, RecvError};
    ///
    /// let mqb = MessageQueueBroker::<i32, i32>::unbounded();
    /// let sub = mqb.subscribe(1);
    ///
    /// mqb.close();
    /// assert_eq!(sub.recv_blocking().unwrap_err(), RecvError);
    /// ```
    ///
    /// ### Queue is empty
    /// ```
    /// use mqb::MessageQueueBroker;
    ///
    /// let mqb = MessageQueueBroker::<i32, i32>::bounded(1);
    /// let sub = mqb.subscribe(1);
    ///
    /// # stringify!(
    /// // Wait endlessly...
    /// sub.recv_blocking();
    /// # );
    /// ```
    pub fn recv_blocking(&self) -> Result<M, RecvError> {
        self.recv().wait()
    }

    /// Returns the associated tag.
    ///
    /// ### Example
    /// ```
    /// use mqb::MessageQueueBroker;
    ///
    /// # futures::executor::block_on(async move {
    /// let mqb = MessageQueueBroker::<i32, i32>::unbounded();
    /// let sub = mqb.subscribe(1);
    ///
    /// assert_eq!(sub.tag(), &1);
    /// # });
    /// ```
    pub fn tag(&self) -> &T {
        &self.tag
    }
}

impl<T, M> Clone for Subscriber<T, M>
where
    T: Hash + Eq + Clone,
{
    fn clone(&self) -> Self {
        self.bucket.subs.fetch_add(1, Ordering::Relaxed);
        Self {
            tag: self.tag.clone(),
            bucket: self.bucket.clone(),
            broker: self.broker.clone(),
            listener: None,
            _pin: Default::default(),
        }
    }
}

#[pinned_drop]
impl<T, M> PinnedDrop for Subscriber<T, M>
where
    T: Hash + Eq,
{
    fn drop(self: Pin<&mut Self>) {
        if !self.is_closed()
            && self.bucket.subs.fetch_sub(1, Ordering::Relaxed) == 1
        {
            self.broker.unsubscribe(&self.tag);
        }
    }
}

impl<T, M> Stream for Subscriber<T, M>
where
    T: Hash + Eq,
{
    type Item = M;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        loop {
            // If this stream is listening for events, first wait for a notification.
            {
                let this = self.as_mut().project();
                if let Some(listener) = this.listener.as_mut() {
                    ready!(Pin::new(listener).poll(cx));
                    *this.listener = None;
                }
            }

            'internal: loop {
                // Attempt to receive a message.
                match self.try_recv() {
                    Ok(msg) => {
                        // The stream is not blocked on an event - drop the listener.
                        let this = self.as_mut().project();
                        *this.listener = None;
                        return Poll::Ready(Some(msg));
                    }
                    Err(TryRecvError::Closed) => {
                        // The stream is not blocked on an event - drop the listener.
                        let this = self.as_mut().project();
                        *this.listener = None;
                        return Poll::Ready(None);
                    }
                    Err(TryRecvError::Empty) => {}
                }

                // Receiving failed - now start listening for notifications or wait for one.
                let this = self.as_mut().project();
                if this.listener.is_some() {
                    // Go back to the outer loop to wait for a notification.
                    break 'internal;
                } else {
                    *this.listener = Some(this.bucket.recv_notify.listen());
                }
            }
        }
    }
}

#[pin_project]
/// A future returned by [`Subscriber::recv()`].
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Recv<'a, T: Hash + Eq, M> {
    #[pin]
    inner: FutureWrapper<RecvInner<'a, T, M>>,
}

impl<'a, T, M> Recv<'a, T, M>
where
    T: Hash + Eq,
{
    #[inline]
    fn new(inner: RecvInner<'a, T, M>) -> Self {
        Self {
            inner: FutureWrapper::new(inner),
        }
    }

    #[inline]
    fn wait(self) -> Result<M, RecvError> {
        self.inner.into_inner().wait()
    }
}

impl<T, M> Future for Recv<'_, T, M>
where
    T: Hash + Eq,
{
    type Output = Result<M, RecvError>;

    #[inline]
    fn poll(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Self::Output> {
        self.project().inner.poll(context)
    }
}

#[pin_project]
#[derive(Debug)]
pub struct RecvInner<'a, T: Hash + Eq, M> {
    sub: &'a Subscriber<T, M>,

    // Listener waiting on the channel.
    listener: Option<EventListener>,

    // Keeping this type `!Unpin` enables future optimizations.
    #[pin]
    _pin: PhantomPinned,
}

impl<T, M> EventListenerFuture for RecvInner<'_, T, M>
where
    T: Hash + Eq,
{
    type Output = Result<M, RecvError>;

    /// Run this future with the given `Strategy`.
    fn poll_with_strategy<'x, S: Strategy<'x>>(
        self: Pin<&mut Self>,
        strategy: &mut S,
        cx: &mut S::Context,
    ) -> Poll<Self::Output> {
        let this = self.project();

        loop {
            // Attempt to receive a message.
            match this.sub.try_recv() {
                Ok(msg) => return Poll::Ready(Ok(msg)),
                Err(TryRecvError::Closed) => {
                    return Poll::Ready(Err(RecvError));
                }
                Err(TryRecvError::Empty) => {}
            }

            // Receiving failed - now start listening for notifications or wait for one.
            if this.listener.is_some() {
                // Poll using the given strategy
                ready!(S::poll(strategy, &mut *this.listener, cx));
            } else {
                *this.listener = Some(this.sub.bucket.recv_notify.listen());
            }
        }
    }
}

#[pin_project]
/// A future returned by [`MessageQueueBroker::send()`].
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Send<'a, T: Hash + Eq, M, Q: ?Sized> {
    #[pin]
    inner: FutureWrapper<SendInner<'a, T, M, Q>>,
}

impl<'a, T, M, Q> Send<'a, T, M, Q>
where
    T: Hash + Eq,
    Q: Hash + Equivalent<T> + ?Sized,
{
    #[inline]
    fn new(inner: SendInner<'a, T, M, Q>) -> Self {
        Self {
            inner: FutureWrapper::new(inner),
        }
    }

    #[inline]
    fn wait(self) -> Result<(), SendError<M>> {
        self.inner.into_inner().wait()
    }
}

impl<T, M, Q> Future for Send<'_, T, M, Q>
where
    T: Hash + Eq,
    Q: Hash + Equivalent<T> + ?Sized,
{
    type Output = Result<(), SendError<M>>;

    #[inline]
    fn poll(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Self::Output> {
        self.project().inner.poll(context)
    }
}

#[pin_project]
#[derive(Debug)]
pub struct SendInner<'a, T: Hash + Eq, M, Q: ?Sized> {
    broker: &'a MessageQueueBroker<T, M>,
    msg: Option<M>,
    tag: Option<&'a Q>,

    // Listener waiting on the channel.
    listener: Option<EventListener>,

    // Keeping this type `!Unpin` enables future optimizations.
    #[pin]
    _pin: PhantomPinned,
}

impl<T, M, Q> EventListenerFuture for SendInner<'_, T, M, Q>
where
    T: Hash + Eq,
    Q: Hash + Equivalent<T> + ?Sized,
{
    type Output = Result<(), SendError<M>>;

    /// Run this future with the given `Strategy`.
    fn poll_with_strategy<'x, S: Strategy<'x>>(
        self: Pin<&mut Self>,
        strategy: &mut S,
        cx: &mut S::Context,
    ) -> Poll<Self::Output> {
        let this = self.project();

        match &*this.broker.inner {
            MessageQueueBrokerInner::Bounded(broker) => {
                loop {
                    let msg = this.msg.take().expect("message should be");
                    let tag = this.tag.take().expect("tag should be");

                    let res = broker.try_send(tag, msg);
                    match res {
                        Ok(v) => return Poll::Ready(Ok(v)),
                        Err(TrySendError::Closed(msg)) => {
                            return Poll::Ready(Err(SendError(msg)));
                        }
                        // Restore msg and tag
                        Err(TrySendError::Full(msg)) => {
                            *this.msg = Some(msg);
                            *this.tag = Some(tag);
                        }
                    }

                    // Receiving failed - now start listening for notifications or wait for one.
                    if this.listener.is_some() {
                        // Poll using the given strategy
                        ready!(S::poll(strategy, &mut *this.listener, cx));
                    } else {
                        *this.listener = Some(broker.send_notify.listen());
                    }
                }
            }
            MessageQueueBrokerInner::Unbounded(broker) => {
                let msg = this.msg.take().expect("message should be");
                let tag = this.tag.take().expect("tag should be");
                let res = broker.try_send(tag, msg).map_err(|err| match err {
                    TrySendError::Closed(msg) => SendError(msg),
                    TrySendError::Full(_) => unreachable!(),
                });
                Poll::Ready(res)
            }
        }
    }
}

#[derive(thiserror::Error, Eq, PartialEq)]
#[error("sending into a closed channel")]
pub struct SendError<T>(pub T);

impl<T> SendError<T> {
    /// Unwraps the message that couldn't be sent.
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T> Debug for SendError<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "SendError(..)")
    }
}

#[derive(thiserror::Error, Eq, PartialEq)]
pub enum TrySendError<T> {
    #[error("sending into a full channel")]
    Full(T),
    #[error("sending into a closed channel")]
    Closed(T),
}

impl<T> TrySendError<T> {
    /// Unwraps the message that couldn't be sent.
    pub fn into_inner(self) -> T {
        match self {
            TrySendError::Full(t) => t,
            TrySendError::Closed(t) => t,
        }
    }

    /// Returns `true` if the channel is full but not closed.
    pub fn is_full(&self) -> bool {
        match self {
            TrySendError::Full(_) => true,
            TrySendError::Closed(_) => false,
        }
    }

    /// Returns `true` if the channel is closed.
    pub fn is_closed(&self) -> bool {
        match self {
            TrySendError::Full(_) => false,
            TrySendError::Closed(_) => true,
        }
    }
}

impl<T> Debug for TrySendError<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TrySendError::Full(_) => write!(f, "Full(..)"),
            TrySendError::Closed(_) => write!(f, "Closed(..)"),
        }
    }
}

#[derive(Debug, thiserror::Error, Eq, PartialEq)]
#[error("receiving from an empty and closed channel")]
pub struct RecvError;

#[derive(Debug, thiserror::Error, Eq, PartialEq)]
pub enum TryRecvError {
    #[error("receiving from an empty channel")]
    Empty,
    #[error("receiving from an closed channel")]
    Closed,
}

impl TryRecvError {
    /// Returns `true` if the channel is empty but not closed.
    pub fn is_empty(&self) -> bool {
        match self {
            TryRecvError::Empty => true,
            TryRecvError::Closed => false,
        }
    }

    /// Returns `true` if the channel is closed.
    pub fn is_closed(&self) -> bool {
        match self {
            TryRecvError::Empty => false,
            TryRecvError::Closed => true,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::pin::pin;

    use futures::StreamExt;
    use rand::prelude::SliceRandom;
    use tokio::sync::Semaphore;

    use super::*;

    trait Receiver<T, M>
    where
        T: Hash + Eq,
    {
        fn next(
            self: Pin<&mut Self>,
        ) -> impl Future<Output = Option<M>> + std::marker::Send;

        fn new(v: Subscriber<T, M>) -> Self
        where
            Self: Sized;
    }

    struct RecvMethodReceiver<T: Hash + Eq, M>(Subscriber<T, M>);
    impl<T, M> Receiver<T, M> for RecvMethodReceiver<T, M>
    where
        T: Hash + Eq + std::marker::Send + Sync,
        M: std::marker::Send,
    {
        fn next(
            self: Pin<&mut Self>,
        ) -> impl Future<Output = Option<M>> + std::marker::Send {
            async move { self.0.recv().await.ok() }
        }

        fn new(v: Subscriber<T, M>) -> Self
        where
            Self: Sized,
        {
            Self(v)
        }
    }

    #[pin_project]
    struct StreamReceiver<T: Hash + Eq, M>(#[pin] Subscriber<T, M>);
    impl<T, M> Receiver<T, M> for StreamReceiver<T, M>
    where
        T: Hash + Eq + std::marker::Send + Sync,
        M: std::marker::Send,
    {
        fn next(
            self: Pin<&mut Self>,
        ) -> impl Future<Output = Option<M>> + std::marker::Send {
            async move {
                let mut this = self.project();
                this.0.next().await
            }
        }

        fn new(v: Subscriber<T, M>) -> Self
        where
            Self: Sized,
        {
            Self(v)
        }
    }

    async fn parallel_check_async<
        R,
        const WRITER_THREADS: usize,
        const TAGS: usize,
        const MESSAGES_PER_TAG: usize,
        const READERS_PER_TAG: usize,
    >(
        mqb: MessageQueueBroker<usize, usize>,
    ) where
        R: Receiver<usize, usize> + std::marker::Send,
    {
        let all_threads = WRITER_THREADS + TAGS * READERS_PER_TAG;

        let mut rnd = rand::rng();

        let mqb = Arc::new(mqb);
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
                msgs.shuffle(&mut rnd);
                msgs
            };
            let fut = async move {
                let _permit = start_notify.acquire().await;
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
                let sub = mqb.subscribe(tag);
                let start_notify = start_notify.clone();
                let messages_per_reader = messages_per_readers[thread_idx];
                let fut = async move {
                    let mut receiver = pin!(R::new(sub));
                    let _permit = start_notify.acquire().await.unwrap();

                    for _ in 0..messages_per_reader {
                        receiver.as_mut().next().await.unwrap();
                    }
                };

                tasks.push(tokio::spawn(fut));
            }
        }

        start_notify.add_permits(all_threads);
        assert!(
            futures::future::join_all(tasks)
                .await
                .iter()
                .all(Result::is_ok)
        );
    }

    fn parallel_check_blocking<
        const WRITER_THREADS: usize,
        const TAGS: usize,
        const MESSAGES_PER_TAG: usize,
        const READERS_PER_TAG: usize,
    >(
        mqb: MessageQueueBroker<usize, usize>,
    ) {
        let mut rnd = rand::rng();

        let mqb = Arc::new(mqb);

        let mut tasks = easy_parallel::Parallel::new();
        // writers
        for thread_idx in 0..WRITER_THREADS {
            let mqb = mqb.clone();
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
                msgs.shuffle(&mut rnd);
                msgs
            };
            let f = move || {
                for (tag, msg) in messages {
                    mqb.send_blocking(&tag, msg).unwrap();
                }
            };

            tasks = tasks.add(f);
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
                let sub = mqb.subscribe(tag);
                let messages_per_reader = messages_per_readers[thread_idx];
                let fut = move || {
                    for _ in 0..messages_per_reader {
                        sub.recv_blocking().unwrap();
                    }
                };

                tasks = tasks.add(fut);
            }
        }

        let _ = tasks.run();
    }

    #[tokio::test]
    async fn unbounded_parallel() {
        parallel_check_async::<RecvMethodReceiver<_, _>, 1, 1000, 1, 1>(
            MessageQueueBroker::unbounded(),
        )
        .await;
        parallel_check_async::<RecvMethodReceiver<_, _>, 20, 1000, 100, 1>(
            MessageQueueBroker::unbounded(),
        )
        .await;
        parallel_check_async::<RecvMethodReceiver<_, _>, 20, 1000, 100, 2>(
            MessageQueueBroker::unbounded(),
        )
        .await;
    }

    #[tokio::test]
    async fn bounded_parallel() {
        parallel_check_async::<RecvMethodReceiver<_, _>, 1, 1000, 1, 1>(
            MessageQueueBroker::bounded(10),
        )
        .await;
        parallel_check_async::<RecvMethodReceiver<_, _>, 20, 1000, 100, 1>(
            MessageQueueBroker::bounded(10),
        )
        .await;
        parallel_check_async::<RecvMethodReceiver<_, _>, 20, 1000, 100, 2>(
            MessageQueueBroker::bounded(10),
        )
        .await;
    }

    #[tokio::test]
    async fn unbounded_parallel_stream() {
        parallel_check_async::<StreamReceiver<_, _>, 1, 1000, 1, 1>(
            MessageQueueBroker::unbounded(),
        )
        .await;
        parallel_check_async::<StreamReceiver<_, _>, 20, 1000, 100, 1>(
            MessageQueueBroker::unbounded(),
        )
        .await;
        parallel_check_async::<StreamReceiver<_, _>, 20, 1000, 100, 2>(
            MessageQueueBroker::unbounded(),
        )
        .await;
    }

    #[tokio::test]
    async fn bounded_parallel_stream() {
        parallel_check_async::<StreamReceiver<_, _>, 1, 1000, 1, 1>(
            MessageQueueBroker::bounded(10),
        )
        .await;
        parallel_check_async::<StreamReceiver<_, _>, 20, 1000, 100, 1>(
            MessageQueueBroker::bounded(10),
        )
        .await;
        parallel_check_async::<StreamReceiver<_, _>, 20, 1000, 100, 2>(
            MessageQueueBroker::bounded(10),
        )
        .await;
    }

    #[tokio::test]
    async fn unbounded_parallel_blocking() {
        parallel_check_blocking::<1, 1000, 1, 1>(
            MessageQueueBroker::unbounded(),
        );
        parallel_check_blocking::<20, 1000, 100, 1>(
            MessageQueueBroker::unbounded(),
        );
        parallel_check_blocking::<20, 1000, 100, 2>(
            MessageQueueBroker::unbounded(),
        );
    }

    #[tokio::test]
    async fn bounded_parallel_blocking() {
        parallel_check_blocking::<1, 1000, 1, 1>(MessageQueueBroker::bounded(
            10,
        ));
        parallel_check_blocking::<20, 1000, 100, 1>(
            MessageQueueBroker::bounded(10),
        );
        parallel_check_blocking::<20, 1000, 100, 2>(
            MessageQueueBroker::bounded(10),
        );
    }

    #[futures_test::test]
    async fn unbounded() {
        let mbq = MessageQueueBroker::unbounded();

        let sub1 = mbq.subscribe(1);
        let sub2 = mbq.subscribe(2);

        mbq.send(&1, 1).await.unwrap();
        mbq.send(&2, 2).await.unwrap();
        assert_eq!(mbq.len(), 2);
        assert_eq!(mbq.try_send(&3, 42).unwrap_err(), TrySendError::Closed(42));
        assert_eq!(mbq.len(), 2);

        assert_eq!(sub1.len(), 1);
        assert_eq!(sub1.recv().await, Ok(1));
        assert_eq!(sub1.len(), 0);
        assert_eq!(mbq.len(), 1);

        assert_eq!(sub2.len(), 1);
        assert_eq!(sub2.recv().await, Ok(2));
        assert_eq!(sub2.len(), 0);
        assert_eq!(mbq.len(), 0);

        assert!(mbq.is_empty());
    }

    #[futures_test::test]
    async fn bounded() {
        let mqb = MessageQueueBroker::bounded(2);

        let sub1 = mqb.subscribe(1);
        let sub2 = mqb.subscribe(2);

        mqb.send(&1, 1).await.unwrap();
        mqb.send(&2, 2).await.unwrap();
        assert_eq!(mqb.len(), 2);
        assert_eq!(mqb.try_send(&3, 42).unwrap_err(), TrySendError::Closed(42));
        assert_eq!(mqb.try_send(&2, 3).unwrap_err(), TrySendError::Full(3));
        assert_eq!(mqb.len(), 2);

        assert_eq!(sub1.len(), 1);
        assert_eq!(sub1.recv().await, Ok(1));
        assert_eq!(sub1.len(), 0);
        assert_eq!(mqb.len(), 1);

        assert_eq!(sub2.len(), 1);
        assert_eq!(sub2.recv().await, Ok(2));
        assert_eq!(sub2.len(), 0);
        assert_eq!(mqb.len(), 0);

        assert!(mqb.is_empty());
    }

    #[futures_test::test]
    async fn unbounded_stream() {
        let mbq = MessageQueueBroker::unbounded();

        let mut sub1 = pin!(mbq.subscribe(1));
        let mut sub2 = pin!(mbq.subscribe(2));

        mbq.send(&1, 1).await.unwrap();
        mbq.send(&2, 2).await.unwrap();
        assert_eq!(mbq.len(), 2);
        assert_eq!(mbq.try_send(&3, 42).unwrap_err(), TrySendError::Closed(42));
        assert_eq!(mbq.len(), 2);

        assert_eq!(sub1.len(), 1);
        assert_eq!(sub1.next().await, Some(1));
        assert_eq!(sub1.len(), 0);
        assert_eq!(mbq.len(), 1);

        assert_eq!(sub2.len(), 1);
        assert_eq!(sub2.next().await, Some(2));
        assert_eq!(sub2.len(), 0);
        assert_eq!(mbq.len(), 0);

        assert!(mbq.is_empty());
    }

    #[futures_test::test]
    async fn bounded_stream() {
        let mqb = MessageQueueBroker::bounded(2);

        let mut sub1 = pin!(mqb.subscribe(1));
        let mut sub2 = pin!(mqb.subscribe(2));

        mqb.send(&1, 1).await.unwrap();
        mqb.send(&2, 2).await.unwrap();
        assert_eq!(mqb.len(), 2);
        assert_eq!(mqb.try_send(&3, 42).unwrap_err(), TrySendError::Closed(42));
        assert_eq!(mqb.try_send(&2, 3).unwrap_err(), TrySendError::Full(3));
        assert_eq!(mqb.len(), 2);

        assert_eq!(sub1.len(), 1);
        assert_eq!(sub1.next().await, Some(1));
        assert_eq!(sub1.len(), 0);
        assert_eq!(mqb.len(), 1);

        assert_eq!(sub2.len(), 1);
        assert_eq!(sub2.next().await, Some(2));
        assert_eq!(sub2.len(), 0);
        assert_eq!(mqb.len(), 0);

        assert!(mqb.is_empty());
    }

    #[test]
    fn unbounded_blocking() {
        let mbq = MessageQueueBroker::unbounded();

        let sub1 = mbq.subscribe(1);
        let sub2 = mbq.subscribe(2);

        mbq.send_blocking(&1, 1).unwrap();
        mbq.send_blocking(&2, 2).unwrap();
        assert_eq!(mbq.len(), 2);
        assert_eq!(mbq.try_send(&3, 42).unwrap_err(), TrySendError::Closed(42));
        assert_eq!(mbq.len(), 2);

        assert_eq!(sub1.len(), 1);
        assert_eq!(sub1.recv_blocking(), Ok(1));
        assert_eq!(sub1.len(), 0);
        assert_eq!(mbq.len(), 1);

        assert_eq!(sub2.len(), 1);
        assert_eq!(sub2.recv_blocking(), Ok(2));
        assert_eq!(sub2.len(), 0);
        assert_eq!(mbq.len(), 0);

        assert!(mbq.is_empty());
    }

    #[test]
    fn bounded_blocking() {
        let mqb = MessageQueueBroker::bounded(2);

        let sub1 = mqb.subscribe(1);
        let sub2 = mqb.subscribe(2);

        mqb.send_blocking(&1, 1).unwrap();
        mqb.send_blocking(&2, 2).unwrap();
        assert_eq!(mqb.len(), 2);
        assert_eq!(mqb.try_send(&3, 42).unwrap_err(), TrySendError::Closed(42));
        assert_eq!(mqb.try_send(&2, 3).unwrap_err(), TrySendError::Full(3));
        assert_eq!(mqb.len(), 2);

        assert_eq!(sub1.len(), 1);
        assert_eq!(sub1.recv_blocking(), Ok(1));
        assert_eq!(sub1.len(), 0);
        assert_eq!(mqb.len(), 1);

        assert_eq!(sub2.len(), 1);
        assert_eq!(sub2.recv_blocking(), Ok(2));
        assert_eq!(sub2.len(), 0);
        assert_eq!(mqb.len(), 0);

        assert!(mqb.is_empty());
    }

    #[futures_test::test]
    async fn sub_unsub() {
        let mqb = MessageQueueBroker::unbounded();

        let sub1 = mqb.subscribe(1);
        let sub1_copy1 = mqb.subscribe(1);
        let sub1_copy2 = sub1.clone();

        assert_eq!(sub1.subs_count(), 3);

        drop(sub1_copy1);
        assert_eq!(sub1.subs_count(), 2);

        drop(sub1_copy2);
        assert_eq!(sub1.subs_count(), 1);

        drop(sub1);
        assert_eq!(mqb.try_send(&1, 1).unwrap_err(), TrySendError::Closed(1));
        assert_eq!(mqb.send(&1, 1).await.unwrap_err(), SendError(1));
    }

    #[futures_test::test]
    async fn close() {
        let mqb = MessageQueueBroker::<i32, i32>::unbounded();
        let sub1 = mqb.subscribe(1);

        assert!(!sub1.is_closed());
        drop(mqb);
        assert!(sub1.is_closed());
    }
}

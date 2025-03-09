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
    hash::Hash,
    pin::pin,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use crossbeam::queue::SegQueue;
use scc::hash_map::Entry;
use tokio::sync::Notify;

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
        Q: Hash + scc::Equivalent<T> + ?Sized,
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
    /// # tokio_test::block_on(async move {
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
    /// # tokio_test::block_on(async move {
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
    /// # tokio_test::block_on(async move {
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
    /// # tokio_test::block_on(async move {
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
    pub async fn send<Q>(&self, tag: &Q, msg: M) -> Result<(), SendError<M>>
    where
        Q: Hash + scc::Equivalent<T> + ?Sized,
    {
        self.inner.send(tag, msg).await
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

        match buckets.entry(tag.clone()) {
            Entry::Occupied(e) => {
                let bucket = e.get().clone();
                bucket.subs.fetch_add(1, Ordering::Release);
                Subscriber {
                    tag,
                    bucket,
                    broker: Arc::clone(this),
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
        Q: Hash + scc::Equivalent<T> + ?Sized,
    {
        match self {
            MessageQueueBrokerInner::Bounded(b) => b.try_send(tag, msg),
            MessageQueueBrokerInner::Unbounded(b) => b.try_send(tag, msg),
        }
    }

    async fn send<Q>(&self, tag: &Q, msg: M) -> Result<(), SendError<M>>
    where
        Q: Hash + scc::Equivalent<T> + ?Sized,
    {
        match self {
            MessageQueueBrokerInner::Bounded(b) => b.send(tag, msg).await,
            MessageQueueBrokerInner::Unbounded(b) => b.send(tag, msg).await,
        }
    }

    fn unsubscribe<Q>(&self, tag: &Q)
    where
        Q: Hash + scc::Equivalent<T> + ?Sized,
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
    send_notify: Notify,
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
        let mut next_entry = self.buckets.first_entry();
        while let Some(e) = next_entry {
            e.recv_notify.notify_waiters();
            next_entry = e.next();
        }
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
        Q: Hash + scc::Equivalent<T> + ?Sized,
    {
        if self.is_closed() {
            return Err(TrySendError::Closed(msg));
        }

        let Some(bucket) = self.buckets.get(tag) else {
            return Err(TrySendError::Closed(msg));
        };

        match self.try_acquire_slot() {
            Ok(_) => {
                bucket.queue.push(msg);
                bucket.recv_notify.notify_one();
                Ok(())
            }
            Err(_) => Err(TrySendError::Full(msg)),
        }
    }

    async fn send<Q>(&self, tag: &Q, msg: M) -> Result<(), SendError<M>>
    where
        Q: Hash + scc::Equivalent<T> + ?Sized,
    {
        let mut notified = pin!(self.send_notify.notified());

        loop {
            if self.is_closed() {
                return Err(SendError(msg));
            }

            {
                let Some(bucket) = self.buckets.get(tag) else {
                    return Err(SendError(msg));
                };

                notified.as_mut().enable();

                if self.try_acquire_slot().is_ok() {
                    bucket.queue.push(msg);
                    bucket.recv_notify.notify_one();
                    return Ok(());
                }
            }

            notified.as_mut().await;
            notified.set(self.send_notify.notified());
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
        Q: Hash + scc::Equivalent<T> + ?Sized,
    {
        let Some((_tag, bucket)) = self.buckets.remove(tag) else {
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
        let mut next_entry = self.buckets.first_entry();
        while let Some(e) = next_entry {
            e.recv_notify.notify_waiters();
            next_entry = e.next();
        }
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
        Q: Hash + scc::Equivalent<T> + ?Sized,
    {
        if self.is_closed() {
            return Err(TrySendError::Closed(msg));
        }

        let Some(bucket) = self.buckets.get(tag) else {
            return Err(TrySendError::Closed(msg));
        };

        self.len.fetch_add(1, Ordering::Release);
        bucket.queue.push(msg);
        bucket.recv_notify.notify_one();
        Ok(())
    }

    async fn send<Q>(&self, tag: &Q, msg: M) -> Result<(), SendError<M>>
    where
        Q: Hash + scc::Equivalent<T> + ?Sized,
    {
        self.try_send(tag, msg).map_err(|err| match err {
            TrySendError::Closed(msg) => SendError(msg),
            TrySendError::Full(_) => unreachable!(),
        })
    }

    fn unsubscribe<Q>(&self, tag: &Q)
    where
        Q: Hash + scc::Equivalent<T> + ?Sized,
    {
        let Some((_tag, bucket)) = self.buckets.remove(tag) else {
            return;
        };
        self.len.fetch_sub(bucket.queue.len(), Ordering::Release);
    }
}

#[derive(Debug)]
struct Bucket<M> {
    queue: SegQueue<M>,
    subs: AtomicUsize,
    recv_notify: Notify,
}

/// Subscriber to the tagged queue created by [`subscribe()`] function.
///
/// [`subscribe()`]: crate::MessageQueueBroker::subscribe
#[derive(Debug)]
pub struct Subscriber<T: Hash + Eq, M> {
    tag: T,
    bucket: Arc<Bucket<M>>,
    broker: Arc<MessageQueueBrokerInner<T, M>>,
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
    /// # tokio_test::block_on(async move {
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
    /// # tokio_test::block_on(async move {
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
        Self::try_recv2(&self.broker, &self.bucket.queue)
    }

    /// Receives a message from the tagged queue.
    ///
    /// If broker is closed then returns `RecvError`.
    ///
    /// ### Ok
    /// ```
    /// use mqb::MessageQueueBroker;
    ///
    /// # tokio_test::block_on(async move {
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
    /// # tokio_test::block_on(async move {
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
    /// # tokio_test::block_on(async move {
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
    pub async fn recv(&self) -> Result<M, RecvError> {
        let mut notified = pin!(self.bucket.recv_notify.notified());

        loop {
            notified.as_mut().enable();

            match Self::try_recv2(&self.broker, &self.bucket.queue) {
                Ok(msg) => return Ok(msg),
                Err(TryRecvError::Closed) => return Err(RecvError),
                Err(TryRecvError::Empty) => {
                    notified.as_mut().await;
                    notified.set(self.bucket.recv_notify.notified());
                }
            }
        }
    }

    fn try_recv2(
        broker: &MessageQueueBrokerInner<T, M>,
        bucket_queue: &SegQueue<M>,
    ) -> Result<M, TryRecvError> {
        match broker {
            MessageQueueBrokerInner::Bounded(b) => {
                if b.is_closed() {
                    return Err(TryRecvError::Closed);
                }

                let msg = bucket_queue.pop().ok_or(TryRecvError::Empty)?;
                b.len.fetch_sub(1, Ordering::Release);
                b.send_notify.notify_one();
                Ok(msg)
            }
            MessageQueueBrokerInner::Unbounded(b) => {
                if b.is_closed() {
                    return Err(TryRecvError::Closed);
                }

                let msg = bucket_queue.pop().ok_or(TryRecvError::Empty)?;
                b.len.fetch_sub(1, Ordering::Release);
                Ok(msg)
            }
        }
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
        }
    }
}

impl<T, M> Drop for Subscriber<T, M>
where
    T: Hash + Eq,
{
    fn drop(&mut self) {
        if !self.is_closed()
            && self.bucket.subs.fetch_sub(1, Ordering::Relaxed) == 1
        {
            self.broker.unsubscribe(&self.tag);
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

    use rand::prelude::SliceRandom;
    use tokio::sync::Semaphore;

    use super::*;

    async fn parallel_check<
        const WRITER_THREADS: usize,
        const TAGS: usize,
        const MESSAGES_PER_TAG: usize,
        const READERS_PER_TAG: usize,
    >(
        mqb: MessageQueueBroker<usize, usize>,
    ) {
        let all_threads = WRITER_THREADS + TAGS * READERS_PER_TAG;

        let mut gen = rand::rng();

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
                let sub = mqb.subscribe(tag);
                let start_notify = start_notify.clone();
                let messages_per_reader = messages_per_readers[thread_idx];
                let fut = async move {
                    let _permit = start_notify.acquire().await.unwrap();

                    for _ in 0..messages_per_reader {
                        sub.recv().await.unwrap();
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

    #[tokio::test]
    async fn unbounded_parallel() {
        parallel_check::<1, 1000, 1, 1>(MessageQueueBroker::unbounded()).await;
        parallel_check::<20, 1000, 100, 1>(MessageQueueBroker::unbounded())
            .await;
        parallel_check::<20, 1000, 100, 2>(MessageQueueBroker::unbounded())
            .await;
    }

    #[tokio::test]
    async fn bounded_parallel() {
        parallel_check::<1, 1000, 1, 1>(MessageQueueBroker::bounded(10)).await;
        parallel_check::<20, 1000, 100, 1>(MessageQueueBroker::bounded(10))
            .await;
        parallel_check::<20, 1000, 100, 2>(MessageQueueBroker::bounded(10))
            .await;
    }

    #[tokio::test]
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

    #[tokio::test]
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

    #[tokio::test]
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

    #[tokio::test]
    async fn close() {
        let mqb = MessageQueueBroker::<i32, i32>::unbounded();
        let sub1 = mqb.subscribe(1);

        assert!(!sub1.is_closed());
        drop(mqb);
        assert!(sub1.is_closed());
    }
}

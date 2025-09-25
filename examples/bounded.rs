use std::pin::pin;

use futures::{Stream, StreamExt};
use mqb::{MessageQueueBroker, RecvError, Subscriber};

#[tokio::main]
async fn main() {
    let broker = MessageQueueBroker::bounded(2);

    // Receive values from another async tasks
    let fut_receiver_a =
        tokio::spawn(process_sub(broker.subscribe(String::from("tag_a"))));
    let fut_receiver_b =
        tokio::spawn(process_stream(broker.subscribe(String::from("tag_b"))));

    // Publish value
    broker.send("tag_a", 42).await.unwrap();
    broker.send("tag_b", 24).await.unwrap();

    let _ = futures::future::join(fut_receiver_a, fut_receiver_b).await;

    let sub = broker.subscribe(String::from("tag_c"));
    broker.close();
    assert_eq!(sub.recv().await, Err(RecvError));
}

async fn process_sub(sub: Subscriber<String, i32>) {
    assert_eq!(sub.recv().await, Ok(42));
}

async fn process_stream(stream: impl Stream<Item = i32>) {
    let mut stream = pin!(stream);
    assert_eq!(stream.next().await, Some(24));
}

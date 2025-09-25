use mqb::{MessageQueueBroker, TrySendError};

#[tokio::main]
async fn main() {
    let broker = MessageQueueBroker::bounded(1);

    let sub_a = broker.subscribe(String::from("tag_a"));
    let sub_b = broker.subscribe(String::from("tag_b"));

    // Publish value
    broker.send("tag_a", 42).await.unwrap();
    // Queue is full, .send().await will block
    assert_eq!(broker.try_send("tag_b", 24), Err(TrySendError::Full(24)));

    assert_eq!(sub_a.recv().await, Ok(42));

    broker.send("tag_b", 24).await.unwrap();
    assert_eq!(sub_b.recv().await, Ok(24));
}

use mqb::{MessageQueueBroker, TrySendError};

fn main() {
    let broker = MessageQueueBroker::bounded(1);

    let sub_a = broker.subscribe(String::from("tag_a"));
    let sub_b = broker.subscribe(String::from("tag_b"));

    // Publish value
    broker.send_blocking("tag_a", 42).unwrap();
    // Queue is full, .send().await will block
    assert_eq!(broker.try_send("tag_b", 24), Err(TrySendError::Full(24)));

    assert_eq!(sub_a.recv_blocking(), Ok(42));

    broker.send_blocking("tag_b", 24).unwrap();
    assert_eq!(sub_b.recv_blocking(), Ok(24));
}

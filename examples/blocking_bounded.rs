use mqb::{MessageQueueBroker, RecvError, Subscriber};

fn main() {
    let broker = MessageQueueBroker::bounded(2);

    let sub_a = broker.subscribe(String::from("tag_a"));
    let sub_b = broker.subscribe(String::from("tag_b"));

    // Publish value
    broker.send_blocking("tag_a", 42).unwrap();
    broker.send_blocking("tag_b", 42).unwrap();

    // Receive values from another async tasks
    easy_parallel::Parallel::new()
        .add(move || process_sub(sub_a))
        .add(move || process_sub(sub_b))
        .run();

    let sub = broker.subscribe(String::from("tag_c"));
    broker.close();
    assert_eq!(sub.recv_blocking(), Err(RecvError));
}

async fn process_sub(sub: Subscriber<String, i32>) {
    assert_eq!(sub.recv_blocking(), Ok(42));
}

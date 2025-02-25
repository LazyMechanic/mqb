# mqb

[<img alt="Crates.io Version" src="https://img.shields.io/crates/v/mqb?style=flat-square">](https://crates.io/crates/mqb)
[<img alt="docs.rs" src="https://img.shields.io/docsrs/mqb?style=flat-square">](https://docs.rs/mqb)
[<img alt="GitHub Actions Workflow Status" src="https://img.shields.io/github/actions/workflow/status/LazyMechanic/mqb/ci.yml?branch=master&style=flat-square">](https://github.com/LazyMechanic/mqb/actions/workflows/ci.yml)

This library provides lock free in memory message queue broker.

```toml
[dependencies]
mqb = "0.1"
```

## Usage example
```rust
use mqb::MessageQueueBroker;

#[tokio::main]
async fn main() {
    let mqb = MessageQueueBroker::unbounded();
    let sub = mqb.subscribe(1);

    assert!(mqb.send(&1, 1).await.is_ok());
    assert_eq!(sub.recv().await.unwrap(), 1);
}
```

<sup>
Licensed under either of <a href="LICENSE-APACHE">Apache License, Version
2.0</a> or <a href="LICENSE-MIT">MIT license</a> at your option.
</sup>

<br>

<sub>
Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in this crate by you, as defined in the Apache-2.0 license, shall
be dual licensed as above, without any additional terms or conditions.
</sub>
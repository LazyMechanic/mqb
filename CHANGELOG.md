# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.4.0] - 2025-09-27
### Added
- `MessageQueueBroker::close_for<Q>(&self, tag: &Q)`
- `MessageQueueBroker::is_closed_for<Q>(&self, tag: &Q) -> bool`

## [0.3.1] - 2025-09-26
### Changed
- `RecvInner` and `SendInner` are private.
- Fix typos in docs.

## [0.3.0] - 2025-09-25
### Changed
- Upgrade MSRV to `1.85.0`.
- Upgrade Edition to `2024`.
- Using explicit futures instead of `async fn`:
  - `async fn send(..) -> Result<(), SendError<M>>` -> `fn send(..) -> Send<'_, T, M, Q>`
  - `async fn recv(..) -> Result<M, RecvError>` -> `fn recv(..) -> Recv<'_, T, M>`
- Using [equivalent](https://crates.io/crates/equivalent) crate
- Bump `scc` version to `3.1`
- Async runtime-agnostic (use [event_listener](https://crates.io/crates/event-listener) instead of
  [tokio::sync::Notify](https://docs.rs/tokio/latest/tokio/sync/struct.Notify.html))
### Added
- Blocking api:
  - `MessageQueueBroker::send_blocking<Q>(&self, tag: &Q, msg: M) -> Result<(), SendError<M>>`
  - `Subscriber::recv_blocking(&self) -> Result<M, RecvError>`
- `Subscriber::tag(&self) -> &T`
- `Subscriber` implements `futures::Stream`
- `MessageQueueBroker` implements `Clone`
- Examples

## [0.2.0] - 2024-03-09
### Changed
- Downgrade MSRV to 1.74.

## [0.1.0] - 2024-02-26
### Initial crate release

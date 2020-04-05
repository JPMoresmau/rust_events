# Rust Events

This is for the moment a little experiment to add a layer of abstraction on top of messaging API. This project is inspired by [Mass Transit](https://masstransit-project.com/): take some decisions about the topology of the underlying messaging architecture, make it easy to create messages types ("Events").

## Main Features

### Multitenancy

Each event can be optionally attached a tenant, which is only an arbitrary string. A consumer can listen to events for all or one specific tenant.

## Supported Backends

### RabbitMQ

RabbitMQ support via [lapin](https://github.com/sozu-proxy/lapin).

Currently you cannot choose the topology, but hopefully at some stage we can add ways to customize it.

### Kafka

Kafka support via [rust-rdkafka](https://github.com/fede1024/rust-rdkafka)

## Misc.

### License

Licensed under [MIT](https://opensource.org/licenses/MIT) license.

### Running tests

Disable parallel tests since they are integration tests
`RUST_LOG=info cargo test  -- --nocapture --test-threads=1`



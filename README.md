# Rust Events

This is for the moment a little experiment to add a layer of abstraction on top of messaging API. This project is inspired by Mass Transit: take some decisions about the topology of the underlying messaging architecture, make it easy to create messages types ("Events").

## Supported Backends

### RabbitMQ

RabbitMQ support via [lapin](https://github.com/sozu-proxy/lapin).

Currently you cannot choose the topology, but hopefully at some stage we can add ways to customize it.

### Kafka

TODO!

## Misc.

### License

Licensed under [MIT](https://opensource.org/licenses/MIT) license.

### Running tests

Disable parallel tests since they are integration tests
`RUST_LOG=info cargo test  -- --nocapture --test-threads=1`



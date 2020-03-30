# Rust Events

This is for the moment a little experiment to add a layer of abstraction on top of messaging API. This project is inspired by Mass Transit: take some decisions about the topology of the underlying messaging architecture, make it easy to create messages types ("Events").


### Running tests

Disable parallel tests since they are integration tests
`RUST_LOG=info cargo test  -- --nocapture --test-threads=1`



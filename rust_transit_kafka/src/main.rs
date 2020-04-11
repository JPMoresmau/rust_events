//! Example of Kafka usage
#[macro_use]
extern crate maplit;

use futures::executor;
use rust_transit::*;
use rust_transit_derive::*;
use rust_transit_kafka::*;
use serde::{Deserialize, Serialize};
use std::{env, thread, time};

#[derive(Debug, Default, Clone, Serialize, Deserialize, EventType)]
struct MyEvent {
    message: String,
}

#[derive(Clone, Debug, ConsumerGroup)]
struct MyConsumer {
    desc: String,
}

impl Consumer<MyEvent> for MyConsumer {
    fn consume(&self, t: GenericEvent<MyEvent>) -> Result<(), ()> {
        println!("{} received: {:?}", self.desc, t);
        Ok(())
    }
}

fn get_tenant(ot: &str) -> &str {
    match ot {
        "-" => "",
        _ => ot,
    }
}

fn main() -> Result<(), EventError> {
    env_logger::init();

    let addr = env::var("brokers.list").unwrap_or_else(|_| "localhost:9092,localhost:9093".into());

    let mut mgr = KafkaEventManager::new(
        &hashmap! {"bootstrap.servers"=>addr.as_str(),"message.timeout.ms" => "5000"},
    )?;
    let args: Vec<String> = env::args().collect();
    match args.len() {
        2 => {
            mgr.add_consumer(
                get_tenant(&args[1]),
                MyConsumer {
                    desc: format!("consumer for tenant {}", args[1]),
                },
            )?;
            println!("Waiting for events....");
            loop {
                thread::sleep(time::Duration::from_secs(1));
            }
        }
        3 => {
            executor::block_on(mgr.send(
                get_tenant(&args[1]),
                MyEvent {
                    message: args[2].clone(),
                },
            ))?;
        }
        _ => println!("Usage: (tenant or - for all) message?"),
    }

    Ok(())
}

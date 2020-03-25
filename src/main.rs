extern crate rust_events_derive;

use lapin::ConnectionProperties;
use std::{env,thread, time};
use rust_events::*;
use rust_events_derive::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, ConsumerGroup)]
struct MyConsumer {
    desc: String,
}

#[derive(Debug, Default, Clone, Serialize,Deserialize, EventType)]
struct MyEvent {
    message: String,
}

impl Consumer<MyEvent> for MyConsumer {

    fn consume(&self, t: GenericEvent<MyEvent>) -> Result<(),()>{
        println!("{} received: {:?}",self.desc,t);
        Ok(())
    }
}


fn get_tenant(ot: &str)-> Option<&str> {
    match ot{
        "-" => None,
        _ => Some(ot),
    }
}

fn main() -> Result<(),EventError> {
    env_logger::init();

    let addr = env::var("AMQP_ADDR").unwrap_or_else(|_| "amqp://127.0.0.1:5672/%2f".into());

    let mut mgr = RabbitMQEventManager::new(&addr, ConnectionProperties::default())?;
    let args: Vec<String> = env::args().collect();
    match args.len() {
        2 => {
            mgr.add_consumer(get_tenant(&args[1]), MyConsumer{desc:format!("consumer for tenant {}",args[1])})?;
            loop {
                thread::sleep(time::Duration::from_secs(1));
            }
        },
        3 => {
            mgr.send(get_tenant(&args[1]), MyEvent{message:args[2].clone()})?;
            mgr.close()?;
        },
        _ => println!("Usage: (tenant or - for all) message?"),
    }

    Ok(())
}
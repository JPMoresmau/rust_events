use lapin::{
    message::DeliveryResult, options::*, types::FieldTable, BasicProperties, Channel, Connection,
    ConnectionProperties, ConsumerDelegate,
};
use std::env;
use rust_events::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug)]
struct MyConsumer {
    desc: String,
}

#[derive(Debug, Default, Clone, Serialize,Deserialize)]
struct MyEvent {
    message: String,
}

impl EventType for MyEvent {
    fn code(&self) -> String{
        "MyEvent".to_owned()
    }
}

impl Consumer<MyEvent> for MyConsumer {
    fn group(&self) -> String{
        "MyConsumer".to_owned()
    }

    fn consume(&self, t: GenericEvent<MyEvent>) -> Result<(),()>{
        println!("{} received: {:?}",self.desc,t);
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct Subscriber {
    channel: Channel,
}

impl ConsumerDelegate for Subscriber {
    fn on_new_delivery(&self, delivery: DeliveryResult) {
        if let Ok(Some(delivery)) = delivery {
            self.channel
                .basic_ack(delivery.delivery_tag, BasicAckOptions::default())
                .wait()
                .expect("basic_ack");
        }
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
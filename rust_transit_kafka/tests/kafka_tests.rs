//! Test Kafka manager
use rust_transit::event_tests;

use rust_transit_kafka::*;
#[macro_use] extern crate maplit;

event_tests!(KafkaEventManager::new(&hashmap!{"bootstrap.servers"=>"localhost:9092,localhost:9093","message.timeout.ms" => "5000"}),5,10);
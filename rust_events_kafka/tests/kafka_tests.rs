//! Test Kafka manager
use rust_events::event_tests;

use rust_events_kafka::*;
#[macro_use] extern crate maplit;

event_tests!(KafkaEventManager::new(&hashmap!{"bootstrap.servers"=>"localhost:9092,localhost:9093"}));
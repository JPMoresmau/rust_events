//! Test RabbitMQ manager
use rust_transit::event_tests;
use lapin::ConnectionProperties;

use rust_transit_rabbit::*;

event_tests!(RabbitMQEventManager::new("amqp://127.0.0.1:5672/%2f", ConnectionProperties::default()));
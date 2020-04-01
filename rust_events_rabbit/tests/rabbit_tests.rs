//! Test RabbitMQ manager
use rust_events::event_tests;
use lapin::ConnectionProperties;

use rust_events_rabbit::*;

event_tests!(RabbitMQEventManager::new("amqp://127.0.0.1:5672/%2f", ConnectionProperties::default()));
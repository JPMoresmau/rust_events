//! # rust_events library
//!
//! Abstract the underlying messaging technology under one generic interface
//!

use async_trait::async_trait;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::time::SystemTime;

pub mod harness;

/// The different errors that can occur
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EventError {
    ConnectionError(String),
    SetupError(String),
    SerializationError(String),
    DeserializationError(String),
    SendError(String),
    ReceiveError(String),
    CloseError(String),
    AckError(String),
    OtherError(String),
    CleanError(String),
    UnknownConsumerError(ConsumerID),
    NoConsumeError,
}

/// The type of event
pub trait EventType {
    /// The event code to use
    fn code() -> String;
}

/// A consumer ID to identify a consumer within a manager
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ConsumerID(pub u64);

/// Type alias for a Result that can return an EventError
pub type EventResult<T> = Result<T, EventError>;

/// The Event Manager is the main trait, offering functions to send and consume events
#[async_trait]
pub trait EventManager {
    /// Send an event optionally related to a tenant (use empty string for no tenant)
    async fn send<T>(&mut self, tenant: &str, t: T) -> EventResult<()>
    where
        T: EventType + Serialize + Send;

    /// Add a consumer to handle specific events, optionally for a specific tenant (use empty string for no tenant)
    fn add_consumer<T, C>(&mut self, tenant: &str, c: C) -> EventResult<ConsumerID>
    where
        T: EventType + 'static + Sync + Send + DeserializeOwned,
        C: Consumer<T> + 'static + Clone + Sync + Send;

    /// Remove a consumer given its ID
    fn remove_consumer(&mut self, cid: &ConsumerID) -> EventResult<()>;

    /// Close the manager and all the resources it holds
    fn close(&mut self) -> EventResult<()>;

    /// Clean the underlying system, usually to ensure tests start from a clean slate
    fn clean(&mut self) -> EventResult<()>;
}

/// Consumer Group trait, giving the group to use
///
/// Only one consumer within the same group will get an event
pub trait ConsumerGroup {
    /// the consumer group to use
    fn group() -> String;
}

/// Consumer trait defining what to do with the event
pub trait Consumer<T: EventType>: ConsumerGroup {
    fn consume(&self, t: GenericEvent<T>) -> Result<(), ()>;
}

/// General info on all events
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq, PartialOrd, Ord)]
pub struct EventInfo {
    /// Event Code
    pub code: String,
    /// Tenant or empty if none
    pub tenant: String,
    /// Creation timestamp
    pub created: SystemTime,
}

/// Generic event holds the event info + specific event structure
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq, PartialOrd, Ord)]
pub struct GenericEvent<T: EventType> {
    pub info: EventInfo,
    pub data: T,
}

/// Generic event helper functions
impl<T: EventType + Serialize> GenericEvent<T> {
    /// Create a new Generic Event
    pub fn new(tenant: &str, data: T) -> Self {
        Self {
            info: EventInfo {
                code: T::code(),
                tenant: tenant.to_owned(),
                created: SystemTime::now(),
            },
            data,
        }
    }

    /// Convert event to binary payload
    pub fn payload(&self) -> Result<Vec<u8>, EventError> {
        serde_json::to_vec(self).map_err(|e| EventError::SerializationError(e.to_string()))
    }
}

/// Generic event deserialize functions
impl<'a, T> GenericEvent<T>
where
    T: EventType + Deserialize<'a>,
{
    /// Deserialiez from binary payload
    pub fn from_payload(payload: &'a [u8]) -> Result<Self, EventError> {
        serde_json::from_slice(payload)
            .map_err(|se| EventError::DeserializationError(se.to_string()))
    }
}

extern crate serde;
extern crate serde_json;

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::time::SystemTime;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EventError {
    ConnectionError(String),
    SetupError(String),
    SerializationError(String),
    DeserializationError(String),
    SendError(String),
    CloseError(String),
    AckError(String),
    OtherError(String),
    NoConsumeError,
}

pub trait EventType {
    fn code() -> String;
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ConsumerID(pub u64);

pub trait EventManager {

    fn send<T>(&mut self, otenant: Option<&str>,t: T) -> Result<(),EventError>
        where T: EventType + Serialize;

    fn add_consumer<T,C>(&mut self, otenant:Option<&str>, c: C)
        -> Result<ConsumerID,EventError>
        where T: EventType + 'static + Clone + Sync + Send + DeserializeOwned,
            C: Consumer<T> + 'static + Clone + Sync + Send;

    fn close(&mut self)-> Result<(),EventError>;
}


pub trait ConsumerGroup {

    fn group() -> String;
}

pub trait Consumer<T: EventType> : ConsumerGroup {

    fn consume(&self, t: GenericEvent<T>) -> Result<(),()>;
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct EventInfo {
    pub code: String,
    pub tenant: String,
    pub created: SystemTime,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct GenericEvent<T: EventType> {
    pub info: EventInfo,
    pub data: T,
}

impl<T: EventType+Serialize> GenericEvent<T> {
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

    pub fn payload(&self) -> Result<Vec<u8>,EventError> {
        serde_json::to_vec(self).map_err(|e| EventError::SerializationError(e.to_string()))
    }
}

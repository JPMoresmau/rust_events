use super::*;
use rust_events_derive::*;
use serde::{Deserialize, Serialize};

#[macro_export]
macro_rules! event_tests {
    ($mgr:expr) => {
        
            use rust_events::*;
            
            #[test]
            fn test_open_close() -> Result<(),EventError>{
                let mut mgr = $mgr?;
                mgr.close()
            }
            
    };
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, EventType, Eq, PartialEq)]
pub struct StringEvent {
    pub message: String,
}

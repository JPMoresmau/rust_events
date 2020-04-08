//! # Test harness
//! 
//! Maybe this could be in a separate crate, but hey
//! 
//! These tests should pass on any implementation of EventManager

use super::*;
use rust_events_derive::*;
use serde::{Deserialize, Serialize};
use std::sync::{Mutex, Arc};
use std::{thread,time};
use log::{error};

/// Macro generating the tests from the given expression yielding an EventManager
#[macro_export]
macro_rules! event_tests {
    ($mgr:expr) => { event_tests!($mgr, 5, 1);};
    ($mgr:expr, $delay:expr, $wait:expr) => {

            use rust_events::*;
            use rust_events::harness::*;
            use std::sync::{Mutex, Arc};
            use std::{thread,time};
            use log::{trace};
            use futures::executor;

            fn init_logger() {
                let _ = env_logger::builder().is_test(true).try_init();
            }

            #[test]
            fn test_open_close() -> Result<(),EventError>{
                init_logger();
                let mut mgr = $mgr?;
                mgr.close()
            }
            
            #[test]
            fn test_1_tenant_1_tenant()  -> EventResult<()> {
                init_logger();
                let f = async {
                    let mut mgr = $mgr?;
                    let events = Arc::new(Mutex::new(Vec::new()));
                    mgr.add_consumer("tenant1", StringAccumulateConsumer{accum:Arc::clone(&events)})?;
                    // depending on the underlying system, we cannot be sure messages from a previous tests are not going to be sent, so let's purge the old messages
                    thread::sleep(time::Duration::from_millis($wait*1000));
                    events.lock().unwrap().clear();
                    mgr.send("tenant1", StringEvent{message:"1_tenant_1_tenant_1".to_owned()}).await?;
                    mgr.send("tenant1", StringEvent{message:"1_tenant_1_tenant_2".to_owned()}).await?;
                    assert!(wait_for_condition($delay,|| events.lock().unwrap().len()==2),"events vector not filled: {:?}",events.lock().unwrap());
                    {
                        let mut v = events.lock().unwrap();
                        assert_eq!(2,v.len());
                        v.sort();
                        assert_eq!("1_tenant_1_tenant_1",&v[0].data.message);
                        assert_eq!("1_tenant_1_tenant_2",&v[1].data.message);
                        assert_eq!("tenant1",&v[0].info.tenant);
                        assert_eq!("tenant1",&v[1].info.tenant);
                        assert_eq!("StringEvent",&v[0].info.code);
                        assert_eq!("StringEvent",&v[1].info.code);
                    }
                    mgr.clean()?;
                    mgr.close()?;
                    Ok::<(),EventError>(())
                };
                executor::block_on(f)
            }

            #[test]
            fn test_1_tenant_2_tenant()  -> EventResult<()>{
                init_logger();
                let f = async {
                    let mut mgr = $mgr?;
                    let events = Arc::new(Mutex::new(Vec::new()));
                    mgr.add_consumer("tenant1", StringAccumulateConsumer{accum:Arc::clone(&events)})?;
                    mgr.add_consumer("tenant1", StringAccumulateConsumer{accum:Arc::clone(&events)})?;
                    // depending on the underlying system, we cannot be sure messages from a previous tests are not going to be sent, so let's purge the old messages
                    thread::sleep(time::Duration::from_millis($wait*1000));
                    events.lock().unwrap().clear();
                    mgr.send("tenant1", StringEvent{message:"1_tenant_2_tenant_1".to_owned()}).await?;
                    mgr.send("tenant1", StringEvent{message:"1_tenant_2_tenant_2".to_owned()}).await?;
                    assert!(wait_for_condition($delay,|| events.lock().unwrap().len()==2),"events vector not filled: {:?}",events.lock().unwrap());
                    {
                        let mut v = events.lock().unwrap();
                        assert_eq!(2,v.len());
                        v.sort();
                        assert_eq!("1_tenant_2_tenant_1",&v[0].data.message);
                        assert_eq!("1_tenant_2_tenant_2",&v[1].data.message);
                        assert_eq!("tenant1",&v[0].info.tenant);
                        assert_eq!("tenant1",&v[1].info.tenant);
                        assert_eq!("StringEvent",&v[0].info.code);
                        assert_eq!("StringEvent",&v[1].info.code);
                        
                    }
                    mgr.clean()?;
                    mgr.close()?;
                    Ok::<(),EventError>(())
                };
                executor::block_on(f)
            }

            #[test]
            fn test_1_shared_1_shared()  -> EventResult<()>{
                init_logger();
                let f = async {
                    let mut mgr = $mgr?;
                    let events = Arc::new(Mutex::new(Vec::new()));
                    mgr.add_consumer("", StringAccumulateConsumer{accum:Arc::clone(&events)})?;
                    // depending on the underlying system, we cannot be sure messages from a previous tests are not going to be sent, so let's purge the old messages
                    thread::sleep(time::Duration::from_millis($wait*1000));
                    events.lock().unwrap().clear();
                    mgr.send("", StringEvent{message:"1_shared_1_shared_1".to_owned()}).await?;
                    mgr.send("", StringEvent{message:"1_shared_1_shared_2".to_owned()}).await?;
                    assert!(wait_for_condition($delay,|| events.lock().unwrap().len()==2),"events vector not filled: {:?}",events.lock().unwrap());
                    {
                        let mut v = events.lock().unwrap();
                        assert_eq!(2,v.len());
                        v.sort();
                        assert_eq!("1_shared_1_shared_1",&v[0].data.message);
                        assert_eq!("1_shared_1_shared_2",&v[1].data.message);
                        assert_eq!("",&v[0].info.tenant);
                        assert_eq!("",&v[1].info.tenant);
                        assert_eq!("StringEvent",&v[0].info.code);
                        assert_eq!("StringEvent",&v[1].info.code);
                        
                    }
                    mgr.clean()?;
                    mgr.close()?;
                    Ok::<(),EventError>(())
                };
                executor::block_on(f)
            }

            #[test]
            fn test_2_tenant_1_shared() -> EventResult<()>{
                init_logger();
                let f = async {
                    let mut mgr = $mgr?;
                    let events = Arc::new(Mutex::new(Vec::new()));
                    mgr.add_consumer("", StringAccumulateConsumer{accum:Arc::clone(&events)})?;
                    // depending on the underlying system, we cannot be sure messages from a previous tests are not going to be sent, so let's purge the old messages
                    thread::sleep(time::Duration::from_millis($wait*1000));
                    events.lock().unwrap().clear();
                    mgr.send("tenant1", StringEvent{message:"2_tenant_1_shared_1".to_owned()}).await?;
                    mgr.send("tenant2", StringEvent{message:"2_tenant_1_shared_2".to_owned()}).await?;
                    assert!(wait_for_condition($delay,|| events.lock().unwrap().len()==2),"events vector not filled: {:?}",events.lock().unwrap());
                    {
                        let mut v = events.lock().unwrap();
                        assert_eq!(2,v.len());
                        v.sort();
                        assert_eq!("2_tenant_1_shared_1",&v[0].data.message);
                        assert_eq!("2_tenant_1_shared_2",&v[1].data.message);
                        assert_eq!("tenant1",&v[0].info.tenant);
                        assert_eq!("tenant2",&v[1].info.tenant);
                        assert_eq!("StringEvent",&v[0].info.code);
                        assert_eq!("StringEvent",&v[1].info.code);
                        
                    }
                    mgr.clean()?;
                    mgr.close()?;
                    Ok::<(),EventError>(())
                };
                executor::block_on(f)
            }


    };
}

/// Wait up to max_seconds for the condition to be true
/// The condition is given by the result of the cond closure
pub fn wait_for_condition<F>(max_seconds:u32, cond:F) -> bool 
    where F: Fn() -> bool {
    for _a in 0..max_seconds*10 {
        if cond() {
            return true;
        }
        thread::sleep(time::Duration::from_millis(100));
    }
    false
}

/// A very simple event with a simple String content
#[derive(Debug, Default, Clone, Serialize, Deserialize, EventType, Eq, PartialEq, PartialOrd, Ord)]
pub struct StringEvent {
    pub message: String,
}

/// A consumer that accumulates string events into a shared Vec
#[derive(Clone, Debug, ConsumerGroup)]
pub struct StringAccumulateConsumer {
    pub accum: Arc<Mutex<Vec<GenericEvent<StringEvent>>>>,
}

/// Implementation of Consumer
impl Consumer<StringEvent> for StringAccumulateConsumer {

    fn consume(&self, t: GenericEvent<StringEvent>) -> Result<(),()>{
        {
            let rv=self.accum.lock();
            match rv {
                Ok(mut v) => v.push(t),
                Err(e) => {
                    error!("Consumer error: {}",e);
                    return Err(());
                },
            } 
            
        }
        Ok(())
    }
}

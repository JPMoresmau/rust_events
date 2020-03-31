use super::*;
use rust_events_derive::*;
use serde::{Deserialize, Serialize};
use std::sync::{Mutex, Arc};
use std::{thread,time};

#[macro_export]
macro_rules! event_tests {
    ($mgr:expr) => {
        
            use rust_events::*;
            use rust_events::harness::*;
            use std::sync::{Mutex, Arc};
            use std::{thread,time};

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
            fn test_1_tenant_1_tenant()  -> Result<(),EventError>{
                init_logger();
                let mut mgr = $mgr?;
                let events = Arc::new(Mutex::new(Vec::new()));
                mgr.add_consumer(Some("tenant1"), StringAccumulateConsumer{accum:Arc::clone(&events)})?;
                mgr.send(Some("tenant1"), StringEvent{message:"1_tenant_1_tenant_1".to_owned()})?;
                mgr.send(Some("tenant1"), StringEvent{message:"1_tenant_1_tenant_2".to_owned()})?;
                assert!(wait_for_condition(5,|| events.lock().unwrap().len()==2),"events vector not filled");
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
                mgr.close()
            }

            #[test]
            fn test_1_tenant_2_tenant()  -> Result<(),EventError>{
                init_logger();
                let mut mgr = $mgr?;
                let events = Arc::new(Mutex::new(Vec::new()));
                mgr.add_consumer(Some("tenant1"), StringAccumulateConsumer{accum:Arc::clone(&events)})?;
                mgr.add_consumer(Some("tenant1"), StringAccumulateConsumer{accum:Arc::clone(&events)})?;
                mgr.send(Some("tenant1"), StringEvent{message:"1_tenant_2_tenant_1".to_owned()})?;
                mgr.send(Some("tenant1"), StringEvent{message:"1_tenant_2_tenant_2".to_owned()})?;
                assert!(wait_for_condition(5,|| events.lock().unwrap().len()==2),"events vector not filled");
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
                mgr.close()
            }

            #[test]
            fn test_1_shared_1_shared()  -> Result<(),EventError>{
                init_logger();
                let mut mgr = $mgr?;
                let events = Arc::new(Mutex::new(Vec::new()));
                mgr.add_consumer(None, StringAccumulateConsumer{accum:Arc::clone(&events)})?;
                mgr.send(None, StringEvent{message:"1_shared_1_shared_1".to_owned()})?;
                mgr.send(None, StringEvent{message:"1_shared_1_shared_2".to_owned()})?;
                assert!(wait_for_condition(5,|| events.lock().unwrap().len()==2),"events vector not filled");
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
                mgr.close()
            }

            #[test]
            fn test_2_tenant_1_shared()  -> Result<(),EventError>{
                init_logger();
                let mut mgr = $mgr?;
                let events = Arc::new(Mutex::new(Vec::new()));
                mgr.add_consumer(None, StringAccumulateConsumer{accum:Arc::clone(&events)})?;
                mgr.send(Some("tenant1"), StringEvent{message:"2_tenant_1_shared_1".to_owned()})?;
                mgr.send(Some("tenant2"), StringEvent{message:"2_tenant_1_shared_2".to_owned()})?;
                assert!(wait_for_condition(5,|| events.lock().unwrap().len()==2),"events vector not filled");
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
                mgr.close()
            }


    };
}

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

#[derive(Debug, Default, Clone, Serialize, Deserialize, EventType, Eq, PartialEq, PartialOrd, Ord)]
pub struct StringEvent {
    pub message: String,
}

#[derive(Clone, Debug, ConsumerGroup)]
pub struct StringAccumulateConsumer {
    pub accum: Arc<Mutex<Vec<GenericEvent<StringEvent>>>>,
}

impl Consumer<StringEvent> for StringAccumulateConsumer {

    fn consume(&self, t: GenericEvent<StringEvent>) -> Result<(),()>{
        {
            let mut v=self.accum.lock().unwrap();
            v.push(t);
        }
        Ok(())
    }
}

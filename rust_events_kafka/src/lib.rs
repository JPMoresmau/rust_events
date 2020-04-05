use rdkafka::config::ClientConfig;
use rdkafka::message::{Message, OwnedHeaders};
use rdkafka::consumer::{StreamConsumer,CommitMode};
use rdkafka::consumer::Consumer as KafkaConsumer;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::{Timeout};

use std::collections::HashMap;
use serde::{Serialize,de::DeserializeOwned,de::Deserialize};
use rust_events::{Consumer,ConsumerID,EventError,EventInfo,EventType,EventManager,GenericEvent};
use core::time::Duration;
use std::time::SystemTime;
use futures::executor::ThreadPool;
use futures::executor;
use futures::StreamExt;
use std::sync::{Arc};
use log::{info, trace, error};

pub struct KafkaEventManager {
    /// status is open?
    opened: bool,

    params: HashMap<String,String>,
    thread_pool: ThreadPool,
    producer: FutureProducer,
     /// opened consumers by ID
    consumers: HashMap<ConsumerID,Arc<StreamConsumer>>,
}

impl KafkaEventManager {
    pub fn new(params: &HashMap<&str,&str>)  -> Result<Self,EventError> {
        let thread_pool = ThreadPool::new().map_err(|le| EventError::SetupError(le.to_string()))?;
        let producer:FutureProducer = params.iter().fold( &mut ClientConfig::new(), |cc,(k,v)| {
            cc.set(k,v)
        }).create().map_err(|le| EventError::ConnectionError(le.to_string()))?;

       let ownparams = params.iter().map(|(k,v)| ((*k).to_owned(),(*v).to_owned())).collect();
       Ok(KafkaEventManager{opened:true,params:ownparams
            , thread_pool, producer,consumers:HashMap::new()})

    }

    fn consume<'a,M:Message,T,C>(m: &'a M,c:&C,tenant:&String) -> Result<(),EventError>
        where T: EventType + 'static + Sync + Send + Deserialize<'a>,
            C: Consumer<T> + 'static + Clone + Sync + Send {
        match m.payload() {
            Some(p) => {
                let ge =GenericEvent::from_payload(&p)?;
                if tenant.is_empty() || ge.info.tenant==*tenant {
                    trace!("on delivery: {}:{}.{}",C::group(),ge.info.code,ge.info.tenant);
                    c.consume(ge).map_err(|_| EventError::NoConsumeError)?;
                }
            },
            _ => {

            },
        };

        Ok(())
    }
}

impl EventManager for KafkaEventManager {
    fn send<T>(&mut self, tenant: &str,t: T) -> Result<(),EventError>
        where T: EventType + Serialize{
            let code = T::code();
            let ge=GenericEvent{info:EventInfo {
                code: code.clone(),
                tenant: tenant.to_owned(),
                created: SystemTime::now(),
            },data:t};
            let payload = ge.payload()?;
            let fr:FutureRecord<(),Vec<u8>> = FutureRecord::to(&code)
                .payload(&payload)
                .headers(OwnedHeaders::new().add("tenant",&ge.info.tenant));
            let df = self.producer.send(fr, 0);
            executor::block_on(df).map_err(|_| EventError::SendError("Cancelled".to_owned()))
                .and_then(|r| r.map_err(|(ke,_)| EventError::SendError(ke.to_string())))
                .map(|_| ())
           
    }

    fn add_consumer<T,C>(&mut self, tenant:&str, c: C)
    -> Result<ConsumerID,EventError>
    where T: EventType + 'static + Sync + Send + DeserializeOwned,
        C: Consumer<T> + 'static + Clone + Sync + Send {
            let code = T::code();
            let group =
                if tenant.is_empty() {
                    C::group()
                } else {
                    format!("{}.{}",C::group(),tenant)
                };

            let consumer:StreamConsumer = self.params.iter().fold( &mut ClientConfig::new(), |cc,(k,v)| {
                cc.set(k,v)
            }).set("group.id", &group).set("enable.auto.commit", "false").create().map_err(|le| EventError::ConnectionError(le.to_string()))?;

            consumer.subscribe(&[&code]).map_err(|le| EventError::SetupError(le.to_string()))?;
            let amc = Arc::new(consumer);
            let amc2 = Arc::clone(&amc);

            let ot=tenant.to_owned();
            self.thread_pool.spawn_ok( async move {
                let mut ms = amc2.start();
                while let Some(message) = ms.next().await {
                    match message {
                        Err(e) => error!("{:?}", EventError::ReceiveError(e.to_string())),
                        Ok(m) => {
                            KafkaEventManager::consume(&m, &c, &ot).unwrap_or_else(|ee| error!("{:?}",ee));
                          
                            amc2.commit_message(&m, CommitMode::Async).unwrap();
                        }
                    } ;
                }
            });
            
            // generate key
            let mut id = ConsumerID(self.consumers.len() as u64);
            while self.consumers.contains_key(&id){
                id.0+=1;
            }
            self.consumers.insert(id.clone(), amc);
            Ok(id)

        }

    fn close(&mut self) -> Result<(),EventError>{
        if self.opened {
            self.opened=false;
            info!("Closing KafkaEventManager");
            self.producer.flush(Timeout::After(Duration::from_secs(10)));
            self.consumers.values().for_each(|c| c.stop());
            
            self.consumers.clear();
        }
        Ok(())
    }

    fn clean(&mut self) -> Result<(),EventError>{
        Ok(())
    }
}
//! Implementation of Event Manager for Kakfka
use rdkafka::config::ClientConfig;
use rdkafka::message::{Message, OwnedHeaders};
use rdkafka::consumer::{StreamConsumer,CommitMode};
use rdkafka::consumer::Consumer as KafkaConsumer;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::{Timeout};

use std::collections::HashMap;
use serde::{Serialize,de::DeserializeOwned,de::Deserialize};
use rust_events::{Consumer,ConsumerID,EventError,EventInfo,EventType,EventManager,GenericEvent, EventResult};
use core::time::Duration;
use std::time::SystemTime;
use futures::executor::ThreadPool;
use futures::StreamExt;
use std::sync::{Arc};
use log::{info, trace, error};
use async_trait::async_trait;

/// Kafka struct
pub struct KafkaEventManager {
    /// status is open?
    opened: bool,
    /// connection and client parameters
    params: HashMap<String,String>,
    /// thread pool for asynchronous consuming
    thread_pool: ThreadPool,
    /// producer
    producer: FutureProducer,
     /// opened consumers by ID
    consumers: HashMap<ConsumerID,Arc<StreamConsumer>>,
}

/// Kafka specific methods
impl KafkaEventManager {
    /// New instance from a map of parameters
    pub fn new(params: &HashMap<&str,&str>)  -> Result<Self,EventError> {
        let thread_pool = ThreadPool::new().map_err(|le| EventError::SetupError(le.to_string()))?;
        let producer:FutureProducer = params.iter().fold( &mut ClientConfig::new(), |cc,(k,v)| {
            cc.set(k,v)
        }).create().map_err(|le| EventError::ConnectionError(le.to_string()))?;
       // keep a copy of parameters for consumers
       let ownparams = params.iter().map(|(k,v)| ((*k).to_owned(),(*v).to_owned())).collect();
       Ok(KafkaEventManager{opened:true,params:ownparams
            , thread_pool, producer,consumers:HashMap::new()})

    }

    /// Consume a message
    fn consume<'a,M:Message,T,C>(m: &'a M,c:&C,tenant:&String) -> Result<(),EventError>
        where T: EventType + 'static + Sync + Send + Deserialize<'a>,
            C: Consumer<T> + 'static + Clone + Sync + Send {
        match m.payload() {
            Some(p) => {
                let ge =GenericEvent::from_payload(&p)?;
                // we use the same topic for all tenants, so let's do the filtering here
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

/// Trait implementation
#[async_trait]
impl EventManager for KafkaEventManager {
    /// Send a message
    async fn send<T>(&mut self, tenant: &str,t: T) -> EventResult<()>
        where T: EventType + Serialize + Send {
            let code = T::code();
            let ge=GenericEvent{info:EventInfo {
                code: code.clone(),
                tenant: tenant.to_owned(),
                created: SystemTime::now(),
            },data:t};
            let payload = ge.payload()?;
            // one topic for all tenants
            let fr:FutureRecord<(),Vec<u8>> = FutureRecord::to(&code)
                .payload(&payload)
                .headers(OwnedHeaders::new().add("tenant",&ge.info.tenant));
            self.producer.send(fr, 0).await.map_err(|_| EventError::SendError("Cancelled".to_owned()))
                .and_then(|r| r.map_err(|(ke,_)| EventError::SendError(ke.to_string())))
                .map(|_| ())
           
    }

    /// Add a consumer
    fn add_consumer<T,C>(&mut self, tenant:&str, c: C)
    -> Result<ConsumerID,EventError>
    where T: EventType + 'static + Sync + Send + DeserializeOwned,
        C: Consumer<T> + 'static + Clone + Sync + Send {
            let code = T::code();
            // group is tenant specific
            let group =
                if tenant.is_empty() {
                    C::group()
                } else {
                    format!("{}.{}",C::group(),tenant)
                };

            // we'll do our own commits since StreamConsumer advises it
            let consumer:StreamConsumer = self.params.iter().fold( &mut ClientConfig::new(), |cc,(k,v)| {
                cc.set(k,v)
            }).set("group.id", &group).set("enable.auto.commit", "false").create().map_err(|le| EventError::ConnectionError(le.to_string()))?;

            consumer.subscribe(&[&code]).map_err(|le| EventError::SetupError(le.to_string()))?;
            let amc = Arc::new(consumer);
            let amc2 = Arc::clone(&amc);

            let ot=tenant.to_owned();
            // spawn reading messages from the stream and consuming then
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

    /// Close producer and consumers
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

    /// NOOP for Kafka for now. Should we try to delete the topics we created?
    fn clean(&mut self) -> Result<(),EventError>{
        Ok(())
    }
}
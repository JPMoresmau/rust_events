use lapin::{
    message::DeliveryResult, options::*, types::{FieldTable,AMQPValue}, BasicProperties, Channel, Connection, message::Delivery, 
    ConnectionProperties, ConsumerDelegate,ExchangeKind,
};

use std::collections::{HashMap,HashSet};
use serde::{Serialize,de::DeserializeOwned,de::Deserialize};
use rust_events::{Consumer,ConsumerID,EventError,EventInfo,EventType,EventManager,GenericEvent};
use std::time::SystemTime;
use std::marker::PhantomData;
use log::{info, trace, error};

pub struct RabbitMQEventManager {
    opened: bool,
    connection: Connection,
    channel: Channel,
    exchanges: HashSet<String>,
    queues: HashSet<(String,String)>,
    consumers: HashMap<ConsumerID,lapin::Consumer>,
}

impl RabbitMQEventManager {
    pub fn new(uri: &str, options: ConnectionProperties) -> Result<Self,EventError> {
        Connection::connect(uri, options)
            .wait()
            .and_then(|connection| {
                connection.create_channel().wait().map(|channel| RabbitMQEventManager {opened:true,connection,channel,
                    exchanges: HashSet::new(),queues: HashSet::new(), consumers: HashMap::new()}) 
            })
            .map_err(|le| EventError::ConnectionError(le.to_string()))
      
    }

    fn exchange_declare(&mut self, code: &str, kind: ExchangeKind) -> Result<(),EventError>{
        if !self.exchanges.contains(code){

            self.channel.exchange_declare(code, kind, ExchangeDeclareOptions::default(), FieldTable::default())
                .wait()
                .map_err(|le| EventError::SetupError(le.to_string()))?;
            self.exchanges.insert(code.to_owned());
        }
        Ok(())
    }

    


}

impl Drop for RabbitMQEventManager {
    fn drop(&mut self) {
        self.close().unwrap_or_else(|e| error!("Cannot close RabbitMQEventManager: {:?}",e));
    }
}

impl EventManager for RabbitMQEventManager {

    fn send<T>(&mut self, otenant: Option<&str>,t: T) -> Result<(),EventError>
        where T: EventType + Serialize{
        let code = T::code();
        // topic exchange to route events properly
        self.exchange_declare(&code, ExchangeKind::Topic)?;
        let mut routing_key=code.clone();
        routing_key.push_str(".");
        if let Some(tenant) = otenant {
           routing_key.push_str(tenant);
        }
        let ge=GenericEvent{info:EventInfo{
            code: code.clone(),
            tenant: otenant.map(|s| s.to_owned()).unwrap_or_default(),
            created: SystemTime::now(),
        },data:t};
        ge.payload().and_then(|v| self.channel.basic_publish(&code,&routing_key, BasicPublishOptions::default(),v,BasicProperties::default())
            .wait()
            .map_err(|le| EventError::SendError(le.to_string())))
    }

    fn add_consumer<T,C>(&mut self, otenant:Option<&str>, c: C)
    -> Result<ConsumerID,EventError>
    where T: EventType + 'static + Sync + Send + DeserializeOwned,
        C: Consumer<T> + 'static + Clone + Sync + Send {
        let code = T::code();

        // topic exchange to route events properly
        self.exchange_declare(&code, ExchangeKind::Topic)?;

        let group =
            if let Some(tenant) = otenant {
                format!("{}.{}",C::group(),tenant)
            } else {
                C::group()
            };

        let routing_key=
            if let Some(tenant) = otenant {
                format!("{}.{}",code,tenant)
            } else {
                format!("{}.*",code)
            };
        let key = (group.clone(),routing_key.clone());
        if !self.queues.contains(&key){
            self.queues.insert(key);
            // fanout exchange specific to this group
            self.exchange_declare(&group, ExchangeKind::Fanout)?;

            // bind publisher topic exchange to consumer fanout exchange
            // destination comes before source!
            self.channel.exchange_bind(&group,&code,&routing_key,ExchangeBindOptions::default(), FieldTable::default())
                .wait()
                .map_err(|le| EventError::SetupError(le.to_string()))?;
                
            let mut ft=FieldTable::default();
            ft.insert("x-expires".into(),AMQPValue::LongUInt(600000));

            self.channel.queue_declare(&group,
                QueueDeclareOptions::default(),
                ft
                )
                .wait()
                .map_err(|le| EventError::SetupError(le.to_string()))?;

            // fanout exchange to queue
        
            self.channel.queue_bind(&group,&group,"",QueueBindOptions::default(), FieldTable::default())
                .wait()
                .map_err(|le| EventError::SetupError(le.to_string()))?;
        }
        let lc = self.channel.basic_consume(
            &group,
            "",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .wait()
        .map_err(|le| EventError::SetupError(le.to_string()))?;
        
        lc.set_delegate(Box::new(Subscriber { channel: self.channel.clone(),consumer:c,event_type:PhantomData}));

        let mut id = ConsumerID(self.consumers.len() as u64);
        while self.consumers.contains_key(&id){
            id.0+=1;
        }
        self.consumers.insert(id.clone(), lc);
        Ok(id)
       
    }

    fn close(&mut self) -> Result<(),EventError>{
        if self.opened {
            self.opened=false;
            info!("Closing RabbitMQEvent Manager");
            
            self.exchanges.clear();
            self.queues.clear();
            self.consumers.clear();
            self.channel.close(200, "OK").wait().map_err(|le| EventError::CloseError(le.to_string()))?;
            self.connection.close(200, "OK").wait().map_err(|le| EventError::CloseError(le.to_string()))?;
        }
        Ok(())
    }

    fn clean(&mut self) -> Result<(),EventError>{
        self.queues.iter().try_for_each(|(group, _rk)|{
            self.channel.queue_delete(&group,QueueDeleteOptions::default()).wait().map(|_u| ()).map_err(|le| EventError::CleanError(le.to_string()))
        })?;
        self.exchanges.iter().try_for_each(|e| {
            self.channel.exchange_delete(e,ExchangeDeleteOptions::default()).wait().map_err(|le| EventError::CleanError(le.to_string()))
        })?;
        Ok(())
    }
}

#[derive(Clone)]
struct Subscriber<T: EventType + Sync + Send, C: Consumer<T> + Clone + Sync + Send>  {
    channel: Channel,
    consumer: C,
    event_type: PhantomData<T>,
}

impl <'a, T, C: Consumer<T> + Clone + Sync + Send> Subscriber<T,C>
    where T: EventType + Sync + Send+ Deserialize<'a> {
    fn on_delivery(&self, delivery: &'a Delivery) -> Result<(),EventError> {
        
        let ge =GenericEvent::from_payload(&delivery.data)?;
        trace!("on delivery: {}:{}",C::group(),delivery.routing_key);
        self.consumer.consume(ge).map_err(|_| EventError::NoConsumeError)?;

        self.channel
            .basic_ack(delivery.delivery_tag, BasicAckOptions::default())
            .wait()
            .map_err(|le| EventError::AckError(le.to_string()))?;
        Ok(())
    }
}

impl <T, C: Consumer<T> + Clone + Sync + Send> ConsumerDelegate for Subscriber<T,C>
    where T: EventType + Sync + Send+ DeserializeOwned {
    fn on_new_delivery(&self, delivery: DeliveryResult) {
        if let Ok(Some(delivery)) = delivery {
            self.on_delivery(&delivery).unwrap();
        }
    }

   
}
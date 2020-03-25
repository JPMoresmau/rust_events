use lapin::{
    message::DeliveryResult, options::*, types::FieldTable, BasicProperties, Channel, Connection, message::Delivery, 
    ConnectionProperties, ConsumerDelegate,ExchangeKind,
};

use std::collections::{HashMap,HashSet};
use serde::{Serialize,de::DeserializeOwned};
use super::{Consumer,ConsumerID,EventError,EventInfo,EventType,EventManager,GenericEvent};
use std::time::SystemTime;
use std::marker::PhantomData;

pub struct RabbitMQEventManager {
    connection: Connection,
    channel: Channel,
    exchanges: HashSet<String>,
    consumers: HashMap<ConsumerID,lapin::Consumer>,
}

impl RabbitMQEventManager {
    pub fn new(uri: &str, options: ConnectionProperties) -> Result<Self,EventError> {
        Connection::connect(uri, options)
            .wait()
            .and_then(|connection| {
                connection.create_channel().wait().map(|channel| RabbitMQEventManager {connection,channel,exchanges: HashSet::new(), consumers: HashMap::new()}) 
            })
            .map_err(|le| EventError::ConnectionError(le.to_string()))
      
    }

    fn exchange_declare(&mut self, code: &str) -> Result<(),EventError>{
        if !self.exchanges.contains(code){

            self.channel.exchange_declare(code, ExchangeKind::Topic, ExchangeDeclareOptions::default(), FieldTable::default())
                .wait()
                .map_err(|le| EventError::SetupError(le.to_string()))?;
            self.exchanges.insert(code.to_owned());
        }
        Ok(())
    }
}

impl EventManager for RabbitMQEventManager {


    fn send<T>(&mut self, otenant: Option<&str>,t: T) -> Result<(),EventError>
        where T: EventType + Serialize{
        let code = T::code();
        self.exchange_declare(&code)?;
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
    where T: EventType + 'static + Clone + Sync + Send + DeserializeOwned,
        C: Consumer<T> + 'static + Clone + Sync + Send {
        let code = T::code();

        self.exchange_declare(&code)?;

        let mut group = C::group();
        if let Some(tenant) = otenant {
            group.push_str(".");
            group.push_str(tenant);
        } 

        self.channel.queue_declare(&group,
            QueueDeclareOptions::default(),
            FieldTable::default())
            .wait()
            .map_err(|le| EventError::SetupError(le.to_string()))?;
        let mut routing_key=code.clone();
        if let Some(tenant) = otenant {
            routing_key.push_str(".");
            routing_key.push_str(tenant);
        } else {
            routing_key.push_str(".#");
        }

        self.channel.queue_bind(&group,&code,&routing_key,QueueBindOptions::default(), FieldTable::default())
            .wait()
            .map_err(|le| EventError::SetupError(le.to_string()))?;

        let lc = self.channel.basic_consume(
            &group,
            &group,
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
        //.set_delegate(Box::new(Subscriber { channel: self.channel.clone() }));

    }

    fn close(&mut self) -> Result<(),EventError>{
        /* for c in self.consumers.iter(){
            c.1..close(0,"bye").wait()?;
        }*/
        self.consumers.clear();
        self.channel.close(0,"bye").wait().map_err(|le| EventError::CloseError(le.to_string()))?;
        self.connection.close(0,"bye").wait().map_err(|le| EventError::CloseError(le.to_string()))?;
        Ok(())
    }
}

#[derive(Clone)]
struct Subscriber<T: EventType + Clone + Sync + Send, C: Consumer<T> + Clone + Sync + Send>  {
    channel: Channel,
    consumer: C,
    event_type: PhantomData<T>,
}

impl <T: EventType + Clone + Sync + Send+ DeserializeOwned, C: Consumer<T> + Clone + Sync + Send> Subscriber<T,C> {
    fn on_delivery(&self, delivery: Delivery) -> Result<(),EventError> {

        let ge = serde_json::from_slice(&delivery.data)
            .map_err(|se| EventError::DeserializationError(se.to_string()))?;
        self.consumer.consume(ge).map_err(|_| EventError::NoConsumeError)?;

        self.channel
            .basic_ack(delivery.delivery_tag, BasicAckOptions::default())
            .wait()
            .map_err(|le| EventError::AckError(le.to_string()))?;
        Ok(())
    }
}

impl <T: EventType + Clone + Sync + Send+ DeserializeOwned, C: Consumer<T> + Clone + Sync + Send> ConsumerDelegate for Subscriber<T,C> {
    fn on_new_delivery(&self, delivery: DeliveryResult) {
        if let Ok(Some(delivery)) = delivery {
            self.on_delivery(delivery).expect("error");
        }
    }

   
}
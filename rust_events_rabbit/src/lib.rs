//! Implementation of Event Manager for RabbitMQ
use lapin::{
    message::Delivery,
    message::DeliveryResult,
    options::*,
    types::{AMQPValue, FieldTable},
    BasicProperties, Channel, Connection, ConnectionProperties, ConsumerDelegate, ExchangeKind,
};

use async_trait::async_trait;
use log::{error, info, trace};
use rust_events::{
    Consumer, ConsumerID, EventError, EventInfo, EventManager, EventResult, EventType, GenericEvent,
};
use serde::{de::Deserialize, de::DeserializeOwned, Serialize};
use std::collections::{HashMap, HashSet};
use std::marker::PhantomData;
use std::time::SystemTime;

/// EventManager structure for RabbitMQ
pub struct RabbitMQEventManager {
    /// channel is open?
    opened: bool,
    /// Connection to RabbitMQ
    connection: Connection,
    /// Channel to RabbitMQ
    channel: Channel,
    /// List of declared exchanges
    exchanges: HashSet<String>,
    /// List of declared queues (consumer group + routing key)
    queues: HashSet<(String, String)>,
    /// opened consumers by ID
    consumers: HashMap<ConsumerID, lapin::Consumer>,
}

impl RabbitMQEventManager {
    /// New Event Manager from AMQP connection properties
    pub fn new(uri: &str, options: ConnectionProperties) -> Result<Self, EventError> {
        Connection::connect(uri, options)
            .wait()
            .and_then(|connection| {
                connection
                    .create_channel()
                    .wait()
                    .map(|channel| RabbitMQEventManager {
                        opened: true,
                        connection,
                        channel,
                        exchanges: HashSet::new(),
                        queues: HashSet::new(),
                        consumers: HashMap::new(),
                    })
            })
            .map_err(|le| EventError::ConnectionError(le.to_string()))
    }

    /// Declare an exchange
    fn exchange_declare(&mut self, code: &str, kind: ExchangeKind) -> Result<(), EventError> {
        if !self.exchanges.contains(code) {
            // TODO make exchange options configurable
            self.channel
                .exchange_declare(
                    code,
                    kind,
                    ExchangeDeclareOptions::default(),
                    FieldTable::default(),
                )
                .wait()
                .map_err(|le| EventError::SetupError(le.to_string()))?;
            self.exchanges.insert(code.to_owned());
        }
        Ok(())
    }
}

/// Close on Drop
impl Drop for RabbitMQEventManager {
    fn drop(&mut self) {
        self.close()
            .unwrap_or_else(|e| error!("Cannot close RabbitMQEventManager: {:?}", e));
    }
}

/// Implementation of EventManager trait
#[async_trait]
impl EventManager for RabbitMQEventManager {
    /// Send an event to an Exchange called after the event code
    async fn send<T>(&mut self, tenant: &str, t: T) -> EventResult<()>
    where
        T: EventType + Serialize + Send,
    {
        let code = T::code();
        // topic exchange to route events properly
        self.exchange_declare(&code, ExchangeKind::Topic)?;
        let routing_key = format!("{}.{}", code, tenant);
        let ge = GenericEvent {
            info: EventInfo {
                code: code.clone(),
                tenant: tenant.to_owned(),
                created: SystemTime::now(),
            },
            data: t,
        };
        let v = ge.payload()?;

        self.channel
            .basic_publish(
                &code,
                &routing_key,
                BasicPublishOptions::default(),
                v,
                BasicProperties::default(),
            )
            .await
            .map_err(|le| EventError::SendError(le.to_string()))
    }

    /// Add a consumer by creating queues, exchanges and exchange binding
    fn add_consumer<T, C>(&mut self, tenant: &str, c: C) -> EventResult<ConsumerID>
    where
        T: EventType + 'static + Sync + Send + DeserializeOwned,
        C: Consumer<T> + 'static + Clone + Sync + Send,
    {
        let code = T::code();

        // topic exchange to route events properly
        self.exchange_declare(&code, ExchangeKind::Topic)?;

        let group = if tenant.is_empty() {
            C::group()
        } else {
            format!("{}.{}", C::group(), tenant)
        };

        let routing_key = if tenant.is_empty() {
            format!("{}.*", code)
        } else {
            format!("{}.{}", code, tenant)
        };
        let key = (group.clone(), routing_key.clone());
        if !self.queues.contains(&key) {
            self.queues.insert(key);
            // fanout exchange specific to this group
            // TODO makes this configurable
            self.exchange_declare(&group, ExchangeKind::Fanout)?;

            // bind publisher topic exchange to consumer fanout exchange
            // destination comes before source!
            self.channel
                .exchange_bind(
                    &group,
                    &code,
                    &routing_key,
                    ExchangeBindOptions::default(),
                    FieldTable::default(),
                )
                .wait()
                .map_err(|le| EventError::SetupError(le.to_string()))?;
            // Declare queue to expire in 10 minutes
            // TODO make this configurable
            let mut ft = FieldTable::default();
            ft.insert("x-expires".into(), AMQPValue::LongUInt(600000));

            // Queue
            self.channel
                .queue_declare(&group, QueueDeclareOptions::default(), ft)
                .wait()
                .map_err(|le| EventError::SetupError(le.to_string()))?;

            // fanout exchange to queue
            self.channel
                .queue_bind(
                    &group,
                    &group,
                    "",
                    QueueBindOptions::default(),
                    FieldTable::default(),
                )
                .wait()
                .map_err(|le| EventError::SetupError(le.to_string()))?;
        }
        // generate key
        let mut id = ConsumerID(self.consumers.len() as u64);
        while self.consumers.contains_key(&id) {
            id.0 += 1;
        }
        // register consumer
        let lc = self
            .channel
            .basic_consume(
                &group,
                &format!("consumer-{}", id.0),
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .wait()
            .map_err(|le| EventError::SetupError(le.to_string()))?;
        // add the provided consumer as delegate
        lc.set_delegate(Box::new(Subscriber {
            channel: self.channel.clone(),
            consumer: c,
            event_type: PhantomData,
        }));
        self.consumers.insert(id.clone(), lc);
        Ok(id)
    }

    /// Cancel consumer
    fn remove_consumer(&mut self, cid: &ConsumerID) -> EventResult<()> {
        if let None = self.consumers.remove(cid) {
            return Err(EventError::UnknownConsumerError(cid.clone()));
        }

        self.channel
            .basic_cancel(
                &format!("consumer-{}", cid.0),
                BasicCancelOptions::default(),
            )
            .wait()
            .map_err(|le| EventError::SetupError(le.to_string()))?;
        Ok(())
    }

    /// Close the connection and channel
    fn close(&mut self) -> EventResult<()> {
        if self.opened {
            self.opened = false;
            info!("Closing RabbitMQEvent Manager");
            self.exchanges.clear();
            self.queues.clear();
            self.consumers.clear();
            self.channel
                .close(200, "OK")
                .wait()
                .map_err(|le| EventError::CloseError(le.to_string()))?;
            self.connection
                .close(200, "OK")
                .wait()
                .map_err(|le| EventError::CloseError(le.to_string()))?;
        }
        Ok(())
    }

    /// Remove all declared exchanges and queues
    fn clean(&mut self) -> EventResult<()> {
        self.queues.iter().try_for_each(|(group, _rk)| {
            self.channel
                .queue_delete(&group, QueueDeleteOptions::default())
                .wait()
                .map(|_u| ())
                .map_err(|le| EventError::CleanError(le.to_string()))
        })?;
        self.exchanges.iter().try_for_each(|e| {
            self.channel
                .exchange_delete(e, ExchangeDeleteOptions::default())
                .wait()
                .map_err(|le| EventError::CleanError(le.to_string()))
        })?;
        Ok(())
    }
}

/// The underlying subscriber
#[derive(Clone)]
struct Subscriber<T: EventType + Sync + Send, C: Consumer<T> + Clone + Sync + Send> {
    channel: Channel,
    consumer: C,
    event_type: PhantomData<T>,
}

/// Helper methods for Subscriber
impl<'a, T, C: Consumer<T> + Clone + Sync + Send> Subscriber<T, C>
where
    T: EventType + Sync + Send + Deserialize<'a>,
{
    /// Extract event from payload and call consumer
    fn on_delivery(&self, delivery: &'a Delivery) -> Result<(), EventError> {
        let ge = GenericEvent::from_payload(&delivery.data)?;
        trace!("on delivery: {}:{}", C::group(), delivery.routing_key);
        self.consumer
            .consume(ge)
            .map_err(|_| EventError::NoConsumeError)?;
        // acknowledge delivery
        self.channel
            .basic_ack(delivery.delivery_tag, BasicAckOptions::default())
            .wait()
            .map_err(|le| EventError::AckError(le.to_string()))?;
        Ok(())
    }
}

/// ConsumerDelegate for Subscriber
impl<T, C: Consumer<T> + Clone + Sync + Send> ConsumerDelegate for Subscriber<T, C>
where
    T: EventType + Sync + Send + DeserializeOwned,
{
    fn on_new_delivery(&self, delivery: DeliveryResult) {
        if let Ok(Some(delivery)) = delivery {
            self.on_delivery(&delivery).unwrap();
        }
    }
}

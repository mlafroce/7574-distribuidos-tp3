use crate::messages::Message;
use crate::middleware::RabbitExchange;
use crate::Config;
use amiquip::{
    Channel, Connection, Consumer, ConsumerOptions, Exchange, ExchangeDeclareOptions, ExchangeType,
    Publish, QueueDeclareOptions, Result,
};
use log::{debug, error, info};
use std::cmp::Ordering;

pub struct RabbitConnection {
    connection: Connection,
    channel: Channel,
}

impl RabbitConnection {
    pub fn new(config: &Config) -> Result<Self> {
        let host_addr = format!(
            "amqp://{}:{}@{}:{}",
            config.user, config.pass, config.server_host, config.server_port
        );
        debug!("Connecting to: {}", host_addr);
        let mut connection = Connection::insecure_open(&host_addr)?;
        let channel = connection.open_channel(None)?;
        Ok(Self {
            connection,
            channel,
        })
    }

    pub fn get_consumer(&self, channel_id: &str) -> Result<Consumer> {
        let options = QueueDeclareOptions {
            auto_delete: false,
            ..QueueDeclareOptions::default()
        };
        let queue = self.channel.queue_declare(channel_id, options)?;
        queue.consume(ConsumerOptions::default())
    }

    pub fn get_direct_exchange(&self) -> Exchange {
        Exchange::direct(&self.channel)
    }

    pub fn get_named_exchange(&self, exchange: &str, type_: ExchangeType) -> Result<Exchange> {
        let exchange_options = ExchangeDeclareOptions {
            durable: true,
            ..ExchangeDeclareOptions::default()
        };
        self.channel
            .exchange_declare(type_, exchange, exchange_options)
    }

    pub fn get_channel(&self) -> &Channel {
        &self.channel
    }

    pub fn close(self) -> Result<()> {
        self.connection.close()
    }
}

pub struct BinaryExchange<'a> {
    exchange: Exchange<'a>,
    output_key: String,
    producers: usize,
    consumers: usize,
    finished_producers: usize,
    eos_message: Message,
}

impl<'a> BinaryExchange<'a> {
    pub fn new(
        exchange: Exchange<'a>,
        output_key: Option<String>,
        producers: usize,
        consumers: usize,
    ) -> Self {
        let output_key = output_key.unwrap_or_default();
        let eos_message = Message::EndOfStream;
        let finished_producers = 0;
        Self {
            exchange,
            output_key,
            producers,
            consumers,
            finished_producers,
            eos_message,
        }
    }
}

impl RabbitExchange for BinaryExchange<'_> {
    fn send<T>(&mut self, message: &T) -> Result<()>
    where
        T: serde::Serialize + std::fmt::Debug,
    {
        self.send_with_key(message, &self.output_key.clone())
    }

    fn send_with_key<T>(&mut self, message: &T, key: &str) -> Result<()>
    where
        T: serde::Serialize,
    {
        let body = bincode::serialize(message).unwrap();
        self.exchange.publish(Publish::new(&body, key))
    }

    fn end_of_stream(&mut self) -> Result<bool> {
        self.send(&self.eos_message.clone())?;
        self.send(&Message::Confirmed)?;
        Ok(true)
    }
}

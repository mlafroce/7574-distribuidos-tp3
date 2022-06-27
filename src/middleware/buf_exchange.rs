use crate::messages::Message::BulkMessage;
use crate::middleware::connection::BinaryExchange;
use crate::middleware::RabbitExchange;
use amiquip::{Channel, Delivery, Result};
use serde::Serialize;

const MAX_BUF_SIZE: usize = 1_000_000;

pub struct BufExchange<'a> {
    exchange: BinaryExchange<'a>,
    channel: &'a Channel,
    message_buf: Vec<u8>,
    message_sizes: Vec<usize>,
    delivery_buf: Vec<Delivery>,
    max_buf_size: usize,
    routing_key: String,
}

impl<'a> BufExchange<'a> {
    pub fn new(
        exchange: BinaryExchange<'a>,
        channel: &'a Channel,
        routing_key: Option<String>,
    ) -> Self {
        let message_buf = Vec::new();
        let message_sizes = Vec::new();
        let delivery_buf = Vec::new();
        let max_buf_size = MAX_BUF_SIZE;
        let routing_key = routing_key.unwrap_or("".to_string());
        Self {
            exchange,
            channel,
            routing_key,
            message_buf,
            message_sizes,
            delivery_buf,
            max_buf_size,
        }
    }

    pub fn flush(&mut self) -> Result<()> {
        if self.message_buf.is_empty() {
            return Ok(());
        }
        let message = BulkMessage(self.message_buf.clone(), self.message_sizes.clone());
        self.exchange.send_with_key(&message, &self.routing_key)?;
        for delivery in self.delivery_buf.drain(..) {
            delivery.ack(&self.channel)?;
        }
        self.delivery_buf = Vec::new();
        self.message_buf.clear();
        self.message_sizes.clear();
        self.delivery_buf.clear();
        Ok(())
    }
}

impl Drop for BufExchange<'_> {
    fn drop(&mut self) {
        self.flush().unwrap();
    }
}

impl RabbitExchange for BufExchange<'_> {
    fn send<T>(&mut self, message: &T) -> Result<()>
    where
        T: Serialize,
    {
        let mut data = bincode::serialize(message).unwrap();
        if self.message_buf.len() + data.len() > self.max_buf_size && !self.message_buf.is_empty() {
            self.flush()?;
        }
        self.message_sizes.push(data.len());
        self.message_buf.append(&mut data);
        Ok(())
    }

    fn send_with_key<T>(&mut self, message: &T, key: &str) -> Result<()>
    where
        T: Serialize,
    {
        self.flush()?;
        self.exchange.send_with_key(message, key)
    }

    fn end_of_stream(&mut self) -> Result<bool> {
        self.flush()?;
        self.exchange.end_of_stream()
    }
}

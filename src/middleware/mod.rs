pub mod buf_consumer;
pub mod buf_exchange;
pub mod connection;
pub mod consumer;
pub mod message_processor;
pub mod service;
pub mod transaction_log;

use amiquip::{Consumer, Delivery, Error, Result};

pub enum ServiceError {
    InvalidMessage,
    RabbitError(Error),
}

impl From<Error> for ServiceError {
    fn from(e: Error) -> Self {
        ServiceError::RabbitError(e)
    }
}

pub trait RabbitExchange {
    fn send<T>(&mut self, message: &T) -> Result<()>
    where
        T: serde::Serialize + std::fmt::Debug;

    fn send_with_key<T>(&mut self, message: &T, key: &str) -> Result<()>
    where
        T: serde::Serialize + std::fmt::Debug;

    /// Call when an end of stream arrives. If no producers are left, notify consumers about EOS
    /// Returns true if finished, false otherwise
    fn end_of_stream(&mut self) -> Result<bool>;

    fn ack(&mut self, consumer: &Consumer, delivery: Delivery) -> Result<()> {
        consumer.ack(delivery)
    }
}

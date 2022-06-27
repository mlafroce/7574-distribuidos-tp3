pub mod buf_exchange;
pub mod buf_queue;
pub mod connection;
pub mod service;

use amiquip::{Consumer, Delivery, Error, Result};

pub enum ServiceError {
    InvalidMessage,
    RabbitError(Error)
}

impl From<Error> for ServiceError {
    fn from(e: Error) -> Self {
        ServiceError::RabbitError(e)
    }
}

pub trait RabbitExchange {
    fn send<T>(&mut self, message: &T) -> Result<()>
    where
        T: serde::Serialize;

    fn send_with_key<T>(&mut self, message: &T, key: &str) -> Result<()>
    where
        T: serde::Serialize;

    /// Call when an end of stream arrives. If no producers are left, notify consumers about EOS
    /// Returns true if finished, false otherwise
    fn end_of_stream(&mut self) -> Result<bool>;

    fn ack(&mut self, consumer: &Consumer, delivery: Delivery) -> Result<()> {
        consumer.ack(delivery)
    }
}

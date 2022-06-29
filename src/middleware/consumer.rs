use std::sync::atomic::Ordering;
use amiquip::{Consumer, ConsumerMessage, Delivery};
use crossbeam_channel::RecvTimeoutError;
use crate::middleware::service::TERM_FLAG;
use crate::RECV_TIMEOUT;

pub struct DeliveryConsumer<'a> {
    consumer: Consumer<'a>
}

impl <'a> DeliveryConsumer<'a> {
    pub fn new(consumer: Consumer<'a>) -> Self {
        Self {consumer}
    }
}

impl Iterator for DeliveryConsumer<'_> {
    type Item = Delivery;
    fn next (&mut self) -> Option<Delivery> {
        while !TERM_FLAG.load(Ordering::Relaxed) {
            match self.consumer.receiver().recv_timeout(RECV_TIMEOUT) {
                Ok(msg ) => {
                    match msg {
                        ConsumerMessage::Delivery(delivery) => return Some(delivery),
                        _ => return None
                    }
                }
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => return None
            };
        }
        None
    }
}
use crate::messages::Message;
use amiquip::{Delivery};
use crate::middleware::consumer::DeliveryConsumer;

// TODO:  Crear clase StatefulConsumer

pub struct BufConsumer<'a> {
    consumer: DeliveryConsumer<'a>,
}

impl<'a> BufConsumer<'a> {
    pub fn new(consumer: DeliveryConsumer<'a>) -> Self {
        Self {
            consumer,
        }
    }

    fn recv_messages(&mut self) -> Option<(Vec<Message>, Delivery)> {
        let delivery = self.consumer.next()?;
        let message = bincode::deserialize::<Message>(&delivery.body).unwrap();
        let mut messages = Vec::new();
        if let Message::BulkMessage(bulk, messages_sizes) = message {
            let mut offset = 0;
            for i in messages_sizes {
                let bytes = &bulk[offset..offset + i];
                let msg = bincode::deserialize::<Message>(bytes).unwrap();
                messages.push(msg);
                offset += i;
            }
        } else {
            messages.push(message);
        }
        Some((messages, delivery))
    }
}

impl Iterator for BufConsumer<'_> {
    type Item = (Vec<Message>, Delivery);

    fn next(&mut self) -> Option<Self::Item> {
        self.recv_messages()
    }
}

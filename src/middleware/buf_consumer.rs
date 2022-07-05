use crate::messages::Message;
use crate::middleware::consumer::DeliveryConsumer;
use amiquip::Delivery;
use log::{info, warn};

// TODO:  Crear clase StatefulConsumer

pub struct BufConsumer<'a> {
    consumer: DeliveryConsumer<'a>,
}

pub struct CompoundDelivery {
    pub data: Vec<Message>,
    pub msg_delivery: Delivery,
    pub confirm_delivery: Delivery,
}

impl<'a> BufConsumer<'a> {
    pub fn new(consumer: DeliveryConsumer<'a>) -> Self {
        Self { consumer }
    }

    fn recv_messages(&mut self) -> Option<CompoundDelivery> {
        let mut msg_delivery = self.consumer.next()?;
        let mut confirm_delivery = self.consumer.next()?;
        let mut confirm_message = bincode::deserialize::<Message>(&confirm_delivery.body).unwrap();
        while ! matches!(confirm_message, Message::Confirmed) {
            warn!("Second message is not a confirm, must be a repited msg");
            self.consumer.ack(msg_delivery);
            msg_delivery = confirm_delivery;
            confirm_delivery = self.consumer.next()?;
            confirm_message = bincode::deserialize::<Message>(&confirm_delivery.body).unwrap();
        }
        let message = bincode::deserialize::<Message>(&msg_delivery.body).unwrap();
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
        Some(CompoundDelivery{data: messages, msg_delivery, confirm_delivery})
    }
}

impl Iterator for BufConsumer<'_> {
    type Item = CompoundDelivery;

    fn next(&mut self) -> Option<Self::Item> {
        self.recv_messages()
    }
}

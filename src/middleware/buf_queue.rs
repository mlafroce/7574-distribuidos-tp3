use crate::messages::Message;
use crate::middleware::ServiceError;
use amiquip::{Consumer, ConsumerMessage};

pub struct BufConsumer<'a> {
    consumer: Consumer<'a>,
    buffer: Vec<Message>,
    current_idx: usize,
}

impl<'a> BufConsumer<'a> {
    pub fn new(consumer: Consumer<'a>) -> Self {
        Self {
            consumer,
            buffer: Vec::new(),
            current_idx: 0,
        }
    }

    fn recv_messages(&self) -> Result<Vec<Message>, ServiceError> {
        match self.consumer.receiver().recv() {
            Ok(ConsumerMessage::Delivery(delivery)) => {
                let message = bincode::deserialize::<Message>(&delivery.body).unwrap();
                if let Message::BulkMessage(bulk, messages_sizes) = message {
                    let mut offset = 0;
                    let mut messages = Vec::new();
                    for i in messages_sizes {
                        let bytes = &bulk[offset..offset + i];
                        let msg = bincode::deserialize::<Message>(bytes).unwrap();
                        messages.push(msg);
                        offset += i;
                    }

                    self.consumer.ack(delivery)?;
                    Ok(messages)
                } else {
                    self.consumer.ack(delivery)?;
                    Ok(vec![message])
                }
            }
            _ => Err(ServiceError::InvalidMessage),
        }
    }
}

impl Iterator for BufConsumer<'_> {
    type Item = Message;

    fn next(&mut self) -> Option<Self::Item> {
        if self.buffer.len() <= self.current_idx {
            self.buffer = self.recv_messages().ok()?;
            self.current_idx = 0;
        }
        let message = self.buffer.get(self.current_idx)?;
        self.current_idx += 1;
        Some(message.clone())
    }
}

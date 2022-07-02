/*
use amiquip::{Channel, Delivery};
use crate::messages::Message;
trait MessageProcessor {
    fn process_bulk(&mut self, bulk: Vec<Message>, delivery: Delivery, channel: &Channel) {
        for message in bulk {
            self.process_message(message);
        }
        delivery.ack(&channel)?;
    }

    fn process_bulk_message(&mut self, message: Message) -> Option<Message> {
        match message {
            Message::EndOfStream => {
                stream_finished = exchange.end_of_stream()?;
                if stream_finished {
                    self.on_stream_finished(&mut exchange)?;
                }
            }
            _ => {
                self.process_message(message, &mut exchange)?;
            }
        }
    }
}*/
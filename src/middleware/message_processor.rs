use crate::messages::Message;
use crate::middleware::RabbitExchange;
use amiquip::Result;
use serde::Serialize;


pub trait MessageProcessor {
    type State: Serialize;

    fn process_message(&mut self, message: Message) -> Option<Message>;

    fn on_stream_finished(&self) -> Option<Message> {
        None
    }

    fn send_process_output<E: RabbitExchange>(
        &self,
        exchange: &mut E,
        message: Message,
    ) -> Result<()> {
        exchange.send(&message)
    }

    fn get_state(&self) -> Option<Self::State> { None }
}

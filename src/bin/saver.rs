use std::collections::HashMap;

use amiquip::Result;
use tp2::connection::BinaryExchange;
use tp2::messages::Message;
use tp2::service::{init, RabbitService};
use tp2::{Config, DATA_TO_SAVE_QUEUE_NAME};

struct Saver {
    data: HashMap<String, String>
}

impl RabbitService for Saver {
    fn process_message(&mut self, message: Message, _: &BinaryExchange) -> Result<()> {
        match message {
            Message::DataToSave(key, value) => {
                self.data.insert(key, value);
            }
            _ => {}
        }
        Ok(())
    }
}

fn main() -> Result<()> {
    let env_config = init();
    run_service(env_config)
}

fn run_service(config: Config) -> Result<()> {

    let mut service = Saver {
        data: HashMap::new()
    };

    service.run(
        config,
        DATA_TO_SAVE_QUEUE_NAME,
        None,
    )
}
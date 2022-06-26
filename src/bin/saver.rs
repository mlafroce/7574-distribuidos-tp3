use amiquip::Result;
use tp2::connection::BinaryExchange;
use tp2::messages::Message;
use tp2::service::{init, RabbitService};
use tp2::{Config, DATA_TO_SAVE_QUEUE_NAME};

struct Saver;

impl RabbitService for Saver {
    fn process_message(&mut self, message: Message, bin_exchange: &BinaryExchange) -> Result<()> {
        match message {
            Message::DataToSave(data) => {
                println!("hola")
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
    let mut service = Saver;
    service.run(
        config,
        DATA_TO_SAVE_QUEUE_NAME,
        None,
    )
}
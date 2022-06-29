use super::connection::{BinaryExchange, RabbitConnection};
use crate::messages::Message;
use crate::middleware::buf_exchange::BufExchange;
use crate::middleware::RabbitExchange;
use crate::Config;
use amiquip::{Result};
use envconfig::Envconfig;
use lazy_static::lazy_static;
use log::{info};
use std::sync::atomic::{AtomicBool};
use std::sync::Arc;
use crate::middleware::buf_consumer::BufConsumer;
use crate::middleware::consumer::DeliveryConsumer;

// global
lazy_static! {
    pub static ref TERM_FLAG: Arc<AtomicBool> = Arc::new(AtomicBool::new(false));
}

pub fn init() -> Config {
    let env_config = Config::init_from_env().unwrap();
    println!("Setting logger level: {}", env_config.logging_level);
    std::env::set_var("RUST_LOG", env_config.logging_level.clone());
    env_logger::init();
    signal_hook::flag::register(signal_hook::consts::SIGTERM, Arc::clone(&TERM_FLAG)).unwrap();
    signal_hook::flag::register(signal_hook::consts::SIGINT, Arc::clone(&TERM_FLAG)).unwrap();
    env_config
}

pub trait RabbitService {
    fn process_message<E: RabbitExchange>(
        &mut self,
        message: Message,
        bin_exchange: &mut E,
    ) -> Result<()>;

    fn on_stream_finished<E: RabbitExchange>(&self, _: &mut E) -> Result<()> {
        Ok(())
    }

    fn run(&mut self, config: Config, consumer: &str, output_key: Option<String>) -> Result<()> {
        let consumers = str::parse::<usize>(&config.consumers).unwrap();
        let producers = str::parse::<usize>(&config.producers).unwrap();
        let connection = RabbitConnection::new(&config)?;
        {
            let exchange = connection.get_direct_exchange();
            let channel = connection.get_channel();
            let consumer = connection.get_consumer(consumer)?;
            let consumer = DeliveryConsumer::new(consumer);
            let bin_exchange = BinaryExchange::new(exchange, output_key.clone(), producers, consumers);
            let buf_consumer = BufConsumer::new(consumer);
            let mut exchange = BufExchange::new(bin_exchange, channel, output_key);

            'bulk_loop: for (bulk, delivery) in  buf_consumer {
                for message in bulk {
                    match message {
                        Message::EndOfStream => {
                            let stream_finished = exchange.end_of_stream()?;
                            if stream_finished {
                                self.on_stream_finished(&mut exchange)?;
                                delivery.ack(&channel)?;
                                break 'bulk_loop;
                            } else {
                                continue;
                            }
                        }
                        _ => {
                            self.process_message(message, &mut exchange)?;
                        }
                    }
                }
                delivery.ack(&channel)?;
            }
        }
        info!("Exit");
        connection.close()?;
        Ok(())
    }
}

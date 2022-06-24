use crate::connection::{BinaryExchange, RabbitConnection};
use crate::messages::Message;
use crate::{Config, RECV_TIMEOUT};
use amiquip::{ConsumerMessage, Result};
use envconfig::Envconfig;
use lazy_static::lazy_static;
use log::{error, info};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

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
    fn process_message(&mut self, message: Message, bin_exchange: &BinaryExchange) -> Result<()>;

    fn on_stream_finished(&self, _: &BinaryExchange) -> Result<()> {
        Ok(())
    }

    fn run(&mut self, config: Config, consumer: &str, output_key: Option<String>) -> Result<()> {
        let consumers = str::parse::<usize>(&config.consumers).unwrap();
        let producers = str::parse::<usize>(&config.producers).unwrap();
        let connection = RabbitConnection::new(&config)?;
        {
            let exchange = connection.get_direct_exchange();
            let consumer = connection.get_consumer(consumer)?;
            let mut bin_exchange = BinaryExchange::new(exchange, output_key, producers, consumers);

            while !TERM_FLAG.load(Ordering::Relaxed) {
                let consumer_message = consumer.receiver().recv_timeout(RECV_TIMEOUT);
                match consumer_message {
                    Ok(ConsumerMessage::Delivery(delivery)) => {
                        let message = bincode::deserialize::<Message>(&delivery.body);
                        match message {
                            Ok(Message::EndOfStream) => {
                                let stream_finished = bin_exchange.producer_ended()?;
                                if stream_finished {
                                    self.on_stream_finished(&bin_exchange)?;
                                    consumer.ack(delivery)?;
                                    break;
                                } else {
                                    consumer.ack(delivery)?;
                                    continue;
                                }
                            }
                            Ok(message) => {
                                self.process_message(message, &bin_exchange)?;
                            }
                            Err(_) => {
                                error!("Invalid message arrived");
                            }
                        };
                        consumer.ack(delivery)?;
                    }
                    Err(crossbeam_channel::RecvTimeoutError::Timeout) => {}
                    _ => {
                        error!("Some error on consumer");
                    }
                }
            }
        }
        info!("Exit");
        connection.close()?;
        Ok(())
    }
}

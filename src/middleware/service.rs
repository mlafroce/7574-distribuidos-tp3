use super::connection::{BinaryExchange, RabbitConnection};
use crate::messages::{BulkBuilder, Message};
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
    fn process_message(
        &mut self,
        message: Message,
    ) -> Option<Message>;

    fn on_stream_finished(&self) -> Option<Message> {
        None
    }

    fn send_process_output<E: RabbitExchange>(&self, exchange: &mut E, message: Message) -> Result<()> {
        exchange.send(&message)
    }

    fn run(&mut self, config: Config, consumer: &str, output_key: Option<String>) -> Result<()> {
        self._run(config, consumer, output_key, false)
    }

    fn run_once(&mut self, config: Config, consumer: &str, output_key: Option<String>) -> Result<()> {
        self._run(config, consumer, output_key, true)
    }

    fn _run(&mut self, config: Config, consumer: &str, output_key: Option<String>, run_once: bool) -> Result<()> {
        let consumers = str::parse::<usize>(&config.consumers).unwrap();
        let producers = str::parse::<usize>(&config.producers).unwrap();
        let connection = RabbitConnection::new(&config)?;
        {
            let exchange = connection.get_direct_exchange();
            let channel = connection.get_channel();
            let consumer = connection.get_consumer(consumer)?;
            let consumer = DeliveryConsumer::new(consumer);
            let mut exchange = BinaryExchange::new(exchange, output_key, producers, consumers);
            let buf_consumer = BufConsumer::new(consumer);

            let mut stream_finished= run_once;
            for (bulk, delivery) in  buf_consumer {
                let mut bulk_builder = BulkBuilder::default();
                for message in bulk {
                    match message {
                        Message::EndOfStream => {
                            stream_finished = exchange.end_of_stream()?;
                            if stream_finished {
                                if let Some(result) = self.on_stream_finished() {
                                    info!("Stream finished with result: {:?}", result);
                                    bulk_builder.push(&result);
                                }
                            }
                        }
                        _ => {
                            if let Some(result) = self.process_message(message) {
                                bulk_builder.push(&result);
                            }
                        }
                    }
                }
                if bulk_builder.size() > 0 {
                    let output = bulk_builder.build();
                    self.send_process_output(&mut exchange, output)?;
                }
                delivery.ack(channel)?;
                if stream_finished {
                    break;
                }
            }
        }
        info!("Exit");
        connection.close()?;
        Ok(())
    }
}

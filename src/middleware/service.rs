use std::marker::PhantomData;
use super::connection::{BinaryExchange, RabbitConnection};
use crate::messages::{BulkBuilder, Message};
use crate::middleware::buf_consumer::BufConsumer;
use crate::middleware::consumer::DeliveryConsumer;
use crate::middleware::message_processor::MessageProcessor;
use crate::middleware::transaction_log::{Checkpoint, TransactionLog};
use crate::middleware::RabbitExchange;
use crate::Config;
use amiquip::Result;
use envconfig::Envconfig;
use lazy_static::lazy_static;
use log::info;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use serde::Serialize;

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

pub struct RabbitService<'a, M: MessageProcessor> {
    config: Config,
    message_processor: &'a mut M,
    transaction_log: TransactionLog,
}

impl<'a, M: MessageProcessor> RabbitService<'a, M> {
    pub fn new(config: Config, message_processor: &'a mut M) -> Self {
        let transaction_log = TransactionLog::new(&config.transaction_log_path).unwrap();
        Self {
            config,
            message_processor,
            transaction_log
        }
    }

    pub fn run(&mut self, consumer: &str, output_key: Option<String>) -> Result<()> {
        self._run(consumer, output_key, false)
    }

    pub fn run_once(&mut self, consumer: &str, output_key: Option<String>) -> Result<()> {
        self._run(consumer, output_key, true)
    }

    fn _run(&mut self, consumer: &str, output_key: Option<String>, run_once: bool) -> Result<()> {
        let consumers = str::parse::<usize>(&self.config.consumers).unwrap();
        let producers = str::parse::<usize>(&self.config.producers).unwrap();
        let connection = RabbitConnection::new(&self.config)?;
        {
            let exchange = connection.get_direct_exchange();
            let channel = connection.get_channel();
            let consumer = connection.get_consumer(consumer)?;
            let consumer = DeliveryConsumer::new(consumer);
            let mut exchange = BinaryExchange::new(exchange, output_key, producers, consumers);
            let buf_consumer = BufConsumer::new(consumer);

            let mut stream_finished = run_once;
            let first_run = true;
            for (bulk, delivery) in buf_consumer {
                let mut checkpoint = Checkpoint::<u32>::Clean;
                let mut bulk_builder = BulkBuilder::default();

                if !first_run {
                    //checkpoint = self.transaction_log.load_state(&bulk_builder).unwrap();
                }
                if checkpoint == Checkpoint::Clean {
                    for message in bulk {
                        match message {
                            Message::EndOfStream => {
                                stream_finished = exchange.end_of_stream()?;
                                if stream_finished {
                                    if let Some(result) =
                                        self.message_processor.on_stream_finished()
                                    {
                                        info!("Stream finished with result: {:?}", result);
                                        bulk_builder.push(&result);
                                    }
                                }
                            }
                            _ => {
                                if let Some(result) =
                                    self.message_processor.process_message(message)
                                {
                                    bulk_builder.push(&result);
                                }
                            }
                        }
                    }
                    self.transaction_log.save_state(0); //writeTransactionLog(State, "processed")
                }
                if bulk_builder.size() > 0 {
                    let output = bulk_builder.build();
                    self.message_processor
                        .send_process_output(&mut exchange, output)?;
                }
                //self.transaction_log.save_sent();
                //self.transaction_log.save_confirmed();
                delivery.ack(channel)?;
                //self.transaction_log.save_clean();
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

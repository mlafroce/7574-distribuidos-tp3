use std::ops::Add;
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
    pub is_subservice: bool
}

impl<'a, M: MessageProcessor> RabbitService<'a, M> {
    pub fn new(config: Config, message_processor: &'a mut M) -> Self {
        let transaction_log = TransactionLog::new(&config.transaction_log_path).unwrap();
        Self {
            config,
            message_processor,
            transaction_log,
            is_subservice: false,
        }
    }

    pub fn new_subservice(mut config: Config, message_processor: &'a mut M) -> Self {
        config.transaction_log_path = config.transaction_log_path.add(".subservice");
        let transaction_log = TransactionLog::new(&config.transaction_log_path).unwrap();
        info!("New transaction log: {:?}", config.transaction_log_path);
        Self {
            config,
            message_processor,
            transaction_log,
            is_subservice: true,
        }
    }

    pub fn run(&mut self, consumer: &str, output_key: Option<String>) -> Result<()> {
        self._run(consumer, output_key, false)
    }

    pub fn run_once(&mut self, consumer: &str, output_key: Option<String>) -> Result<()> {
        self._run(consumer, output_key, true)
    }

    fn _run(&mut self, consumer: &str, output_key: Option<String>, run_once: bool) -> Result<()> {
        let connection = RabbitConnection::new(&self.config)?;
        {
            let exchange = connection.get_direct_exchange();
            let channel = connection.get_channel();
            let consumer = connection.get_consumer(consumer)?;
            let consumer = DeliveryConsumer::new(consumer);
            let mut exchange = BinaryExchange::new(exchange, output_key, 1, 1);
            let buf_consumer = BufConsumer::new(consumer);

            let mut stream_finished = run_once;
            info!("Loading state");
            let state = self.transaction_log.load_state::<M::State>().unwrap_or_default();
            self.message_processor.set_state(state);
            let mut checkpoint = self.transaction_log.load_checkpoint::<M::State>().unwrap_or(Checkpoint::Clean);
            if matches!(checkpoint, Checkpoint::ServiceFinished) {
                return Ok(());
            }
            info!("Consuming queue");
            for compound_delivery in buf_consumer {
                let mut bulk_builder = BulkBuilder::default();
                if matches!(checkpoint, Checkpoint::Clean) {
                    for message in compound_delivery.data {
                        match message {
                            Message::EndOfStream => {
                                stream_finished = true;
                                info!("Stream finished, pushing EOS");
                                bulk_builder.push(&Message::EndOfStream);

                                if let Some(result) =
                                    self.message_processor.on_stream_finished()
                                {
                                    info!("pushing Result {:?}", result);
                                    bulk_builder.push(&result);
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
                    if let Some(state) = self.message_processor.get_state() {
                        self.transaction_log.save_state(state).unwrap(); //writeTransactionLog(State, "processed")
                    }
                }
                if bulk_builder.size() > 0 && !matches!(checkpoint, Checkpoint::Confirmed) {
                    let output = bulk_builder.build();
                    if !matches!(checkpoint, Checkpoint::Sent) {
                        self.message_processor
                            .send_process_output(&mut exchange, output)?;
                    }
                    self.transaction_log.save_sent().unwrap();
                    exchange.send(&Message::Confirmed)?;
                    self.transaction_log.save_confirmed().unwrap();
                }

                if stream_finished {
                    self.transaction_log.save_end_of_stream().unwrap();
                }

                // Also ACKs msg_delivery
                compound_delivery.confirm_delivery.ack_multiple(channel)?;

                if stream_finished {
                    if !self.is_subservice {
                        self.transaction_log.delete_log().unwrap();
                    } else {
                        self.transaction_log.save_service_finished().unwrap();
                    }
                    break
                }
                self.transaction_log.save_clean().unwrap();
                checkpoint = Checkpoint::Clean;
            }
        }
        info!("Exit");
        match connection.close() {
            Ok(_) => {},
            Err(e) => {info!("Failed to properly close RabbitMQ connection: {:?}", e)}
        }
        Ok(())
    }
}

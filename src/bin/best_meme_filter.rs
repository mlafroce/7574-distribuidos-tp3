use std::ops::Add;
use amiquip::Result;
use log::{debug, error, info, warn};
use tp2::messages::Message;
use tp2::middleware::message_processor::MessageProcessor;
use tp2::middleware::service::{init, RabbitService};
use tp2::middleware::RabbitExchange;
use tp2::{
    Config, POST_EXTRACTED_URL_QUEUE_NAME, POST_SENTIMENT_MEAN_QUEUE_NAME, RESULTS_QUEUE_NAME,
};
use tp2::health_checker::health_answerer::HealthAnswerer;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use tp2::health_checker::health_answerer_handler::HealthAnswerHandler;
use tp2::health_checker::health_base::HealthBase;
use tp2::sigterm_handler::sigterm_handler::handle_sigterm;

fn main() -> Result<()> {
    let env_config = init();
    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_handler_join = shutdown.clone();
    let sigterm_handler_join = thread::spawn(move || handle_sigterm(shutdown_handler_join));
    let mut health_answerer_handler = HealthAnswerHandler::new(shutdown.clone());
    let health_answerer = HealthAnswerer::new("0.0.0.0:6789", shutdown.clone());
    let health_answerer_thread = thread::spawn(move || {health_answerer.run(&mut health_answerer_handler)});
    while !shutdown.load(Ordering::Relaxed) {
        run_service(env_config.clone())?;
    }
    shutdown.store(true, Ordering::Relaxed);
    health_answerer_thread.join().expect("Failed to join health_answerer_thread");
    sigterm_handler_join.join().expect("Failed to join handle_sigterm");
    Ok(())
}

fn run_service(config: Config) -> Result<()> {
    info!("Getting best meme id");
    let mut id_processor = BestMemeIdConsumer::default();
    let mut subservice = RabbitService::new_subservice(config.clone(), &mut id_processor);
    subservice.run_once(POST_SENTIMENT_MEAN_QUEUE_NAME, None)?;
    info!("Got sentiment {:?}", id_processor.best_meme_id_sentiment);
    let best_meme_id = id_processor.best_meme_id_sentiment.0;
    // FIX: Seems that closing and opening a connection so fast crashes the app, putting a sleep
    std::thread::sleep(std::time::Duration::from_secs(1));
    let consumer_transaction_log_path = config.transaction_log_path.clone().add(".subservice");
    info!("Getting best meme");
    let mut processor = BestMemeFilter::new(best_meme_id);
    let mut service = RabbitService::new(config, &mut processor);
    service.run(POST_EXTRACTED_URL_QUEUE_NAME, None)?;
    std::fs::remove_file(&consumer_transaction_log_path);
    Ok(())
}

struct BestMemeFilter {
    best_meme_id: String,
    best_meme_url: String,
}

impl BestMemeFilter {
    pub fn new(best_meme_id: String) -> Self {
        Self {
            best_meme_id,
            best_meme_url: "".to_string(),
        }
    }
}

impl MessageProcessor for BestMemeFilter {
    type State = ();
    fn process_message(&mut self, message: Message) -> Option<Message> {
        match message {
            Message::PostUrl(id, url) => {
                if id == self.best_meme_id {
                    self.best_meme_url = url;
                }
            }
            _ => {
                warn!("Invalid message arrived");
            }
        }
        None
    }

    fn on_stream_finished(&self) -> Option<Message> {
        debug!("Sending best meme url: {}", self.best_meme_url);
        Some(Message::PostUrl(
            self.best_meme_id.clone(),
            self.best_meme_url.clone(),
        ))
    }

    fn send_process_output<E: RabbitExchange>(
        &self,
        exchange: &mut E,
        message: Message,
    ) -> Result<()> {
        exchange.send_with_key(&message, RESULTS_QUEUE_NAME)?;
        exchange.send_with_key(&Message::Confirmed, RESULTS_QUEUE_NAME)

    }
}
// Should I use a heap of best memes ids in case the best one is missing?

struct BestMemeIdConsumer {
    best_meme_id_sentiment: (String, f32),
}

impl Default for BestMemeIdConsumer {
    fn default() -> Self {
        let best_meme_id_sentiment = ("".to_string(), f32::MIN);
        Self {
            best_meme_id_sentiment,
        }
    }
}

impl MessageProcessor for BestMemeIdConsumer {
    type State = ();

    fn process_message(&mut self, message: Message) -> Option<Message> {
        match message {
            Message::PostIdSentiment(id, sentiment) => {
                if sentiment > self.best_meme_id_sentiment.1 {
                    self.best_meme_id_sentiment = (id, sentiment);
                }
            }
            _ => {
                error!("Invalid message arrived");
            }
        }
        None
    }
}

use amiquip::Result;
use log::{info, warn};
use std::collections::HashMap;
use tp2::messages::Message;
use tp2::middleware::message_processor::MessageProcessor;
use tp2::middleware::service::{init, RabbitService};
use tp2::middleware::RabbitExchange;
use tp2::{Config, FILTERED_POST_ID_SENTIMENT_QUEUE_NAME, POST_SENTIMENT_MEAN_QUEUE_NAME};
use tp2::health_checker::health_answerer::HealthAnswerer;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use tp2::health_checker::health_base::HealthBase;
use tp2::health_checker::health_answerer_handler::HealthAnswerHandler;

fn main() -> Result<()> {
    let config = init();
    let shutdown = Arc::new(AtomicBool::new(false));
    let health_answerer = HealthAnswerer::new("0.0.0.0:6789", shutdown.clone());
    let mut health_answerer_handler = HealthAnswerHandler::new(shutdown.clone());
    let health_answerer_thread = thread::spawn(move || {health_answerer.run(&mut health_answerer_handler)});
    run_service(config)?;
    shutdown.store(true, Ordering::Relaxed);
    health_answerer_thread.join().expect("Failed to join health_answerer_thread");
    Ok(())
}

fn run_service(config: Config) -> Result<()> {
    let mut processor = PostSentimentCalculator::default();
    let mut service = RabbitService::new(config, &mut processor);
    service.run(FILTERED_POST_ID_SENTIMENT_QUEUE_NAME, None)
}

#[derive(Default)]
struct PostSentimentCalculator {
    post_sentiments_map: HashMap<String, (f32, i32)>,
}

impl MessageProcessor for PostSentimentCalculator {
    type State = HashMap<String, (f32, i32)>;
    fn process_message(&mut self, message: Message) -> Option<Message> {
        match message {
            Message::PostIdSentiment(post_id, sentiment) => {
                let value = self.post_sentiments_map.entry(post_id).or_insert((0.0, 0));
                value.0 += sentiment;
                value.1 += 1;
            }
            _ => {
                warn!("Invalid message arrived");
            }
        }
        None
    }

    fn on_stream_finished(&self) -> Option<Message> {
        let post_sentiment = get_highest_post_sentiment(&self.post_sentiments_map);
        info!("Sending highest post sentiment {:?}", post_sentiment);
        Some(Message::PostIdSentiment(post_sentiment.0, post_sentiment.1))
    }

    fn send_process_output<E: RabbitExchange>(
        &self,
        exchange: &mut E,
        message: Message,
    ) -> Result<()> {
        exchange.send_with_key(&message, POST_SENTIMENT_MEAN_QUEUE_NAME)?;
        exchange.send_with_key(&Message::Confirmed, POST_SENTIMENT_MEAN_QUEUE_NAME)
    }
}

fn get_highest_post_sentiment(sentiment_map: &HashMap<String, (f32, i32)>) -> (String, f32) {
    let mut highest_id = "".to_string();
    let mut highest = -1.0;
    for (id, sentiment) in sentiment_map.iter() {
        let sentiment_mean = sentiment.0 / sentiment.1 as f32;
        if sentiment_mean > highest {
            highest = sentiment_mean;
            highest_id = id.clone();
        }
    }
    (highest_id, highest)
}

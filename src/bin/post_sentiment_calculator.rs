use amiquip::Result;
use log::{info, warn};
use std::collections::HashMap;
use tp2::middleware::service::{init, RabbitService};
use tp2::messages::Message;
use tp2::{Config, FILTERED_POST_ID_SENTIMENT_QUEUE_NAME, POST_SENTIMENT_MEAN_QUEUE_NAME, DATA_TO_SAVE_QUEUE_NAME};
use tp2::middleware::RabbitExchange;

fn main() -> Result<()> {
    let config = init();
    run_service(config)
}

#[derive(Default)]
struct PostSentimentCalculator {
    post_sentiments_map: HashMap<String, (f32, i32)>,
}

impl RabbitService for PostSentimentCalculator {
    fn process_message(&mut self, message: Message) -> Option<Message> {
        match message {
            Message::PostIdSentiment(post_id, sentiment) => {
                let value = self.post_sentiments_map.entry(post_id.to_string()).or_insert((0.0, 0));
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
        info!("Sending highest post sentiment {:?}" , post_sentiment);
        Some(Message::PostIdSentiment(post_sentiment.0, post_sentiment.1))
    }

    fn send_process_output<E: RabbitExchange>(&self, exchange: &mut E, message: Message) -> Result<()> {
        exchange.send_with_key(&message, POST_SENTIMENT_MEAN_QUEUE_NAME)
    }
}

fn run_service(config: Config) -> Result<()> {
    let mut service = PostSentimentCalculator::default();
    service.run(config, FILTERED_POST_ID_SENTIMENT_QUEUE_NAME, None)
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

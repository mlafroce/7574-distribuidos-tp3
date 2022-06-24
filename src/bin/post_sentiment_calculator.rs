use amiquip::Result;
use log::warn;
use std::collections::HashMap;
use tp2::connection::BinaryExchange;
use tp2::messages::Message;
use tp2::service::{init, RabbitService};
use tp2::{Config, FILTERED_POST_ID_SENTIMENT_QUEUE_NAME, POST_SENTIMENT_MEAN_QUEUE_NAME};

fn main() -> Result<()> {
    let config = init();
    run_service(config)
}

#[derive(Default)]
struct PostSentimentCalculator {
    post_sentiments_map: HashMap<String, (f32, i32)>,
}

impl RabbitService for PostSentimentCalculator {
    fn process_message(&mut self, message: Message, _: &BinaryExchange) -> Result<()> {
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
        Ok(())
    }

    fn on_stream_finished(&self, bin_exchange: &BinaryExchange) -> Result<()> {
        let post_sentiment = get_highest_post_sentiment(&self.post_sentiments_map);
        let msg = Message::PostIdSentiment(post_sentiment.0, post_sentiment.1);
        bin_exchange.send_with_key(&msg, POST_SENTIMENT_MEAN_QUEUE_NAME)
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

use std::sync::atomic::Ordering;
use amiquip::{ConsumerMessage, Result};
use envconfig::Envconfig;
use log::{error, info, warn};
use tp2::connection::{BinaryExchange, RabbitConnection};
use tp2::messages::Message;
use tp2::service::{RabbitService, TERM_FLAG};
use tp2::{Config, POST_COLLEGE_QUEUE_NAME, POST_SCORE_AVERAGE_QUEUE_NAME, POST_URL_AVERAGE_QUEUE_NAME, RECV_TIMEOUT};

fn main() -> Result<()> {
    let env_config = Config::init_from_env().unwrap();
    println!("Setting logger level: {}", env_config.logging_level);
    std::env::set_var("RUST_LOG", env_config.logging_level.clone());
    env_logger::init();
    run_service(env_config)
}

fn run_service(config: Config) -> Result<()> {
    info!("Getting score average");
    if let Some(score_average) = get_score_average(&config)? {
        info!("Filtering above average");
        let mut service = PostAverageFilter { score_average };
        service.run(
            config,
            POST_COLLEGE_QUEUE_NAME,
            Some(POST_URL_AVERAGE_QUEUE_NAME.to_string()),
        )
    } else {
        // Graceful quit while getting score average
        Ok(())
    }
}

struct PostAverageFilter {
    score_average: f32,
}

impl RabbitService for PostAverageFilter {
    fn process_message(&mut self, message: Message, exchange: &BinaryExchange) -> Result<()> {
        match message {
            Message::FullPost(post) => {
                if post.score as f32 > self.score_average && post.url.starts_with("https") {
                    let msg = Message::PostUrl(post.id, post.url);
                    exchange.send(&msg)?;
                }
            }
            _ => {
                warn!("Invalid message arrived");
            }
        }
        Ok(())
    }
}

// Should I use a heap of best memes ids in case the best one is missing?
fn get_score_average(config: &Config) -> Result<Option<f32>> {
    let connection = RabbitConnection::new(config)?;
    let mut result = None;
    {
        let consumer = connection.get_consumer(POST_SCORE_AVERAGE_QUEUE_NAME)?;
        while !TERM_FLAG.load(Ordering::Relaxed) {
            let consumer_message = consumer.receiver().recv_timeout(RECV_TIMEOUT);
            // Return if valid score mean, keep iterating otherwise
            if let Ok(ConsumerMessage::Delivery(delivery)) = consumer_message {
                match bincode::deserialize::<Message>(&delivery.body) {
                    Ok(Message::PostScoreMean(mean)) => {
                        consumer.ack(delivery)?;
                        result = Some(mean);
                        break
                    }
                    _ => {
                        error!("Invalid message arrived");
                    }
                }
                consumer.ack(delivery)?;
            }
        }
    }
    connection.close()?;
    Ok(result)
}

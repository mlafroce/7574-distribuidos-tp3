use amiquip::{ConsumerMessage, Result};
use envconfig::Envconfig;
use log::{error, info, warn};
use std::sync::atomic::Ordering;
use tp2::messages::Message;
use tp2::middleware::connection::{BinaryExchange, RabbitConnection};
use tp2::middleware::service::{RabbitService, TERM_FLAG};
use tp2::middleware::RabbitExchange;
use tp2::{Config, POST_COLLEGE_QUEUE_NAME, POST_SCORE_AVERAGE_QUEUE_NAME, POST_URL_AVERAGE_QUEUE_NAME, RECV_TIMEOUT, DATA_TO_SAVE_QUEUE_NAME};

fn main() -> Result<()> {
    let env_config = Config::init_from_env().unwrap();
    println!("Setting logger level: {}", env_config.logging_level);
    std::env::set_var("RUST_LOG", env_config.logging_level.clone());
    env_logger::init();
    run_service(env_config)
}

fn run_service(config: Config) -> Result<()> {
    info!("Getting score average");
    let mut consumer = PostAverageConsumer::default();
    consumer.run_once(config.clone(), POST_SCORE_AVERAGE_QUEUE_NAME, None)?;
    if let Some(score_average) = consumer.score_average {
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
    fn process_message(
        &mut self,
        message: Message,
    ) -> Option<Message> {
        match message {
            Message::FullPost(post) => {
                if post.score as f32 > self.score_average && post.url.starts_with("https") {
                    return Some(Message::PostUrl(post.id, post.url));
                }
            }
            _ => {
                warn!("Invalid message arrived");
            }
        }
        None
    }
}

#[derive(Default)]
struct PostAverageConsumer {
    score_average: Option<f32>
}

impl RabbitService for PostAverageConsumer {
    fn process_message(
        &mut self,
        message: Message,
    ) -> Option<Message> {
        match message {
            Message::PostScoreMean(mean) => {
                self.score_average = Some(mean);
            }
            _ => {
                warn!("Invalid message arrived");
            }
        }
        None
    }
}

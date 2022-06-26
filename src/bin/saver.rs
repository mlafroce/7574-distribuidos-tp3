use std::collections::HashMap;

use amiquip::Result;
use tp2::connection::BinaryExchange;
use tp2::messages::Message;
use tp2::service::{init, RabbitService};
use tp2::{Config, DATA_TO_SAVE_QUEUE_NAME};

#[derive(Default)]
struct Score {
    sum: u32,
    count: u32
}

struct Saver {
    score: Score,
    posts_sentiment: HashMap<String, (f32, i32)>
}

impl RabbitService for Saver {
    fn process_message(&mut self, message: Message, _: &BinaryExchange) -> Result<()> {
        match message {
            Message::DataCurrentScore(value) => {
                self.score = Score {
                    sum: value.sum,
                    count: value.count
                };
            }
            Message::DataPostSentiment(value) => {
                self.posts_sentiment.insert(value.post_id, (value.sentiment, value.count));
            }
            _ => {}
        }
        Ok(())
    }
}

fn main() -> Result<()> {
    let env_config = init();
    run_service(env_config)
}

fn run_service(config: Config) -> Result<()> {

    let mut service = Saver {
        score: Score::default(),
        posts_sentiment: HashMap::new()
    };

    service.run(
        config,
        DATA_TO_SAVE_QUEUE_NAME,
        None,
    )
}
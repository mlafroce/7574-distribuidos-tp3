use amiquip::Result;
use log::warn;
use tp2::connection::BinaryExchange;
use tp2::messages::Message;
use tp2::service::{init, RabbitService};
use tp2::{Config, POST_SCORES_QUEUE_NAME, POST_SCORE_MEAN_QUEUE_NAME};

fn main() -> Result<()> {
    let env_config = init();
    run_service(env_config)
}

struct ScoreExtractor;

impl RabbitService for ScoreExtractor {
    fn process_message(&mut self, message: Message, bin_exchange: &BinaryExchange) -> Result<()> {
        match message {
            Message::FullPost(post) => {
                let score = Message::PostScore(post.score);
                bin_exchange.send(&score)?;
            }
            _ => {
                warn!("Invalid message arrived");
            }
        }
        Ok(())
    }
}

fn run_service(config: Config) -> Result<()> {
    let mut service = ScoreExtractor;
    service.run(
        config,
        POST_SCORES_QUEUE_NAME,
        Some(POST_SCORE_MEAN_QUEUE_NAME.to_owned()),
    )
}

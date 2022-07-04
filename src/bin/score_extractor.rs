use amiquip::Result;
use log::warn;
use tp2::messages::Message;
use tp2::middleware::message_processor::MessageProcessor;
use tp2::middleware::service::{init, RabbitService};
use tp2::{Config, POST_SCORES_QUEUE_NAME, POST_SCORE_MEAN_QUEUE_NAME};

fn main() -> Result<()> {
    let env_config = init();
    run_service(env_config)
}

struct ScoreExtractor;

impl MessageProcessor for ScoreExtractor {
    type State = ();
    fn process_message(&mut self, message: Message) -> Option<Message> {
        match message {
            Message::FullPost(post) => Some(Message::PostScore(post.score)),
            _ => {
                warn!("Invalid message arrived: {:?}", message);
                None
            }
        }
    }
}

fn run_service(config: Config) -> Result<()> {
    let mut processor = ScoreExtractor;
    let mut service = RabbitService::new(config, &mut processor);
    service.run(
        POST_SCORES_QUEUE_NAME,
        Some(POST_SCORE_MEAN_QUEUE_NAME.to_owned()),
    )
}

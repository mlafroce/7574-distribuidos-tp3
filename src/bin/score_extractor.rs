use amiquip::Result;
use log::warn;
use tp2::messages::Message;
use tp2::middleware::message_processor::MessageProcessor;
use tp2::middleware::service::{init, RabbitService};
use tp2::{Config, POST_SCORES_QUEUE_NAME, POST_SCORE_MEAN_QUEUE_NAME};
use tp2::health_checker::health_answerer::HealthAnswerer;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use tp2::health_checker::health_base::HealthBase;
use tp2::health_checker::health_answerer_handler::HealthAnswerHandler;

fn main() -> Result<()> {
    let env_config = init();
    let shutdown = Arc::new(AtomicBool::new(false));
    let health_answerer = HealthAnswerer::new("0.0.0.0:6789", shutdown.clone());
    let mut health_answerer_handler = HealthAnswerHandler::new(shutdown.clone());
    let health_answerer_thread = thread::spawn(move || {health_answerer.run(&mut health_answerer_handler)});
    run_service(env_config)?;
    shutdown.store(true, Ordering::Relaxed);
    health_answerer_thread.join().expect("Failed to join health_answerer_thread");
    Ok(())
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

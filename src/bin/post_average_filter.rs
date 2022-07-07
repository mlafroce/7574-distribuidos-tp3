use std::ops::Add;
use amiquip::Result;
use log::{info, warn};
use tp2::messages::Message;
use tp2::middleware::message_processor::MessageProcessor;
use tp2::middleware::service::{init, RabbitService};
use tp2::{
    Config, POST_COLLEGE_QUEUE_NAME, POST_SCORE_AVERAGE_QUEUE_NAME, POST_URL_AVERAGE_QUEUE_NAME,
};
use tp2::health_checker::health_answerer::HealthAnswerer;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use tp2::health_checker::health_base::HealthBase;
use tp2::health_checker::health_answerer_handler::HealthAnswerHandler;
use tp2::sigterm_handler::sigterm_handler::handle_sigterm;

fn main() -> Result<()> {
    let env_config = init();
    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_handler_join = shutdown.clone();
    let sigterm_handler_join = thread::spawn(move || handle_sigterm(shutdown_handler_join));
    let health_answerer = HealthAnswerer::new("0.0.0.0:6789", shutdown.clone());
    let mut health_answerer_handler = HealthAnswerHandler::new(shutdown.clone());
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
    info!("Getting score average");
    let mut processor = PostAverageConsumer::default();
    let mut consumer = RabbitService::new_subservice(config.clone(), &mut processor);
    consumer.run_once(POST_SCORE_AVERAGE_QUEUE_NAME, None)?;
    let consumer_transaction_log_path = config.transaction_log_path.clone().add(".subservice");
    if let Some(score_average) = processor.score_average {
        info!("Filtering above average");
        let mut processor = PostAverageFilter { score_average };
        // FIX: Seems that closing and opening a connection so fast crashes the app, putting a sleep
        std::thread::sleep(std::time::Duration::from_secs(1));
        let mut service = RabbitService::new(config, &mut processor);
        service.run(
            POST_COLLEGE_QUEUE_NAME,
            Some(POST_URL_AVERAGE_QUEUE_NAME.to_string()),
        )?;
        std::fs::remove_file(&consumer_transaction_log_path);
    } else {
        warn!("Couldn't pop score average");
    }
    Ok(())
}

struct PostAverageFilter {
    score_average: f32,
}

impl MessageProcessor for PostAverageFilter {
    type State = ();
    fn process_message(&mut self, message: Message) -> Option<Message> {
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
    score_average: Option<f32>,
}

impl MessageProcessor for PostAverageConsumer {
    type State = f32;
    fn process_message(&mut self, message: Message) -> Option<Message> {
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

    fn get_state(&self) -> Option<Self::State> {
        self.score_average
    }

    fn set_state(&mut self, state: Self::State) {
        self.score_average = Some(state);
    }
}

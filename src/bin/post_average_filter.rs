use amiquip::Result;
use log::{info, warn};
use tp2::messages::Message;
use tp2::middleware::message_processor::MessageProcessor;
use tp2::middleware::service::{init, RabbitService};
use tp2::{
    Config, POST_COLLEGE_QUEUE_NAME, POST_SCORE_AVERAGE_QUEUE_NAME, POST_URL_AVERAGE_QUEUE_NAME,
};

fn main() -> Result<()> {
    let env_config = init();
    run_service(env_config)
}

fn run_service(config: Config) -> Result<()> {
    info!("Getting score average");
    let mut processor = PostAverageConsumer::default();
    let mut consumer = RabbitService::new_subservice(config.clone(), &mut processor);
    consumer.run_once(POST_SCORE_AVERAGE_QUEUE_NAME, None)?;
    if let Some(score_average) = processor.score_average {
        info!("Filtering above average");
        let mut processor = PostAverageFilter { score_average };
        // FIX: Seems that closing and opening a connection so fast crashes the app, putting a sleep
        std::thread::sleep(std::time::Duration::from_secs(1));
        let mut service = RabbitService::new(config, &mut processor);
        service.run(
            POST_COLLEGE_QUEUE_NAME,
            Some(POST_URL_AVERAGE_QUEUE_NAME.to_string()),
        )
    } else {
        warn!("Couldn't pop score average");
        Ok(())
    }
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

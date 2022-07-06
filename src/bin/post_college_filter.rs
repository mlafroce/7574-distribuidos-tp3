use amiquip::Result;
use log::{info, warn};
use std::collections::HashSet;
use tp2::messages::Message;
use tp2::middleware::message_processor::MessageProcessor;
use tp2::middleware::service::{init, RabbitService};
use tp2::{Config, POST_ID_COLLEGE_QUEUE_NAME, POST_URL_AVERAGE_QUEUE_NAME, RESULTS_QUEUE_NAME};

fn main() -> Result<()> {
    let env_config = init();
    run_service(env_config)
}

fn run_service(config: Config) -> Result<()> {
    info!("Getting college post ids");
    let ids = get_college_posts_ids(&config)?;
    info!("Filtering college posts");
    let mut processor = PostCollegeFilter { ids };
    // FIX: Seems that closing and opening a connection so fast crashes the app, putting a sleep
    std::thread::sleep(std::time::Duration::from_secs(1));
    let mut service = RabbitService::new(config, &mut processor);
    service.run(
        POST_URL_AVERAGE_QUEUE_NAME,
        Some(RESULTS_QUEUE_NAME.to_string()),
    )
}

struct PostCollegeFilter {
    ids: HashSet<String>,
}

impl MessageProcessor for PostCollegeFilter {
    type State = ();
    fn process_message(&mut self, message: Message) -> Option<Message> {
        match message {
            Message::PostUrl(id, url) => {
                if self.ids.contains(&id) {
                    return Some(Message::CollegePostUrl(url));
                }
            }
            _ => {
                warn!("Invalid message arrived");
            }
        }
        None
    }

    fn on_stream_finished(&self) -> Option<Message> {
        Some(Message::CollegePostEnded)
    }
}

#[derive(Default)]
struct CollegePostIdConsumer {
    ids: HashSet<String>,
}

impl MessageProcessor for CollegePostIdConsumer {
    type State = HashSet<String>;
    fn process_message(&mut self, message: Message) -> Option<Message> {
        match message {
            Message::PostId(id) => {
                self.ids.insert(id);
            }
            _ => {
                warn!("Invalid message arrived");
            }
        }
        None
    }

    fn get_state(&self) -> Option<Self::State> {
        Some(self.ids.clone())
    }

    fn set_state(&mut self, state: Self::State) {
        self.ids = state;
    }
}

fn get_college_posts_ids(config: &Config) -> Result<HashSet<String>> {
    let config = config.clone();
    let mut processor = CollegePostIdConsumer::default();
    let mut service = RabbitService::new_subservice(config, &mut processor);
    service.run(POST_ID_COLLEGE_QUEUE_NAME, None)?;
    Ok(processor.ids)
}

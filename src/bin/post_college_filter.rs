use amiquip::Result;
use log::{info, warn};
use std::collections::HashSet;
use tp2::messages::Message;
use tp2::middleware::service::{init, RabbitService};
use tp2::middleware::RabbitExchange;
use tp2::{Config, POST_ID_COLLEGE_QUEUE_NAME, POST_URL_AVERAGE_QUEUE_NAME, RESULTS_QUEUE_NAME, DATA_TO_SAVE_QUEUE_NAME};

fn main() -> Result<()> {
    let env_config = init();
    run_service(env_config)
}

fn run_service(config: Config) -> Result<()> {
    info!("Getting college post ids");
    let ids = get_college_posts_ids(&config)?;
    info!("Filtering college posts");
    let mut service = PostCollegeFilter { ids };
    service.run(
        config,
        POST_URL_AVERAGE_QUEUE_NAME,
        Some(RESULTS_QUEUE_NAME.to_string()),
    )
}

struct PostCollegeFilter {
    ids: HashSet<String>,
}

impl RabbitService for PostCollegeFilter {
    fn process_message(
        &mut self,
        message: Message,
    ) -> Option<Message> {
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
}

#[derive(Default)]
struct CollegePostIdConsumer {
    ids: HashSet<String>,
}

impl RabbitService for CollegePostIdConsumer {
    fn process_message(&mut self, message: Message) -> Option<Message> {
        match message {
            Message::PostId(id) => {
                self.ids.insert(id.clone());
            }
            _ => {
                warn!("Invalid message arrived");
            }
        }
        None
    }
}

fn get_college_posts_ids(config: &Config) -> Result<HashSet<String>> {
    let config = config.clone();
    let mut service = CollegePostIdConsumer::default();
    service.run(config, POST_ID_COLLEGE_QUEUE_NAME, None)?;
    Ok(service.ids)
}

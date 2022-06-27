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
    fn process_message<E: RabbitExchange>(
        &mut self,
        message: Message,
        exchange: &mut E,
    ) -> Result<()> {
        match message {
            Message::PostUrl(id, url) => {
                if self.ids.contains(&id) {
                    let msg = Message::CollegePostUrl(url);
                    exchange.send(&msg)?;
                }
            }
            _ => {
                warn!("Invalid message arrived");
            }
        }
        Ok(())
    }

    fn on_stream_finished<E: RabbitExchange>(&self, exchange: &mut E) -> Result<()> {
        /* Persist Reset */
        let msg = Message::DataReset("post_college_filter".to_string());
        exchange.send_with_key(&msg, DATA_TO_SAVE_QUEUE_NAME)?;
        /* */
        Ok(())
    }
}

#[derive(Default)]
struct CollegePostIdConsumer {
    ids: HashSet<String>,
}

impl RabbitService for CollegePostIdConsumer {
    fn process_message<E: RabbitExchange>(&mut self, message: Message, exchange: &mut E) -> Result<()> {
        match message {
            Message::PostId(id) => {
                self.ids.insert(id.clone());
                /* Persist State */
                let msg = Message::DataPostIdCollege(id);
                exchange.send_with_key(&msg, DATA_TO_SAVE_QUEUE_NAME)?;
                /* */
            }
            _ => {
                warn!("Invalid message arrived");
            }
        }
        Ok(())
    }
}

fn get_college_posts_ids(config: &Config) -> Result<HashSet<String>> {
    let config = config.clone();
    let mut service = CollegePostIdConsumer::default();
    service.run(config, POST_ID_COLLEGE_QUEUE_NAME, None)?;
    Ok(service.ids)
}

use amiquip::Result;
use envconfig::Envconfig;
use log::{info, warn};
use std::collections::HashSet;
use tp2::messages::Message;
use tp2::middleware::message_processor::MessageProcessor;
use tp2::middleware::service::RabbitService;
use tp2::{
    Config, FILTERED_POST_ID_SENTIMENT_QUEUE_NAME, POST_ID_SENTIMENT_QUEUE_NAME,
    POST_ID_WITH_URL_QUEUE_NAME,
};

fn main() -> Result<()> {
    let env_config = Config::init_from_env().unwrap();
    println!("Setting logger level: {}", env_config.logging_level);
    std::env::set_var("RUST_LOG", env_config.logging_level.clone());
    env_logger::init();
    run_service(env_config)
}

fn run_service(config: Config) -> Result<()> {
    info!("Getting post ids with url");
    let ids = get_posts_ids_with_url(&config)?;
    info!("Filtering sentiments with url");
    let mut processor = PostSentimentFilter { ids };
    let mut service = RabbitService::new(config, &mut processor);
    service.run(
        POST_ID_SENTIMENT_QUEUE_NAME,
        Some(FILTERED_POST_ID_SENTIMENT_QUEUE_NAME.to_string()),
    )
}

struct PostSentimentFilter {
    ids: HashSet<String>,
}

impl MessageProcessor for PostSentimentFilter {
    type State = HashSet<String>;
    fn process_message(&mut self, message: Message) -> Option<Message> {
        match message {
            Message::PostIdSentiment(post_id, sentiment) => {
                if self.ids.contains(&post_id) {
                    return Some(Message::PostIdSentiment(post_id, sentiment));
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
struct PostIdWithUrlConsumer {
    ids: HashSet<String>,
}

impl MessageProcessor for PostIdWithUrlConsumer {
    type State = HashSet<String>;
    fn process_message(&mut self, message: Message) -> Option<Message> {
        match message {
            Message::PostUrl(id, _) => {
                self.ids.insert(id);
            }
            _ => {
                warn!("Invalid message arrived");
            }
        }
        None
    }
}

fn get_posts_ids_with_url(config: &Config) -> Result<HashSet<String>> {
    let config = config.clone();
    let mut processor = PostIdWithUrlConsumer::default();
    let mut service = RabbitService::new_subservice(config, &mut processor);
    service.run(POST_ID_WITH_URL_QUEUE_NAME, None)?;
    Ok(processor.ids)
}

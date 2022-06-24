use amiquip::Result;
use envconfig::Envconfig;
use log::{info, warn};
use std::collections::HashSet;
use tp2::messages::Message;
use tp2::{
    Config, FILTERED_POST_ID_SENTIMENT_QUEUE_NAME, POST_ID_SENTIMENT_QUEUE_NAME,
    POST_ID_WITH_URL_QUEUE_NAME,
};
use tp2::connection::BinaryExchange;
use tp2::service::RabbitService;

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
    let mut service = PostSentimentFilter { ids };
    service.run(
        config,
        POST_ID_SENTIMENT_QUEUE_NAME,
        Some(FILTERED_POST_ID_SENTIMENT_QUEUE_NAME.to_string()),
    )
}


struct PostSentimentFilter {
    ids: HashSet<String>
}

impl RabbitService for PostSentimentFilter {
    fn process_message(&mut self, message: Message, bin_exchange: &BinaryExchange) -> Result<()> {
        match message {
            Message::PostIdSentiment(post_id, sentiment) => {
                if self.ids.contains(&post_id) {
                    let msg = Message::PostIdSentiment(post_id, sentiment);
                    bin_exchange.send(&msg)?;
                }
            }
            _ => {
                warn!("Invalid message arrived");
            }
        }
        Ok(())
    }
}

#[derive(Default)]
struct PostIdWithUrlConsumer {
    ids: HashSet<String>,
}

impl RabbitService for PostIdWithUrlConsumer {
    fn process_message(&mut self, message: Message, _: &BinaryExchange) -> Result<()> {
        match message {
            Message::PostId(id) => {
                self.ids.insert(id);
            }
            _ => {
                warn!("Invalid message arrived");
            }
        }
        Ok(())
    }
}

fn get_posts_ids_with_url(config: &Config) -> Result<HashSet<String>> {
    let config = config.clone();
    let mut service = PostIdWithUrlConsumer::default();
    service.run(
        config,
        POST_ID_WITH_URL_QUEUE_NAME,
        None,
    )?;
    Ok(service.ids)
}

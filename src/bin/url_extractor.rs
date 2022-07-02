use amiquip::Result;
use log::warn;
use tp2::messages::Message;
use tp2::middleware::service::{init, RabbitService};
use tp2::{
    Config, POST_EXTRACTED_URL_QUEUE_NAME, POST_ID_WITH_URL_QUEUE_NAME, POST_URL_QUEUE_NAME,
};
use tp2::middleware::RabbitExchange;

fn main() -> Result<()> {
    let env_config = init();
    run_service(env_config)
}

struct UrlExtractor {
    consumers: usize,
}

impl RabbitService for UrlExtractor {
    fn process_message (
        &mut self,
        message: Message,
    ) -> Option<Message> {
        match message {
            Message::FullPost(post) => {
                if post.url.starts_with("http") {
                    return Some(Message::PostUrl(post.id, post.url));
                }
            }
            _ => {
                warn!("Invalid message arrived");
            }
        }
        None
    }

    fn on_stream_finished(&self) -> Option<Message> {
        Some(Message::EndOfStream)
    }
    
    fn send_process_output<E: RabbitExchange>(&self, exchange: &mut E, message: Message) -> Result<()> {
        exchange.send_with_key(&message, POST_EXTRACTED_URL_QUEUE_NAME)?;
        exchange.send_with_key(&message, POST_ID_WITH_URL_QUEUE_NAME)
    }
}

fn run_service(config: Config) -> Result<()> {
    let consumers = str::parse::<usize>(&config.consumers).unwrap();
    let mut service = UrlExtractor { consumers };
    service.run(config, POST_URL_QUEUE_NAME, None)
}

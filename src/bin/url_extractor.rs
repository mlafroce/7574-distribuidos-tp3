use amiquip::Result;
use log::warn;
use tp2::messages::Message;
use tp2::middleware::service::{init, RabbitService};
use tp2::middleware::RabbitExchange;
use tp2::{
    Config, POST_EXTRACTED_URL_QUEUE_NAME, POST_ID_WITH_URL_QUEUE_NAME, POST_URL_QUEUE_NAME,
};

fn main() -> Result<()> {
    let env_config = init();
    run_service(env_config)
}

struct UrlExtractor {
    consumers: usize,
}

impl RabbitService for UrlExtractor {
    fn process_message<E: RabbitExchange>(
        &mut self,
        message: Message,
        exchange: &mut E,
    ) -> Result<()> {
        match message {
            Message::FullPost(post) => {
                if post.url.starts_with("http") {
                    let score = Message::PostUrl(post.id.clone(), post.url.clone());
                    exchange.send_with_key(&score, POST_EXTRACTED_URL_QUEUE_NAME)?;
                    let id = Message::PostId(post.id.clone());
                    exchange.send_with_key(&id, POST_ID_WITH_URL_QUEUE_NAME)?;
                }
            }
            _ => {
                warn!("Invalid message arrived");
            }
        }
        Ok(())
    }

    fn on_stream_finished<E: RabbitExchange>(&self, bin_exchange: &mut E) -> Result<()> {
        for _ in 0..self.consumers {
            bin_exchange.send_with_key(&Message::EndOfStream, POST_EXTRACTED_URL_QUEUE_NAME)?;
            bin_exchange.send_with_key(&Message::EndOfStream, POST_ID_WITH_URL_QUEUE_NAME)?;
        }
        Ok(())
    }
}

fn run_service(config: Config) -> Result<()> {
    let consumers = str::parse::<usize>(&config.consumers).unwrap();
    let mut service = UrlExtractor { consumers };
    service.run(config, POST_URL_QUEUE_NAME, None)
}

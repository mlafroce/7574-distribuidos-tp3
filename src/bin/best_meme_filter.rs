use amiquip::{ConsumerMessage, Result};
use log::{debug, error, info, warn};
use tp2::connection::{BinaryExchange, RabbitConnection};
use tp2::messages::Message;
use tp2::service::{init, RabbitService};
use tp2::{
    Config, POST_EXTRACTED_URL_QUEUE_NAME, POST_SENTIMENT_MEAN_QUEUE_NAME, RESULTS_QUEUE_NAME,
};

fn main() -> Result<()> {
    let env_config = init();
    run_service(env_config)
}

fn run_service(config: Config) -> Result<()> {
    info!("Getting best meme id");
    let best_meme_id = get_best_meme_id(&config)?;
    info!("Getting best meme");
    let mut service = BestMemeFilter::new(best_meme_id);
    service.run(
        config,
        POST_EXTRACTED_URL_QUEUE_NAME,
        None,
    )
}

struct BestMemeFilter {
    best_meme_id: String,
    meme_url: String,
}

impl BestMemeFilter {
    pub fn new(best_meme_id: String) -> Self {
        Self {
            best_meme_id,
            meme_url: "".to_string(),
        }
    }
}

impl RabbitService for BestMemeFilter {
    fn process_message(&mut self, message: Message, _: &BinaryExchange) -> Result<()> {
        match message {
            Message::PostUrl(id, url) => {
                if id == self.best_meme_id {
                    info!("Best meme with url: {:?} {:?}", id, url);
                    self.meme_url = url;
                }
            }
            _ => {
                warn!("Invalid message arrived");
            }
        }
        Ok(())
    }

    fn on_stream_finished(&self, exchange: &BinaryExchange) -> Result<()> {
        debug!("Sending best meme url: {}", self.meme_url);
        let message = Message::PostUrl(self.best_meme_id.clone(), self.meme_url.clone());
        exchange.send_with_key(&message, RESULTS_QUEUE_NAME)
    }
}

// Should I use a heap of best memes ids in case the best one is missing?
fn get_best_meme_id(config: &Config) -> Result<String> {
    let connection = RabbitConnection::new(config)?;
    let mut best_meme_id_sentiment = ("".to_string(), f32::MIN);
    {
        let consumer = connection.get_consumer(POST_SENTIMENT_MEAN_QUEUE_NAME)?;
        if let Some(ConsumerMessage::Delivery(delivery)) = consumer.receiver().iter().next() {
            match bincode::deserialize::<Message>(&delivery.body) {
                Ok(Message::PostIdSentiment(id, sentiment)) => {
                    if sentiment > best_meme_id_sentiment.1 {
                        best_meme_id_sentiment = (id, sentiment);
                    }
                }
                _ => {
                    error!("Invalid message arrived");
                }
            }
            consumer.ack(delivery)?;
        }
    }
    info!("Best meme sentiment: {:?}", best_meme_id_sentiment);
    connection.close()?;
    Ok(best_meme_id_sentiment.0)
}

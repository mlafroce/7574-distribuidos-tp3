use amiquip::{ExchangeType, Result};
use log::info;
use tp2::comment::CommentIterator;
use tp2::messages::Message;
use tp2::middleware::buf_exchange::BufExchange;
use tp2::middleware::connection::{BinaryExchange, RabbitConnection};
use tp2::middleware::service::init;
use tp2::middleware::RabbitExchange;
use tp2::{Config, COMMENTS_SOURCE_EXCHANGE_NAME};
use tp2::health_checker::health_answerer::HealthAnswerer;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use tp2::health_checker::health_base::HealthBase;
use tp2::health_checker::health_answerer_handler::HealthAnswerHandler;

fn main() -> Result<()> {
    let env_config = init();
    let comments_file = envconfig::load_var_with_default(
        "COMMENTS_FILE",
        None,
        "data/the-reddit-irl-dataset-comments.csv",
    )
    .unwrap();
    let shutdown = Arc::new(AtomicBool::new(false));
    let health_answerer = HealthAnswerer::new("0.0.0.0:6789", shutdown.clone());
    let mut health_answerer_handler = HealthAnswerHandler::new(shutdown.clone());
    let health_answerer_thread = thread::spawn(move || {health_answerer.run(&mut health_answerer_handler)});
    run_service(env_config, comments_file)?;
    shutdown.store(true, Ordering::Relaxed);
    health_answerer_thread.join().expect("Failed to join health_answerer_thread");
    Ok(())
}

fn run_service(config: Config, comments_file: String) -> Result<()> {
    let connection = RabbitConnection::new(&config)?;
    let consumers = str::parse::<usize>(&config.consumers).unwrap();
    {
        let exchange =
            connection.get_named_exchange(COMMENTS_SOURCE_EXCHANGE_NAME, ExchangeType::Fanout)?;
        let bin_exchange = BinaryExchange::new(exchange, None, 1, consumers);
        let mut exchange = BufExchange::new(bin_exchange);
        let comments = CommentIterator::from_file(&comments_file);
        info!("Iterating comments");
        let published = comments
            .map(Message::FullComment)
            .flat_map(|message| exchange.send(&message))
            .count();

        exchange.end_of_stream()?;

        info!("Published {} comments", published);
    }
    info!("Exit");
    connection.close()
}

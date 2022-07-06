use std::net::TcpListener;
use amiquip::{ExchangeType, Result};
use envconfig::Envconfig;
use log::info;
use tp2::messages::Message;
use tp2::middleware::buf_exchange::BufExchange;
use tp2::middleware::connection::{BinaryExchange, RabbitConnection};
use tp2::middleware::RabbitExchange;
use tp2::post::PostIterator;
use tp2::{Config, POSTS_SOURCE_EXCHANGE_NAME};
use tp2::health_checker::health_answerer::HealthAnswerer;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use tp2::health_checker::health_base::HealthBase;
use tp2::health_checker::health_answerer_handler::HealthAnswerHandler;


fn main() -> Result<()> {
    let env_config = Config::init_from_env().unwrap();
    println!("Setting logger level: {}", env_config.logging_level);
    std::env::set_var("RUST_LOG", env_config.logging_level.clone());
    env_logger::init();
    let shutdown = Arc::new(AtomicBool::new(false));
    let health_answerer = HealthAnswerer::new("0.0.0.0:6789", shutdown.clone());
    let mut health_answerer_handler = HealthAnswerHandler::new(shutdown.clone());
    let health_answerer_thread = thread::spawn(move || {health_answerer.run(&mut health_answerer_handler)});
    run_service(env_config)?;
    shutdown.store(true, Ordering::Relaxed);
    health_answerer_thread.join().expect("Failed to join health_answerer_thread");
    Ok(())
}

fn run_service(config: Config) -> Result<()> {
    let connection = RabbitConnection::new(&config)?;
    {
        let consumers = str::parse::<usize>(&config.consumers).unwrap();
        let exchange =
            connection.get_named_exchange(POSTS_SOURCE_EXCHANGE_NAME, ExchangeType::Fanout)?;
        let bin_exchange = BinaryExchange::new(exchange, None, 1, consumers);
        let mut exchange = BufExchange::new(bin_exchange);
        let listener = TcpListener::bind("0.0.0.0:9090").expect("Could not bind listener");
        let (stream, _) = listener.accept().expect("Could not accept server connection");
        let posts = PostIterator::from_stream(stream);
        info!("Iterating posts");
        let published = posts
            .map(Message::FullPost)
            .flat_map(|message| exchange.send(&message))
            .count();

        exchange.end_of_stream()?;

        info!("Published {} posts", published);
        info!("Exit");
    }
    connection.close()
}

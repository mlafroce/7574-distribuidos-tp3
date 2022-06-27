use amiquip::{ExchangeType, Result};
use envconfig::Envconfig;
use log::info;
use tp2::messages::Message;
use tp2::middleware::buf_exchange::BufExchange;
use tp2::middleware::connection::{BinaryExchange, RabbitConnection};
use tp2::middleware::RabbitExchange;
use tp2::post::PostIterator;
use tp2::{Config, POSTS_SOURCE_EXCHANGE_NAME};

fn main() -> Result<()> {
    let env_config = Config::init_from_env().unwrap();
    println!("Setting logger level: {}", env_config.logging_level);
    std::env::set_var("RUST_LOG", env_config.logging_level.clone());
    env_logger::init();
    let posts_file = envconfig::load_var_with_default("POSTS_FILE", None, "").unwrap();
    run_service(env_config, posts_file)
}

fn run_service(config: Config, posts_file: String) -> Result<()> {
    let connection = RabbitConnection::new(&config)?;
    {
        let consumers = str::parse::<usize>(&config.consumers).unwrap();
        let exchange =
            connection.get_named_exchange(POSTS_SOURCE_EXCHANGE_NAME, ExchangeType::Fanout)?;
        let bin_exchange = BinaryExchange::new(exchange, None, 1, consumers);
        let mut exchange = BufExchange::new(bin_exchange, connection.get_channel(), None);
        let posts = PostIterator::from_file(&posts_file);
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

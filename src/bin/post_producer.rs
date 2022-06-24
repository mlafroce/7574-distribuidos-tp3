use amiquip::{ExchangeType, Publish, Result};
use envconfig::Envconfig;
use log::info;
use tp2::connection::RabbitConnection;
use tp2::messages::Message;
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
    let exchange =
        connection.get_named_exchange(POSTS_SOURCE_EXCHANGE_NAME, ExchangeType::Fanout)?;
    let posts = PostIterator::from_file(&posts_file);
    info!("Iterating posts");
    let published = posts
        .map(Message::FullPost)
        .flat_map(|message| bincode::serialize(&message))
        .flat_map(|data| exchange.publish(Publish::new(&data, "")))
        .count();

    info!("Published {} posts", published);

    let consumers = str::parse::<usize>(&config.consumers).unwrap();
    for _ in 0..consumers {
        let data = bincode::serialize(&Message::EndOfStream).unwrap();
        exchange.publish(Publish::new(&data, ""))?;
    }
    info!("Exit");
    connection.close()
}

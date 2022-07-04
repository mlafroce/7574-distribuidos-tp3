use amiquip::{ExchangeType, Result};
use log::info;
use tp2::comment::CommentIterator;
use tp2::messages::Message;
use tp2::middleware::buf_exchange::BufExchange;
use tp2::middleware::connection::{BinaryExchange, RabbitConnection};
use tp2::middleware::service::init;
use tp2::middleware::RabbitExchange;
use tp2::{Config, COMMENTS_SOURCE_EXCHANGE_NAME};

fn main() -> Result<()> {
    let env_config = init();
    let comments_file = envconfig::load_var_with_default(
        "COMMENTS_FILE",
        None,
        "data/the-reddit-irl-dataset-comments.csv",
    )
    .unwrap();
    run_service(env_config, comments_file)
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

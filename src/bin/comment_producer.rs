use amiquip::{ExchangeType, Result};
use log::info;
use std::net::TcpListener;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tp2::comment::CommentIterator;
use tp2::health_checker::health_answerer::HealthAnswerer;
use tp2::health_checker::health_answerer_handler::HealthAnswerHandler;
use tp2::health_checker::health_base::HealthBase;
use tp2::messages::Message;
use tp2::middleware::buf_exchange::BufExchange;
use tp2::middleware::connection::{BinaryExchange, RabbitConnection};
use tp2::middleware::service::init;
use tp2::middleware::RabbitExchange;
use tp2::sigterm_handler::sigterm_handler::handle_sigterm;
use tp2::{Config, COMMENTS_SOURCE_EXCHANGE_NAME};

fn main() -> Result<()> {
    let env_config = init();
    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_handler_join = shutdown.clone();
    let sigterm_handler_join = thread::spawn(move || handle_sigterm(shutdown_handler_join));
    let health_answerer = HealthAnswerer::new("0.0.0.0:6789", shutdown.clone());
    let mut health_answerer_handler = HealthAnswerHandler::new(shutdown.clone());
    let health_answerer_thread =
        thread::spawn(move || health_answerer.run(&mut health_answerer_handler));
    run_service(env_config.clone(), shutdown.clone())?;
    shutdown.store(true, Ordering::Relaxed);
    health_answerer_thread
        .join()
        .expect("Failed to join health_answerer_thread");
    sigterm_handler_join
        .join()
        .expect("Failed to join handle_sigterm");
    Ok(())
}

fn run_service(config: Config, shutdown: Arc<AtomicBool>) -> Result<()> {
    let connection = RabbitConnection::new(&config)?;
    let consumers = str::parse::<usize>(&config.consumers).unwrap();
    {
        let exchange =
            connection.get_named_exchange(COMMENTS_SOURCE_EXCHANGE_NAME, ExchangeType::Fanout)?;
        let bin_exchange = BinaryExchange::new(exchange, None, 1, consumers);
        let mut exchange = BufExchange::new(bin_exchange);
        let listener = TcpListener::bind("0.0.0.0:9090").expect("Could not bind listener");
        listener
            .set_nonblocking(true)
            .expect("Could not set non blocking to true");

        for stream_result in listener.incoming() {
            match stream_result {
                Ok(stream) => {
                    
                    let comments = CommentIterator::from_stream(stream);
                    info!("Iterating comments");
                    let published = comments
                        .map(Message::FullComment)
                        .flat_map(|message| exchange.send(&message))
                        .count();

                    exchange.end_of_stream()?;

                    info!("Published {} comments", published);
                }
                Err(_) => {
                    if shutdown.load(Ordering::Relaxed) {
                        break;
                    } else {
                        thread::sleep(Duration::from_secs(1));
                        continue;
                    }
                }
            }
        }
    }
    info!("Exit");
    connection.close()
}

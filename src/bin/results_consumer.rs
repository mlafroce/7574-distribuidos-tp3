use amiquip::{Connection, ConsumerOptions, QueueDeclareOptions, Result};
use log::{debug, error, info};
use std::io::Write;
use tp2::messages::Message;
use tp2::middleware::buf_consumer::BufConsumer;
use tp2::middleware::consumer::DeliveryConsumer;
use tp2::middleware::service::init;
use tp2::{Config, RESULTS_QUEUE_NAME};
use tp2::health_checker::health_answerer::HealthAnswerer;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use tp2::health_checker::health_base::HealthBase;
use tp2::health_checker::health_answerer_handler::HealthAnswerHandler;

fn main() -> Result<()> {
    let env_config = init();
    let output_path =
        envconfig::load_var_with_default("OUTPUT_PATH", None, "data/output.txt").unwrap();
    let shutdown = Arc::new(AtomicBool::new(false));
    let health_answerer = HealthAnswerer::new("0.0.0.0:6789", shutdown.clone());
    let mut health_answerer_handler = HealthAnswerHandler::new(shutdown.clone());
    let health_answerer_thread = thread::spawn(move || {health_answerer.run(&mut health_answerer_handler)});
    run_service(env_config, output_path)?;
    shutdown.store(true, Ordering::Relaxed);
    health_answerer_thread.join().expect("Failed to join health_answerer_thread");
    Ok(())
}

#[derive(Debug, Default)]
struct Results {
    best_meme: String,
    score_mean: f32,
    college_posts: Vec<String>,
}

fn run_service(config: Config, output_path: String) -> Result<()> {
    let host_addr = format!(
        "amqp://{}:{}@{}:{}",
        config.user, config.pass, config.server_host, config.server_port
    );
    debug!("Connecting to: {}", host_addr);

    let mut connection = Connection::insecure_open(&host_addr)?;
    let channel = connection.open_channel(None)?;

    let options = QueueDeclareOptions {
        auto_delete: false,
        ..QueueDeclareOptions::default()
    };
    let queue = channel.queue_declare(RESULTS_QUEUE_NAME, options)?;

    // Query results
    let mut results = Results::default();
    let mut data_received = (false, false, false);
    let consumer = queue.consume(ConsumerOptions::default())?;
    let consumer = DeliveryConsumer::new(consumer);
    let buf_consumer = BufConsumer::new(consumer);
    info!("Starting iteration");
    for compound_delivery in buf_consumer {
        for message in compound_delivery.data {
            match message {
                Message::PostScoreMean(mean) => {
                    info!("got mean: {:?}", mean);
                    results.score_mean = mean;
                    data_received.0 = true;
                }
                Message::PostUrl(id, url) => {
                    info!("got best meme url: {:?}, {}", url, id);
                    results.best_meme = url;
                    data_received.1 = true;
                }
                Message::CollegePostUrl(url) => {
                    results.college_posts.push(url);
                }
                Message::EndOfStream => {}
                Message::CollegePostEnded => {
                    info!("College posts ended");
                    data_received.2 = true;
                }
                Message::Confirmed  => {}
                _ => {
                    error!("Invalid message arrived {:?}", message);
                }
            }
        }
        compound_delivery.msg_delivery.ack(&channel)?;
        compound_delivery.confirm_delivery.ack(&channel)?;
        if data_received.0 && data_received.1 && data_received.2 {
            break;
        }
    }
    if let Ok(mut file) = std::fs::File::create(output_path) {
        results.college_posts.sort();
        write!(file, "Results: {:?}", results).unwrap();
    } else {
        error!("Couldn't write results!");
    }
    info!("Exit");
    connection.close()
}

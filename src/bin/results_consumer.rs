use amiquip::{Connection, ConsumerOptions, QueueDeclareOptions, Result};
use log::{debug, error, info};
use std::io::Write;
use std::sync::atomic::Ordering;
use tp2::messages::Message;
use tp2::middleware::service::{init, TERM_FLAG};
use tp2::{Config, RECV_TIMEOUT, RESULTS_QUEUE_NAME};
use tp2::middleware::buf_consumer::BufConsumer;
use tp2::middleware::consumer::DeliveryConsumer;

const N_RESULTS: usize = 1;

fn main() -> Result<()> {
    let env_config = init();
    let output_path =
        envconfig::load_var_with_default("OUTPUT_PATH", None, "data/output.txt").unwrap();
    run_service(env_config, output_path)
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
    let mut count = 0;
    let mut results = Results::default();
    let mut data_received = (false, true, true);
    let consumer = queue.consume(ConsumerOptions::default())?;
    let consumer = DeliveryConsumer::new(consumer);
    let mut buf_consumer = BufConsumer::new(consumer);
    info!("Starting iteration");
    for (bulk, delivery) in  buf_consumer {
        for message in bulk {
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
                Message::EndOfStream => {
                    info!("College posts ended");
                    count += 1;
                    if count == N_RESULTS {
                        data_received.2 = true;
                    }
                }
                _ => {
                    error!("Invalid message arrived {:?}", message);
                }
            }
        }
        delivery.ack(&channel)?;
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

use amiquip::{Connection, ConsumerMessage, ConsumerOptions, QueueDeclareOptions, Result};
use log::{debug, error, info};
use std::io::Write;
use std::sync::atomic::Ordering;
use tp2::messages::Message;
use tp2::service::{init, TERM_FLAG};
use tp2::{Config, RECV_TIMEOUT, RESULTS_QUEUE_NAME};

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
    let mut data_received = (false, false, false);
    let consumer = queue.consume(ConsumerOptions::default())?;
    info!("Starting iteration");
    while !TERM_FLAG.load(Ordering::Relaxed) {
        let consumer_message = consumer.receiver().recv_timeout(RECV_TIMEOUT);
        match consumer_message {
            Ok(ConsumerMessage::Delivery(delivery)) => {
                let message = bincode::deserialize::<Message>(&delivery.body);
                match message {
                    Ok(Message::PostScoreMean(mean)) => {
                        info!("got mean: {:?}", mean);
                        results.score_mean = mean;
                        data_received.0 = true;
                    }
                    Ok(Message::PostUrl(id, url)) => {
                        info!("got best meme url: {:?}, {}", url, id);
                        results.best_meme = url;
                        data_received.1 = true;
                    }
                    Ok(Message::CollegePostUrl(url)) => {
                        results.college_posts.push(url);
                    }
                    Ok(Message::EndOfStream) => {
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
                consumer.ack(delivery)?;
                if data_received.0 && data_received.1 && data_received.2 {
                    break;
                }
            }
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => {}
            _ => {
                error!("Some error on consumer");
            }
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

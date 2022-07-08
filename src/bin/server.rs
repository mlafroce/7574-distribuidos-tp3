use std::{io, thread};
use std::net::{TcpListener, TcpStream};
use std::io::{Error, ErrorKind, Read, Write};
use std::net::Shutdown::Both;
use std::sync::Arc;
use amiquip::{Connection, ConsumerOptions, QueueDeclareOptions, Result};
use tp2::{Config, RESULTS_QUEUE_NAME};
use tp2::middleware::buf_consumer::BufConsumer;
use tp2::middleware::consumer::DeliveryConsumer;
use tp2::messages::Message;
use log::{debug, error, info};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::spawn;
use std::time::Duration;
use envconfig::Envconfig;
use signal_hook::consts::SIGTERM;
use signal_hook::iterator::Signals;

fn main() {
    println!("Server started");
    let env_config = Config::init_from_env().unwrap();
    println!("Setting logger level: {}", env_config.logging_level);
    std::env::set_var("RUST_LOG", env_config.logging_level.clone());
    env_logger::init();
    let config = ServerConfig::init_from_env().expect("Failed to read env configuration");
    let mut server = Server::new(config.chunk_size, config.server_address, config.posts_producer_address, config.comments_producer_address);
    server.run(&env_config);
}

const MAX_TRIES_FOR_COMMENT_PRODUCER_CONNECTOR: u64 = 5;
const SEC_BETWEEN_RETRIES_FOR_COMMENT: u64 = 20;

pub struct Server {
    chunk_size: u64,
    server_address: String,
    posts_producer_address: String,
    comments_producer_address: String,
    invalid_state: bool
}

impl Server {
    pub fn new(chunk_size: u64,
               server_address: String,
               posts_producer_address: String,
               comments_producer_address: String) -> Self {
        Server{
            chunk_size,
            server_address,
            posts_producer_address,
            comments_producer_address,
            invalid_state: false
        }
    }

    pub fn run(&mut self, config: &Config) {
        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_for_signals_thread = shutdown.clone();
        let signals_thread = spawn(move || {
            handle_sigterm(shutdown_for_signals_thread.clone());
        });

        let listener = TcpListener::bind(self.server_address.clone()).expect(&*format!("Could not bind to address: {}", self.server_address));
        listener.set_nonblocking(true).expect("Could not set non blocking to true");
        for client in listener.incoming() {
            match client {
                Ok(mut stream) => {
                    match self.handle_client(&mut stream, config) {
                        Ok(_) => {}
                        Err(e) => {
                            println!("Error while handling client: {:?}", e);
                            self.answer_not_available(&mut stream);
                        }
                    }
                } Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_secs(1));
                }  Err(e) => {
                    println!("Failed to accept client. Error: {:?}", e);
                }
            }
            if shutdown.load(Ordering::Relaxed) {
                break;
            }
        }
        signals_thread.join().expect("Failed to join signals thread");
    }

    fn handle_client(&mut self, stream: &mut TcpStream, config: &Config) -> io::Result<()> {
        if self.invalid_state {
            error!("System is in an invalid state!");
            return Err(Error::new(ErrorKind::Other, "Server in an invalid state."));
        }
        println!("Forwarding posts");
        let mut got_any_error = false;
        // Can safely exit if post_producer connection fails
        let mut post_producer_stream = TcpStream::connect(self.posts_producer_address.clone())?;
        match self.forward_file(stream, &mut post_producer_stream) {
            Ok(_) => {}
            Err(e) => {
                got_any_error = true;
                error!("Got error {:?} while forwarding file", e);
            }
        };
        // Don't care what error got as long as socket closes
        let _ = post_producer_stream.shutdown(Both);
        println!("Forwarding comments");
        let mut connected_to_comment_producer = false;
        let mut tries = MAX_TRIES_FOR_COMMENT_PRODUCER_CONNECTOR;
        // This is the tricky part, we need to get at least a successful connect with the comment producer,
        // so that we don't leave the services on an invalid state. This is because we need to trigger the EndOfStream by triggering
        // and EOF on the producer.
        while !connected_to_comment_producer {
            match TcpStream::connect(self.comments_producer_address.clone()) {
                Ok(mut comment_producer_stream) => {
                    connected_to_comment_producer = true;
                    match self.forward_file(stream, &mut comment_producer_stream) {
                        Ok(_) => {}
                        Err(e) => {
                            got_any_error = true;
                            error!("Got error {:?} while forwarding file", e);
                        }
                    }
                    // Same as before
                    let _ = comment_producer_stream.shutdown(Both);
                }
                Err(e) => {
                    error!("Failed to connect to comment producer: {:?}. Will retry", e);
                    tries -= 1;
                    if tries == 0 {
                        // If reached this point, something REALLY bad happened and the whole system
                        // is in an invalid state...
                        self.invalid_state = true;
                        return Err(Error::new(ErrorKind::Other, "System in an invalid state."));
                        error!("System in an invalid state. We should never reach this point right? RIGHT!?");
                    }
                    thread::sleep(Duration::from_secs(SEC_BETWEEN_RETRIES_FOR_COMMENT));
                }
            }
        }
        println!("Waiting for response");
        return match self.wait_for_results(config) {
            Ok(results) => {
                if got_any_error {
                    return Err(Error::new(ErrorKind::Other, "Something went wrong. Check the logs. Answering not available"));
                }
                println!("Best meme received: {:?}", results.best_meme);
                println!("Score mean received: {:?}", results.score_mean);
                println!("College posts received: {:?}", results.college_posts.len());
                self.send_results_to_client(stream, &results)?;
                Ok(())
            }
            Err(_) => {
                Err(Error::new(ErrorKind::Other, "Failed to wait for results. Server not available"))
            }
        }
    }

    fn forward_file(&self, from: &mut TcpStream, to: &mut TcpStream) -> io::Result<()> {

        let file_size = self.read_file_size(from)?;
        println!("file_size is {:?}", file_size);
        let mut sent = 0;
        loop {
            let chunk_to_read = if file_size - sent > self.chunk_size { self.chunk_size }  else {file_size - sent};
            let chunk = self.read_chunk(from, chunk_to_read)?;
            to.write_all(&chunk)?;
            sent += chunk_to_read as u64;
            if sent >= file_size { break; }
        }
        println!("Finished sending {:?}", file_size);
        Ok(())
    }

    fn read_file_size(&self, stream: &mut TcpStream) -> io::Result<u64> {
        let mut file_size_buff = [0u8; 8];
        let mut n_received = 0;
        loop {
            let n_bytes = stream.read(&mut file_size_buff[n_received..])?;
            if n_bytes == 0 {
                return Err(Error::new(ErrorKind::UnexpectedEof, "Stream closed"));
            }
            n_received += n_bytes;
            if n_received == file_size_buff.len() {
                break;
            }
        }
        return Ok(u64::from_be_bytes(file_size_buff));
    }

    fn read_chunk(&self, stream: &mut TcpStream, n: u64) -> io::Result<Vec<u8>> {
        let mut received = vec![0; n as usize];
        let mut n_received = 0;
        loop {
            let n_bytes = stream.read(&mut received[n_received..])?;
            if n_bytes == 0 {
                return Err(Error::new(ErrorKind::UnexpectedEof, "Stream closed"));
            }
            n_received += n_bytes;
            if n_received == received.len() {
                break;
            }
        }
        return Ok(received);
    }

    fn wait_for_results(&self, config: &Config) -> Result<Results> {
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
        info!("Exit");
        let _ = connection.close();
        Ok(results)
    }

    fn send_results_to_client(&self, stream: &mut TcpStream, results: &Results) -> io::Result<()>{
        let best_meme = results.best_meme.as_bytes();
        let best_meme_len = best_meme.len() as u64;
        stream.write_all(&best_meme_len.to_be_bytes())?;
        stream.write_all(best_meme)?;
        let score_mean_string = results.score_mean.to_string();
        let score_mean_bytes = score_mean_string.as_bytes();
        let score_mean_len = score_mean_bytes.len() as u64;
        stream.write_all(&score_mean_len.to_be_bytes())?;
        stream.write_all(score_mean_bytes)?;
        for college_post in results.college_posts.clone() {
            let college_post_bytes = college_post.as_bytes();
            let college_post_len = college_post_bytes.len() as u64;
            stream.write_all(&college_post_len.to_be_bytes())?;
            stream.write_all(college_post_bytes)?;
        }
        let ending_msg = "FINISHED";
        let ending_msg_bytes = ending_msg.as_bytes() ;
        let ending_msg_bytes_len = ending_msg.len() as u64;
        stream.write_all(&ending_msg_bytes_len.to_be_bytes())?;
        stream.write_all(ending_msg_bytes)?;
        Ok(())
    }

    fn answer_not_available(&self, stream: &mut TcpStream) {
        let not_available = "Server not available";
        let not_available_bytes = not_available.as_bytes() ;
        let not_available_bytes_len = not_available.len() as u64;
        let _ = stream.write_all(&not_available_bytes_len.to_be_bytes());
        let _ = stream.write_all(not_available_bytes);
        println!("Sent not available to client");
    }

}

#[derive(Clone, Envconfig)]
pub struct ServerConfig {
    /// Configuration file with the run commands for every service
    #[envconfig(from = "CHUNK_SIZE", default = "1000")]
    pub chunk_size: u64,
    #[envconfig(from = "SERVER_ADDRESS", default = "0.0.0.0:9090")]
    pub server_address: String,
    #[envconfig(from = "POSTS_PRODUCER_ADDRESS", default = "")]
    pub posts_producer_address: String,
    #[envconfig(from = "COMMENTS_PRODUCER_ADDRESS", default = "")]
    pub comments_producer_address: String,
}

#[derive(Debug, Default)]
struct Results {
    best_meme: String,
    score_mean: f32,
    college_posts: Vec<String>,
}

fn handle_sigterm(shutdown: Arc<AtomicBool>) {
    let mut signals = Signals::new(&[SIGTERM]).expect("Failed to register SignalsInfo");
    for sig in signals.forever() {
        println!("RECEIVED SIGNAL {}", sig);
        if sig == SIGTERM {
            println!("ENTERED IF {}", sig);
            shutdown.store(true, Ordering::Relaxed);
            break;
        }
    }
}
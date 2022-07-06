use std::io;
use std::io::{Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::io::BufReader;
use amiquip::{Connection, ConsumerOptions, QueueDeclareOptions, Result};
use tp2::{Config, RESULTS_QUEUE_NAME};
use tp2::middleware::buf_consumer::BufConsumer;
use tp2::middleware::consumer::DeliveryConsumer;
use tp2::messages::Message;
use log::{debug, error, info};
use envconfig::Envconfig;

const N_RESULTS: usize = 1;

fn main() {
    println!("Server started");
    let env_config = Config::init_from_env().unwrap();
    println!("Setting logger level: {}", env_config.logging_level);
    std::env::set_var("RUST_LOG", env_config.logging_level.clone());
    env_logger::init();
    let config = ServerConfig::init_from_env().expect("Failed to read env configuration");
    let server = Server::new(config.chunk_size, config.server_address, config.posts_producer_address, config.comments_producer_address);
    server.run(&env_config);
}

pub struct Server {
    chunk_size: u64,
    server_address: String,
    posts_producer_address: String,
    comments_producer_address: String,
}

impl Server {
    pub fn new(chunk_size: u64,
               server_address: String,
               posts_producer_address: String,
               comments_producer_address: String,) -> Self {
        Server{
            chunk_size,
            server_address,
            posts_producer_address,
            comments_producer_address
        }
    }

    pub fn run(&self, config: &Config) {
        let listener = TcpListener::bind(self.server_address.clone()).expect(&*format!("Could not bind to address: {}", self.server_address));
        for client in listener.incoming() {
            match client {
                Ok(mut stream) => {
                    self.handle_client(&mut stream, config);
                    let _ = stream.shutdown(Shutdown::Both);
                }
                Err(e) => {
                    println!("Failed to accept client. Error: {:?}", e);
                }
            }
        }
    }

    fn handle_client(&self, stream: &mut TcpStream, config: &Config) {
        println!("Forwarding posts");
        match TcpStream::connect(self.posts_producer_address.clone()) {
            Ok(mut post_producer_stream) => {
                match self.forward_file(stream, &mut post_producer_stream) {
                    Ok(_) => {},
                    Err(e) => panic!("Error while forwarding posts: {:?}", e)
                }
            }
            Err(e) => {
                panic!("Error while forwarding posts: {:?}", e)
            }
        }
        println!("Forwarding comments");
        match TcpStream::connect(self.comments_producer_address.clone()) {
            Ok(mut comment_producer_stream) => {
                match self.forward_file(stream, &mut comment_producer_stream) {
                    Ok(_) => {},
                    Err(e) => panic!("Error while forwarding comments: {:?}", e)
                }
            }
            Err(e) => {
                panic!("Error while forwarding comments: {:?}", e)
            }
        }
        match self.wait_for_results(config) {
            Ok(results) => {
                println!("Best meme received: {:?}", results.best_meme);
                println!("Score mean received: {:?}", results.score_mean);
                println!("College posts received: {:?}", results.college_posts.len());
                // send_results
            }
            Err(e) => {
                panic!("Error while waiting for results: {:?}", e);
            }
        }
        // En los errores segun cuales lleguen podemos contestarle que no estamos disponibles y
        // a listo no?
        // Y que pasa si el propio server se cae a la mitad????? -> Medio dificil manejar esto no?
        // Tal vez podria tener un log para cuando termina de enviar todo entonces sabe que cuando se recupere,
        // si el resultado que tenga en la cola es valido o no no?. Despues deja que todo el mundo procese y fue.
        // De ultima apura el EOF y listo no?
    }

    fn forward_file(&self, from: &mut TcpStream, to: &mut TcpStream) -> io::Result<()> {

        let file_size = self.read_file_size(from)?;
        println!("file_size is {:?}", file_size);
        let mut sent = 0;
        loop {
            let chunk_to_read = if file_size - sent > self.chunk_size { self.chunk_size }  else {file_size - sent};
            let mut chunk = self.read_chunk(from, chunk_to_read as usize)?;
            to.write_all(&chunk)?; // -> TODO: EL DESTINO SE PUEDE CAER (POSTS O COMMENTS)!!!!!
            // Supongo que en ese caso simplemente fallo maybe? O que onda?? Hay que pensar bien como
            // manejar esto... Pero a priori tal vez lo mas facil es contestar que no estamos disponibles supongo
            // y chau no?
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
            n_received += n_bytes;
            if n_received == file_size_buff.len() {
                break;
            }
        }
        return Ok(u64::from_be_bytes(file_size_buff));
    }

    fn read_chunk(&self, stream: &mut TcpStream, n: usize) -> io::Result<Vec<u8>> {
        let mut received = vec![0; n];
        let mut n_received = 0;
        loop {
            let n_bytes = stream.read(&mut received[n_received..])?;
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
        let mut count = 0;
        let mut results = Results::default();
        let mut data_received = (false, false, false);
        let consumer = queue.consume(ConsumerOptions::default())?;
        let consumer = DeliveryConsumer::new(consumer);
        let buf_consumer = BufConsumer::new(consumer);
        info!("Starting iteration");
        for (bulk, delivery) in buf_consumer {
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
        info!("Exit");
        Ok(results)
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
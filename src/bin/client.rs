use std::{io, thread};
use std::io::{BufReader, Read, Write};
use std::net::TcpStream;
use std::time::Duration;
use envconfig::Envconfig;

fn main() {
    thread::sleep(Duration::from_secs(30));
    println!("Client started");
    let config = ClientConfig::init_from_env().expect("Failed to read env configuration");
    let mut connection = TcpStream::connect(config.server_address).expect("Failed to connect to server");
    send_file(&mut connection, &config.posts_path, config.chunk_size).expect("Failed to send posts");
    send_file(&mut connection, &config.comments_path, config.chunk_size).expect("Failed to send comments");
    // receive_results(&connection); ???
    println!("Client finished sending everything");
}

pub fn send_file(connection: &mut TcpStream, path: &str, chunk_size: u32) -> io::Result<()> {
    let file = std::fs::File::open(path)?;
    let file_size = file.metadata()?.len();
    println!("file_size is {:?}", file_size);
    let mut buf_reader = BufReader::new(file);
    connection.write_all(&file_size.to_be_bytes())?;
    let mut sent = 0;
    loop {
        let mut chunk = Vec::with_capacity(chunk_size as usize);
        let read_size = buf_reader.by_ref().take(chunk_size as u64).read_to_end(&mut chunk)?;
        connection.write_all(&chunk)?;
        sent += read_size as u64;
        if sent >= file_size { break; }
    }
    Ok(())
}

#[derive(Clone, Envconfig)]
pub struct ClientConfig {
    /// Configuration file with the run commands for every service
    #[envconfig(from = "CHUNK_SIZE", default = "1000")]
    pub chunk_size: u32,
    #[envconfig(from = "SERVER_ADDRESS", default = "server:6789")]
    pub server_address: String,
    #[envconfig(from = "POSTS_FILE", default = "")]
    pub posts_path: String,
    #[envconfig(from = "COMMENTS_FILE", default = "")]
    pub comments_path: String,
}
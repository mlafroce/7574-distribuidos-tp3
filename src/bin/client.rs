use std::{io, thread};
use std::io::{BufReader, Read, Write};
use std::net::Shutdown::Both;
use std::net::TcpStream;
use std::time::Duration;
use envconfig::Envconfig;
use std::str;

fn main() {
    thread::sleep(Duration::from_secs(30));
    println!("Client started");
    let config = ClientConfig::init_from_env().expect("Failed to read env configuration");
    let mut connection = TcpStream::connect(config.server_address).expect("Failed to connect to server");
    let _ = send_file(&mut connection, &config.posts_path, config.chunk_size);
    let _ = send_file(&mut connection, &config.comments_path, config.chunk_size);
    println!("Client finished sending everything");
    println!("Client waiting for results");
    receive_results(&mut connection);
    let _ = connection.shutdown(Both);
    println!("Client exiting");
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

pub fn receive_results(connection:&mut TcpStream) {
    let best_meme = read_string(connection);
    println!("Best meme received: {:?}", best_meme);
    if best_meme == "Server not available" {
        return
    }
    let score_mean = read_string(connection);
    println!("Score mean received: {:?}", score_mean);
    if best_meme == "Server not available" {
        return
    }
    let mut college_posts = vec![];
    loop {
        let msg = read_string(connection);
        if msg == "FINISHED" || msg == "Server not available" || msg == "" {
            println!("Received end: {}", msg);
            break;
        }
        println!("College post: {:?}", msg);
        college_posts.push(msg);
    }
    println!("College posts received: {:?}", college_posts.len());
}

pub fn read_string(connection:&mut TcpStream) -> String {
    let best_meme_size = read_msg_size(connection).expect("Could not receive msg len");
    let best_meme_vec = read_chunk(connection, best_meme_size).expect("Failed to read best meme size");
    str::from_utf8(&best_meme_vec).unwrap().to_string()
}

pub fn read_msg_size(stream: &mut TcpStream) -> io::Result<u64> {
    let mut file_size_buff = [0u8; 8];
    let mut n_received = 0;
    loop {
        let n_bytes = stream.read(&mut file_size_buff[n_received..])?;
        n_received += n_bytes;
        if n_received == file_size_buff.len() {
            break;
        }
        if n_bytes == 0 {
            break;
        }
    }
    return Ok(u64::from_be_bytes(file_size_buff));
}

pub fn read_chunk(stream: &mut TcpStream, n: u64) -> io::Result<Vec<u8>> {
    let mut received = vec![0; n as usize];
    let mut n_received = 0;
    loop {
        let n_bytes = stream.read(&mut received[n_received..])?;
        n_received += n_bytes;
        if n_received == received.len() {
            break;
        }
        if n_bytes == 0 {
            break;
        }
    }
    return Ok(received);
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
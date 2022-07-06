use std::io;
use std::io::{Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::io::BufReader;
use envconfig::Envconfig;

const READ_LOOP_SIZE: usize = 1000;

fn main() {
    println!("Server started");
    let config = ServerConfig::init_from_env().expect("Failed to read env configuration");
    let server = Server::new(config.chunk_size, config.server_address, config.posts_producer_address, config.comments_producer_address);
    server.run();
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

    pub fn run(&self) {
        let listener = TcpListener::bind(self.server_address.clone()).expect(&*format!("Could not bind to address: {}", self.server_address));
        for client in listener.incoming() {
            match client {
                Ok(mut stream) => {
                    self.handle_client(&mut stream);
                    let _ = stream.shutdown(Shutdown::Both);
                }
                Err(e) => {
                    println!("Failed to accept client. Error: {:?}", e);
                }
            }
        }
    }

    fn handle_client(&self, stream: &mut TcpStream) {
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
        let mut buf_reader = BufReader::new(from.try_clone()?);
        let mut sent = 0;
        loop {
            let chunk_to_read = if file_size - sent > self.chunk_size { self.chunk_size }  else {file_size - sent};
            let mut chunk = Vec::with_capacity(chunk_to_read as usize);
            let read_size = buf_reader.by_ref().take(chunk_to_read as u64).read_to_end(&mut chunk)?;
            to.write_all(&chunk)?; // -> TODO: EL DESTINO SE PUEDE CAER (POSTS O COMMENTS)!!!!!
            // Supongo que en ese caso simplemente fallo maybe? O que onda?? Hay que pensar bien como
            // manejar esto... Pero a priori tal vez lo mas facil es contestar que no estamos disponibles supongo
            // y chau no?
            sent += read_size as u64;
            if sent >= file_size { break; }
        }
        println!("Finished sending {:?}", file_size);
        Ok(())
    }

    pub fn read_file_size(&self, stream: &mut TcpStream) -> io::Result<u64> {
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

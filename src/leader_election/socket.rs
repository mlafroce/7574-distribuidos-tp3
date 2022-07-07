use std::{
    io::{Read, Write},
    net::{Shutdown, TcpStream}
};

const CHUNK_SIZE: usize = 1;

pub struct Socket {
    stream: TcpStream,
}

impl Socket {
    pub fn new(stream: TcpStream) -> Socket {
        Socket { stream }
    }

    pub fn clone(&self) -> Socket {
        Socket {
            stream: self.stream.try_clone().unwrap(),
        }
    }

    pub fn read(&mut self, n: usize) -> Result<Vec<u8>, String> {
        let mut n_received = 0;
        let mut received: Vec<u8> = vec![];
        let mut received_chunk = [0u8; CHUNK_SIZE];

        loop {
            let n_bytes = self.stream.read(&mut received_chunk).unwrap();
            
            if n_bytes == 0 {
                return Err("socket closed".to_string());
            }

            n_received += n_bytes;

            received.extend_from_slice(&received_chunk[..n_bytes]);

            if n_received == n {
                break;
            }
        }

        return Ok(received);
    }

    pub fn write(&mut self, buffer: &Vec<u8>) {
        self.stream.write_all(buffer).unwrap()
    }

    pub fn shutdown(&self) {
        self.stream.shutdown(Shutdown::Both).unwrap();
    }
}
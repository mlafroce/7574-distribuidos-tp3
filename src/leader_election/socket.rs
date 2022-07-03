use std::{net::TcpStream, io::{Read}, thread};

pub fn socket_read(socket: &mut TcpStream) -> Vec<u8> {
    let mut total_received = 0;
    let mut received: Vec<u8> = vec![0; 9];
    let mut rx_bytes = [0; 9];

    loop {

        let bytes_read = socket.read(&mut rx_bytes).unwrap();

        if bytes_read > 0 {
            println!("hola mundo")
        }
        total_received += bytes_read;
        received.extend_from_slice(&rx_bytes[..bytes_read]);

        if total_received == 9 {
            break;
        }
    }

    return received
}

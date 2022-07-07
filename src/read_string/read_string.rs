use std::io;
use std::io::Read;
use std::net::TcpStream;
use std::str;

pub fn read_string(connection:&mut TcpStream) -> io::Result<String> {
    let string_size = read_msg_size(connection)?;
    let string_vec = read_chunk(connection, string_size)?;
    Ok(str::from_utf8(&string_vec).unwrap().to_string())
}

fn read_msg_size(stream: &mut TcpStream) -> io::Result<u64> {
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

fn read_chunk(stream: &mut TcpStream, n: u64) -> io::Result<Vec<u8>> {
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
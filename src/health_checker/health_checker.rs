use std::io::{BufReader, Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::time::Duration;
use std::convert::TryFrom;
use crate::health_checker::health_msg::HealthMsg;
use crate::health_checker::health_checker_handler::HealthCheckerHandler;
use std::thread;

pub struct HealthChecker {
    address: String,
    sec_between_requests: u64
}

impl HealthChecker {

    pub fn new(address: &str, sec_between_requests: u64) -> Self {
        HealthChecker {
            address: String::from(address),
            sec_between_requests
        }
    }

    #[allow(dead_code)]
    pub fn run_health_answerer(&self, handler: &mut dyn HealthCheckerHandler) {
        let listener = TcpListener::bind(self.address.clone()).expect("Bind failed in run_health_answerer");
        for stream in listener.incoming() {
            println!("Received new client");
            let stream = stream.expect("Failed to accept new connection in run_health_answerer");
            self.answer_health_messages(&stream, HealthMsg::Ping, HealthMsg::Pong, handler);
            let _ = stream.shutdown(Shutdown::Both);
        }
    }

    #[allow(dead_code)]
    pub fn run_health_checker(&self, timeout_sec: u64, handler: &mut dyn HealthCheckerHandler) {
        loop {
            let stream_result = TcpStream::connect(self.address.clone());
            match stream_result {
                Ok(mut stream) => {
                    stream.set_read_timeout(Option::from(Duration::from_secs(timeout_sec))).expect("Failed trying to set read_timeout in run_health_checker");
                    stream.write_all(&[HealthMsg::Ping as u8]).expect("Failed to write first Ping in run_health_checker");
                    self.answer_health_messages(&stream, HealthMsg::Pong, HealthMsg::Ping, handler);
                    let _ = stream.shutdown(Shutdown::Both);
                }
                Err(_) => {
                    handler.handle_connection_refused(self);
                }
            }
            self.wait();
        }
    }

    fn wait(&self) {
        thread::sleep(Duration::from_secs(self.sec_between_requests));
    }

    fn answer_health_messages(&self, mut stream: &TcpStream,
                              expected: HealthMsg,
                              answer: HealthMsg,
                              handler: &mut dyn HealthCheckerHandler) {
        let mut stop_answering = false;
        while !stop_answering {
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut buffer: [u8; 1] = [0; 1];
            match reader.read_exact(&mut buffer) {
                Ok(received) => {
                    println!("Received msg: {}", buffer[0]);
                    let health_msg_received = HealthMsg::try_from(buffer[0]).expect("Failed to convert u8 to health msg");
                    if health_msg_received == expected {
                        stream.write_all(&[answer as u8]).unwrap();
                        println!("Sent msg answer: {}", answer as u8);
                    } else {
                        panic!("Received non expected HealthMsg: {:?}, wanted: {:?}", received, answer as u8);
                    }
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    println!("Received EOF");
                    handler.handle_connection_closed(self);
                    stop_answering = true;
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    println!("Timeout");
                    handler.handle_timeout(self);
                    stop_answering = true;
                }
                Err(e) => {
                    panic!("Received error: {:?}", e);
                }
            }
            self.wait();
        }
    }
}
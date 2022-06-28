use std::io::{BufReader, Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::time::Duration;
use std::convert::TryFrom;
use crate::health_checker::health_msg::HealthMsg;
use crate::health_checker::connection_closed_handler::ConnectionClosedHandler;
use std::thread;
use crate::health_checker::timeout_handler::TimeoutHandler;

pub struct HealthChecker {
    // stop_answering: bool,
    // stop_receiving_new_clients: bool
}

impl HealthChecker {
    pub fn new() -> Self {
        HealthChecker {
            // stop_answering: false,
            // stop_receiving_new_clients: false
        }
    }

    pub fn run_health_answerer(&self, address: &str, timeout_in_sec: u64,
                               eof_handler: &dyn ConnectionClosedHandler,
                               timeout_handler: &dyn TimeoutHandler) {
        let listener = TcpListener::bind(address).expect("Bind failed in run_health_answerer");
        let stop_receiving_new_clients = false;
        for stream in listener.incoming() {
            if stop_receiving_new_clients {
                break
            }
            println!("Received new client");
            let stream = stream.expect("Failed to accept new connection in run_health_answerer");
            stream.set_read_timeout(Option::from(Duration::from_secs(timeout_in_sec))).expect("Failed trying to set read_timeout in run_health_answerer");
            self.answer_health_messages(&stream, HealthMsg::Ping, HealthMsg::Pong, 0, eof_handler, timeout_handler);
            let _ = stream.shutdown(Shutdown::Both);
        }
    }

    pub fn run_health_checker(&self, address: &str, timeout_in_sec: u64, sec_between_requests: u64,
                              eof_handler: &dyn ConnectionClosedHandler,
                              timeout_handler: &dyn TimeoutHandler) {
        let mut stream = TcpStream::connect(address).expect("Connection failed in run_health_checker");
        stream.set_read_timeout(Option::from(Duration::from_secs(timeout_in_sec))).expect("Failed trying to set read_timeout in run_health_checker");
        stream.write_all(&[HealthMsg::Ping as u8]).expect("Failed to write first Ping in run_health_checker");
        self.answer_health_messages(&stream, HealthMsg::Pong, HealthMsg::Ping, sec_between_requests, eof_handler, timeout_handler);
        let _ = stream.shutdown(Shutdown::Both);
    }

    fn answer_health_messages(&self, mut stream: &TcpStream,
                              expected: HealthMsg,
                              answer: HealthMsg,
                              sleep_before_answer_sec: u64,
                              eof_handler: &dyn ConnectionClosedHandler,
                              timeout_handler: &dyn TimeoutHandler) {
        let mut stop_answering = false;
        while !stop_answering {
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut buffer: [u8; 1] = [0; 1];
            match reader.read_exact(&mut buffer) {
                Ok(received) => {
                    println!("Received msg: {}", buffer[0]);
                    let health_msg_received = HealthMsg::try_from(buffer[0]).expect("Failed to convert u8 to health msg");
                    if health_msg_received == expected {
                        thread::sleep(Duration::from_secs(sleep_before_answer_sec));
                        stream.write_all(&[answer as u8]).unwrap();
                        println!("Sent msg answer: {}", answer as u8);
                    } else {
                        panic!("Received non expected HealthMsg: {:?}, wanted: {:?}", received, answer as u8);
                    }
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    println!("Received EOF");
                    eof_handler.handle_connection_closed(self);
                    // TODO: llamar a funcion pasada por parametro tipo handle_connection_closed. No decidir el stop answering aca... o si?
                    // stop_answering = true;
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    println!("Timeout");
                    timeout_handler.handle_timeout(self);
                    // TODO: llamar a funcion pasada por parametro tipo handle_connection_closed. No decidir el stop answering aca... o si?
                    // stop_answering = true;
                }
                Err(e) => {
                    panic!("Received error: {:?}", e);
                }
            }
        }
    }
}
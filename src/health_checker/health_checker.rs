use std::io::Write;
use std::net::{Shutdown, TcpStream};
use std::time::Duration;
use crate::health_checker::health_msg::HealthMsg;
use crate::health_checker::health_checker_handler::HealthCheckerHandler;
use crate::health_checker::health_base::HealthBase;
use std::thread;

pub struct HealthChecker {
    address: String,
    sec_between_requests: u64,
    timeout_sec: u64
}

impl HealthChecker {

    #[allow(dead_code)]
    pub fn new(address: &str, sec_between_requests: u64, timeout_sec: u64) -> Self {
        HealthChecker {
            address: String::from(address),
            sec_between_requests,
            timeout_sec
        }
    }
}

impl HealthBase for HealthChecker {
    fn run(&self, handler: &mut dyn HealthCheckerHandler) {
        while !handler.shutdown() {
            let stream_result = TcpStream::connect(self.address.clone());
            match stream_result {
                Ok(mut stream) => {
                    println!("Connected with address {}", self.address);
                    stream.set_read_timeout(Option::from(Duration::from_secs(self.timeout_sec))).expect("Failed trying to set read_timeout in run_health_checker");
                    stream.write_all(&[HealthMsg::Ping as u8]).expect("Failed to write first Ping in run_health_checker");
                    self.answer_health_messages(&stream, HealthMsg::Pong, HealthMsg::Ping, handler);
                    let _ = stream.shutdown(Shutdown::Both);
                }
                Err(_) => {
                    handler.handle_connection_refused();
                }
            }
            self.wait();
        }
    }

    fn  wait(&self) {
        thread::sleep(Duration::from_secs(self.sec_between_requests));
    }
}
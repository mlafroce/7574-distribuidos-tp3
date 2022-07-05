use std::net::{Shutdown, TcpListener};
use std::io;
use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use crate::health_checker::health_base::HealthBase;
use crate::health_checker::health_checker_handler::HealthCheckerHandler;
use crate::health_checker::health_msg::HealthMsg;

pub struct HealthAnswerer {
    listener: TcpListener,
    shutdown: Arc<AtomicBool>
}

impl HealthAnswerer {

    #[allow(dead_code)]
    pub fn new(address: &str, shutdown: Arc<AtomicBool>) -> Self {
        let listener = TcpListener::bind(address).expect("Bind failed in health answerer");
        listener.set_nonblocking(true).expect("Could not set non blocking to true");
        HealthAnswerer {
            listener,
            shutdown
        }
    }

}

impl HealthBase for HealthAnswerer {
    fn run(&self, handler: &mut dyn HealthCheckerHandler) {
        for stream in self.listener.incoming() {
            match stream {
                Ok(mut stream) => {
                    println!("Received new client");
                    self.answer_health_messages(&stream, HealthMsg::Ping, HealthMsg::Pong, handler);
                    stream.write_all(&[HealthMsg::Exit as u8]).unwrap();
                    let _ = stream.shutdown(Shutdown::Both);
                }
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_secs(1));
                }
                Err(e) => panic!("Unknown error while receiving clients: {}", e),
            }
            if self.shutdown.load(Ordering::Relaxed) {
                break;
            }
        }
    }

    fn wait(&self) {}
}
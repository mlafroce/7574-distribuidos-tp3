use std::net::{Shutdown, TcpListener};
use crate::health_checker::health_base::HealthBase;
use crate::health_checker::health_checker_handler::HealthCheckerHandler;
use crate::health_checker::health_msg::HealthMsg;

pub struct HealthAnswerer {
    listener: TcpListener,
    shutdown: bool
}

impl HealthAnswerer {

    #[allow(dead_code)]
    pub fn new(address: &str) -> Self {
        HealthAnswerer {
            listener: TcpListener::bind(address).expect("Bind failed in health answerer"),
            shutdown: false
        }
    }

    #[allow(dead_code)]
    pub fn shutdown(&mut self) {
        self.shutdown = true;
        self.listener.set_nonblocking(true).expect("Could not set non blocking to true");
    }

}

impl HealthBase for HealthAnswerer {
    fn run(&self, handler: &mut dyn HealthCheckerHandler) {
        for stream in self.listener.incoming() {
            if self.shutdown {
                break;
            }
            println!("Received new client");
            let stream = stream.expect("Failed to accept new connection in run_health_answerer");
            self.answer_health_messages(&stream, HealthMsg::Ping, HealthMsg::Pong, handler);
            let _ = stream.shutdown(Shutdown::Both);
        }
    }

    fn wait(&self) {}
}
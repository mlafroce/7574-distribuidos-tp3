use crate::health_checker::health_msg::HealthMsg;
use crate::health_checker::health_checker_handler::HealthCheckerHandler;
use std::net::TcpStream;
use std::io::{Read, Write};

pub trait HealthBase {

    fn run(&self, handler: &mut dyn HealthCheckerHandler);
    fn wait(&self);
    fn answer_health_messages(&self, mut stream: &TcpStream,
                              expected: HealthMsg,
                              answer: HealthMsg,
                              handler: &mut dyn HealthCheckerHandler) {
        let mut stop_answering = false;
        while !stop_answering && !handler.shutdown() {
            let mut buffer: [u8; 1] = [0; 1];
            match stream.read(&mut buffer) {
                Ok(received) => {
                    if received < 1 {
                        println!("Received EOF");
                        handler.handle_connection_closed();
                        stop_answering = true;
                    } else {
                        let health_msg_received = HealthMsg::try_from(buffer[0]).expect("Failed to convert u8 to health msg");
                        if health_msg_received == expected {
                            stream.write_all(&[answer as u8]).unwrap();
                        } else if health_msg_received == HealthMsg::Exit {
                            println!("Exit received. Proceeding to shutdown");
                            handler.handle_exit_msg();
                        } else {
                            panic!("Received non expected HealthMsg: {:?}, wanted: {:?}", received, answer as u8);
                        }
                    }
                } Err(ref e) if e.kind() == std::io::ErrorKind::ConnectionReset => {
                    println!("Connection reset by peer");
                    handler.handle_connection_closed();
                    stop_answering = true;
                } Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    handler.handle_connection_closed();
                    stop_answering = true;
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    handler.handle_timeout();
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
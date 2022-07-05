use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use crate::health_checker::health_checker::HealthChecker;
use crate::health_checker::health_checker_handler::HealthCheckerHandler;
use crate::health_checker::health_base::HealthBase;

const MAXIMUM_CONNECTION_RETRIES: u32 = 3;

pub struct TaskManager {
    service: String,
    connection_retries: u32,
    service_port: String,
    timeout_sec: u64,
    sec_between_requests: u64,
    shutdown: Arc<AtomicBool>,
    received_end_from_client: bool
}

impl TaskManager {

    pub fn new(service: String, service_port: String, timeout_sec: u64, sec_between_requests: u64, shutdown: Arc<AtomicBool>) -> TaskManager {
        TaskManager {
            service,
            connection_retries: 0,
            service_port,
            timeout_sec,
            sec_between_requests,
            shutdown,
            received_end_from_client: false
        }
    }

    pub fn run(&mut self) {
        let health_checker = HealthChecker::new(&format!("{}:{}", self.service, self.service_port), self.sec_between_requests, self.timeout_sec);
        health_checker.run(self);
        println!("Finished run_health_checker. Task manager {}", self.service);
    }

    pub fn start_service(&mut self) {
        println!("Task manager start_service for service: {}", self.service);
        Command::new("docker").arg("start").arg(self.service.clone()).spawn().expect(&format!("Failed to start service {}", self.service.clone()));
    }

    pub fn shutdown_service(&mut self) {
        println!("Task manager shutdown_service for service: {}", self.service);
        Command::new("docker").arg("stop").arg(self.service.clone()).spawn().expect(&format!("Failed to start service {}", self.service.clone()));
    }
}

impl HealthCheckerHandler for TaskManager {
    fn handle_connection_closed(&mut self) {
        println!("Task manager handle_connection_closed for service: {}", self.service);
        if !self.received_end_from_client {
            self.connection_retries = 0;
            self.shutdown_service();
            self.start_service();
        }
    }

    fn handle_timeout(&mut self) {
        println!("Task manager handle_timeout for service: {}", self.service);
        self.connection_retries = 0;
        self.shutdown_service();
        self.start_service();
    }

    fn handle_connection_refused(&mut self) {
        println!("Task manager handle connection refused for service: {}", self.service);
        if self.connection_retries >= MAXIMUM_CONNECTION_RETRIES {
            self.connection_retries = 0;
            self.shutdown_service();
            self.start_service();
        }
        self.connection_retries += 1;
    }

    fn handle_exit_msg(&mut self) {
        println!("Task manager received handle_exit_msg for service {}", self.service);
        self.received_end_from_client = true;
    }

    fn shutdown(&mut self) -> bool {
        return self.received_end_from_client || (*self.shutdown).load(Ordering::Relaxed);
    }
}

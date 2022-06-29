use crate::health_checker::health_checker::HealthChecker;
use std::process::Command;
use crate::health_checker::health_checker_handler::HealthCheckerHandler;


const MAXIMUM_CONNECTION_RETRIES: u32 = 3;

pub struct TaskManager {
    service: String,
    connection_retries: u32,
}

impl TaskManager {

    pub fn new(service: &str) -> TaskManager {
        TaskManager {
            service: String::from(service),
            connection_retries: 0
        }
    }

    pub fn run(&mut self) {
        let health_checker = HealthChecker::new(&format!("{}:7890", self.service), 80, 10);
        health_checker.run_health_checker(self);
    }

    pub fn start_service(&mut self) {
        println!("Task manager start_service for service: {}", self.service);
        Command::new("docker-compose").arg("up").arg(self.service.clone()).spawn().expect("Failed to start new service");
    }
}

impl HealthCheckerHandler for TaskManager {
    fn handle_connection_closed(&mut self, _health_checker: &HealthChecker) {
        println!("Task manager handle_connection_closed for service: {}", self.service);
        self.connection_retries = 0;
        self.start_service();
    }

    fn handle_timeout(&mut self, _health_checker: &HealthChecker) {
        println!("Task manager handle_timeout for service: {}", self.service);
        self.connection_retries = 0;
        self.start_service();
    }

    fn handle_connection_refused(&mut self, _health_checker: &HealthChecker) {
        println!("Task manager handle connection refused for service: {}", self.service);
        if self.connection_retries >= MAXIMUM_CONNECTION_RETRIES {
            self.connection_retries = 0;
            self.start_service();
        }
        self.connection_retries += 1;
    }
}

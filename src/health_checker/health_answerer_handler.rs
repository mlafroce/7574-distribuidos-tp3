use crate::health_checker::health_checker_handler::HealthCheckerHandler;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

pub struct HealthAnswerHandler {
    shutdown: Arc<AtomicBool>
}

impl HealthAnswerHandler {
    pub fn new(shutdown: Arc<AtomicBool>) -> Self {
        HealthAnswerHandler {
            shutdown
        }
    }
}

impl HealthCheckerHandler for HealthAnswerHandler {
    fn handle_connection_closed(&mut self) {}

    fn handle_timeout(&mut self) {}

    fn handle_connection_refused(&mut self) {}

    fn handle_exit_msg(&mut self) {}

    fn shutdown(&mut self) -> bool {
        self.shutdown.load(Ordering::Relaxed)
    }
}
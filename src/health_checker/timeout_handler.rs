use crate::health_checker::health_checker::HealthChecker;

pub trait TimeoutHandler {
    fn handle_timeout(&self, health_checker: &HealthChecker);
}

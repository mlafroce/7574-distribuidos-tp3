use crate::health_checker::health_checker::HealthChecker;

pub trait HealthCheckerHandler {
    fn handle_connection_closed(&mut self, health_checker: &HealthChecker);
    fn handle_timeout(&mut self, health_checker: &HealthChecker);
    fn handle_connection_refused(&mut self, health_checker: &HealthChecker);
}

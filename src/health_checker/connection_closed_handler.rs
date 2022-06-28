use crate::health_checker::health_checker::HealthChecker;

pub trait ConnectionClosedHandler {
    fn handle_connection_closed(&self, health_checker: &HealthChecker);
}

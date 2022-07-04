pub trait HealthCheckerHandler {
    fn handle_connection_closed(&mut self);
    fn handle_timeout(&mut self);
    fn handle_connection_refused(&mut self);
    fn handle_exit_msg(&mut self);
    fn shutdown(&mut self) -> bool;
}

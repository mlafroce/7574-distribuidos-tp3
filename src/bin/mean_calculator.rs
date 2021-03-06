use amiquip::Result;
use log::{info, warn};
use tp2::messages::Message;
use tp2::middleware::message_processor::MessageProcessor;
use tp2::middleware::service::{init, RabbitService};
use tp2::middleware::RabbitExchange;
use tp2::{Config, POST_SCORE_AVERAGE_QUEUE_NAME, POST_SCORE_MEAN_QUEUE_NAME, RESULTS_QUEUE_NAME};
use tp2::health_checker::health_answerer::HealthAnswerer;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use tp2::health_checker::health_base::HealthBase;
use tp2::health_checker::health_answerer_handler::HealthAnswerHandler;
use tp2::sigterm_handler::sigterm_handler::handle_sigterm;

fn main() -> Result<()> {
    let env_config = init();
    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_handler_join = shutdown.clone();
    let sigterm_handler_join = thread::spawn(move || handle_sigterm(shutdown_handler_join));
    let health_answerer = HealthAnswerer::new("0.0.0.0:6789", shutdown.clone());
    let mut health_answerer_handler = HealthAnswerHandler::new(shutdown.clone());
    let health_answerer_thread = thread::spawn(move || {health_answerer.run(&mut health_answerer_handler)});
    while !shutdown.load(Ordering::Relaxed) {
        run_service(env_config.clone())?;
    }
    shutdown.store(true, Ordering::Relaxed);
    health_answerer_thread.join().expect("Failed to join health_answerer_thread");
    sigterm_handler_join.join().expect("Failed to join handle_sigterm");
    Ok(())
}

fn run_service(config: Config) -> Result<()> {
    let mut processor = MeanCalculator::default();
    let mut service = RabbitService::new(config, &mut processor);
    service.run(POST_SCORE_MEAN_QUEUE_NAME, None)
}

#[derive(Default)]
struct MeanCalculator {
    score_count: u32,
    score_sum: u32,
}

impl MessageProcessor for MeanCalculator {
    type State = (u32, u32);
    fn process_message(&mut self, message: Message) -> Option<Message> {
        match message {
            Message::PostScore(score) => {
                self.score_count += 1;
                self.score_sum += score;
            }
            _ => {
                warn!("Invalid message arrived");
            }
        }
        None
    }

    fn on_stream_finished(&self) -> Option<Message> {
        let mean = self.score_sum as f32 / self.score_count as f32;
        info!("End of stream received, sending mean: {}", mean);
        Some(Message::PostScoreMean(mean))
    }

    fn send_process_output<E: RabbitExchange>(
        &self,
        exchange: &mut E,
        message: Message,
    ) -> Result<()> {
        exchange.send_with_key(&message, RESULTS_QUEUE_NAME)?;
        exchange.send_with_key(&Message::Confirmed, RESULTS_QUEUE_NAME)?;
        exchange.send_with_key(&message, POST_SCORE_AVERAGE_QUEUE_NAME)?;
        exchange.send_with_key(&Message::Confirmed, POST_SCORE_AVERAGE_QUEUE_NAME)
    }

    fn get_state(&self) -> Option<Self::State> {
        Some((self.score_count, self.score_sum))
    }

    fn set_state(&mut self, state: Self::State) {
        (self.score_count, self.score_sum) = state;
    }
}

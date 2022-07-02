use amiquip::Result;
use log::{info, warn};
use tp2::middleware::service::{init, RabbitService};
use tp2::middleware::RabbitExchange;
use tp2::messages::{Message, Score};
use tp2::{
    Config, DATA_TO_SAVE_QUEUE_NAME, POST_SCORE_AVERAGE_QUEUE_NAME, POST_SCORE_MEAN_QUEUE_NAME,
    RESULTS_QUEUE_NAME,
};

fn main() -> Result<()> {
    let env_config = init();
    run_service(env_config)
}

fn run_service(config: Config) -> Result<()> {
    let mut service = MeanCalculator::default();
    service.run(config, POST_SCORE_MEAN_QUEUE_NAME, None)
}

#[derive(Default)]
struct MeanCalculator {
    score_count: u32,
    score_sum: u32,
}

impl RabbitService for MeanCalculator {
    fn process_message(
        &mut self,
        message: Message
    ) -> Option<Message> {
        match message {
            Message::PostScore(score) => {
                self.score_count += 1;
                self.score_sum += score;
                /* Persist State
                let msg =
                    Message::DataScore(Score {
                        sum: self.score_sum,
                        count: self.score_count
                    });
                exchange.send_with_key(&msg, DATA_TO_SAVE_QUEUE_NAME)?;
                 */
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

    fn send_process_output<E: RabbitExchange>(&self, exchange: &mut E, message: Message) -> Result<()> {
        exchange.send_with_key(&message, RESULTS_QUEUE_NAME)?;
        exchange.send_with_key(&message, POST_SCORE_AVERAGE_QUEUE_NAME)
    }
}

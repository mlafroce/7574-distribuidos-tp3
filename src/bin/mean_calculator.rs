use amiquip::Result;
use log::{info, warn};
use tp2::connection::BinaryExchange;
use tp2::messages::{Message, Score};
use tp2::service::{init, RabbitService};
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
    fn process_message(&mut self, message: Message, exchange: &BinaryExchange) -> Result<()> {
        match message {
            Message::PostScore(score) => {
                self.score_count += 1;
                self.score_sum += score;

                /* Persist State */
                let msg =
                    Message::DataScore(Score {
                        sum: self.score_sum,
                        count: self.score_count  
                    });
                exchange.send_with_key(&msg, DATA_TO_SAVE_QUEUE_NAME)?;
                /*  */
            }
            _ => {
                warn!("Invalid message arrived");
            }
        }
        Ok(())
    }

    fn on_stream_finished(&self, exchange: &BinaryExchange) -> Result<()> {
        let mean = self.score_sum as f32 / self.score_count as f32;
        info!("End of stream received, sending mean: {}", mean);
        let msg = Message::PostScoreMean(mean);
        exchange.send_with_key(&msg, RESULTS_QUEUE_NAME)?;
        exchange.send_with_key(&msg, POST_SCORE_AVERAGE_QUEUE_NAME)?;
        /* Persist Reset */
        let msg_reset = Message::DataReset("mean_calculator".to_string());
        exchange.send_with_key(&msg_reset, DATA_TO_SAVE_QUEUE_NAME)
        /*  */
    }
}

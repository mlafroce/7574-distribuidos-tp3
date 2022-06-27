use std::collections::{HashMap, HashSet};

use amiquip::Result;
use tp2::connection::BinaryExchange;
use tp2::messages::Message;
use tp2::service::{init, RabbitService};
use tp2::{Config, DATA_TO_SAVE_QUEUE_NAME};

struct MeanCalculatorState {
    pub score: Score,
}

struct BestMemeFilterState {
    id: String,
    url: String,
}

struct PostAverageFilterState {
    score_average: f32,
}

struct PostCollegeFilterState {
    post_ids: HashSet<String>,
}

struct PostSentimenCalculatorState {
    posts_sentiment: HashMap<String, (f32, i32)>,
}

struct PostSentimentFilterState {
    posts_ids: HashSet<String>
}

#[derive(Default)]
struct Score {
    sum: u32,
    count: u32,
}

const SERVICE_MEAN_CALCULATOR: &str = "mean_calculator";
const SERVICE_BEST_MEME_FILTER: &str = "best_meme_filter";
const SERVICE_POST_AVERAGE_FILTER: &str = "post_average_filter";
const SERVICE_POST_COLLEGE_FILTER: &str = "post_college_filter";
const SERVICE_POST_SENTIMENT_CALCULATOR: &str = "post_sentiment_calculator";
const SERVICE_POST_SENTIMENT_FITLER: &str = "post_sentiment_filter";

struct Saver {
    mean_calculator_state: MeanCalculatorState,
    best_meme_filter_state: BestMemeFilterState,
    post_average_filter_state: PostAverageFilterState,
    post_college_filter_state: PostCollegeFilterState,
    post_sentiment_calculator_state: PostSentimenCalculatorState,
    post_sentiment_filter_state: PostSentimentFilterState
}

impl RabbitService for Saver {
    fn process_message(&mut self, message: Message, _: &BinaryExchange) -> Result<()> {
        match message {
            Message::DataScore(value) => {
                self.mean_calculator_state.score = Score {
                    sum: value.sum,
                    count: value.count,
                };
            }
            Message::DataPostSentiment(value) => {
                self.post_sentiment_calculator_state
                    .posts_sentiment
                    .insert(value.post_id, (value.sentiment, value.count));
            }
            Message::DataScoreAverage(value) => {
                self.post_average_filter_state.score_average = value;
            }
            Message::DataPostIdCollege(value) => {
                self.post_college_filter_state.post_ids.insert(value);
            }
            Message::DataPostId(value) => {
                self.post_sentiment_filter_state.posts_ids.insert(value);
            }

            Message::DataReset(service) => {
                if service == SERVICE_MEAN_CALCULATOR {
                    self.mean_calculator_state.score.sum = 0;
                    self.mean_calculator_state.score.count = 0;
                }
                if service == SERVICE_BEST_MEME_FILTER {
                    self.best_meme_filter_state.id = "".to_string();
                    self.best_meme_filter_state.url = "".to_string();
                }
                if service == SERVICE_POST_AVERAGE_FILTER {
                    self.post_average_filter_state.score_average = 0.0;
                }
                if service == SERVICE_POST_COLLEGE_FILTER {
                    self.post_college_filter_state.post_ids = HashSet::new();
                }
                if service == SERVICE_POST_SENTIMENT_CALCULATOR {
                    self.post_sentiment_calculator_state.posts_sentiment = HashMap::new();
                }
                if service == SERVICE_POST_SENTIMENT_FITLER {
                    self.post_sentiment_filter_state.posts_ids = HashSet::new()
                }
            }
            _ => {}
        }
        Ok(())
    }
}

fn main() -> Result<()> {
    let env_config = init();
    run_service(env_config)
}

fn run_service(config: Config) -> Result<()> {
    let mut service = Saver {
        mean_calculator_state: MeanCalculatorState {
            score: Score { sum: 0, count: 0 },
        },
        best_meme_filter_state: BestMemeFilterState {
            id: "".to_string(),
            url: "".to_string(),
        },
        post_average_filter_state: PostAverageFilterState { score_average: 0.0 },
        post_college_filter_state: PostCollegeFilterState {
            post_ids: HashSet::new(),
        },
        post_sentiment_calculator_state: PostSentimenCalculatorState {
            posts_sentiment: HashMap::new(),
        },
        post_sentiment_filter_state: PostSentimentFilterState {
            posts_ids: HashSet::new()
        }
    };

    service.run(config, DATA_TO_SAVE_QUEUE_NAME, None)
}

use envconfig::Envconfig;
use std::time::Duration;

pub mod comment;
pub mod connection;
pub mod messages;
pub mod post;
pub mod service;

#[derive(Clone, Envconfig)]
pub struct Config {
    /// logger level: valid values: "DEBUG", "INFO", "WARN", "ERROR"
    #[envconfig(from = "LOGGING_LEVEL", default = "INFO")]
    pub logging_level: String,
    /// RabbitMQ host
    #[envconfig(from = "RABBITMQ_HOST", default = "localhost")]
    pub server_host: String,
    /// RabbitMQ port
    #[envconfig(from = "RABBITMQ_PORT", default = "5672")]
    pub server_port: String,
    /// RabbitMQ username
    #[envconfig(from = "RABBITMQ_USER", default = "guest")]
    pub user: String,
    /// RabbitMQ password
    #[envconfig(from = "RABBITMQ_PASS", default = "guest")]
    pub pass: String,
    /// Number of producers sending data
    #[envconfig(from = "PRODUCERS", default = "1")]
    pub producers: String,
    /// Number of consumers reading data (in fanouts, use max number of each endpoint)
    #[envconfig(from = "CONSUMERS", default = "1")]
    pub consumers: String,
}

/// Timeout for receive operations
pub const RECV_TIMEOUT: Duration = Duration::from_secs(1);

/// Exchange with full posts
pub const POSTS_SOURCE_EXCHANGE_NAME: &str = "tp2.posts";
/// Exchange with full posts
pub const COMMENTS_SOURCE_EXCHANGE_NAME: &str = "tp2.comments";
/// Queue with full posts for mean score calculations
pub const POST_SCORES_QUEUE_NAME: &str = "tp2.posts.score_src";
/// Queue with full posts for college memes extraction
pub const POST_COLLEGE_QUEUE_NAME: &str = "tp2.posts.college_src";
/// Queue with full posts for college memes extraction
pub const POST_URL_QUEUE_NAME: &str = "tp2.posts.url_src";
/// Queue with posts urls with above average score
pub const POST_URL_AVERAGE_QUEUE_NAME: &str = "tp2.posts.above_average";
/// Queue with posts urls for best meme
pub const POST_EXTRACTED_URL_QUEUE_NAME: &str = "tp2.posts.urls";
/// Queue with ids of posts with urls
pub const POST_ID_WITH_URL_QUEUE_NAME: &str = "tp2.posts.urls.id";
/// Input for the mean calculator
pub const POST_SCORE_MEAN_QUEUE_NAME: &str = "tp2.posts.mean";
/// Input of college posts filter. Output of the mean calculator
pub const POST_SCORE_AVERAGE_QUEUE_NAME: &str = "tp2.posts.mean.result";
/// Input of comment sentiment extractor
pub const COMMENT_SENTIMENT_QUEUE_NAME: &str = "tp2.comments.sentiment_src";
/// Input of post sentiment filter. Output of comment sentiment extractor.
pub const POST_ID_SENTIMENT_QUEUE_NAME: &str = "tp2.posts.sentiment";
/// Input of post sentiment calculator. Output of post sentiment filter.
pub const FILTERED_POST_ID_SENTIMENT_QUEUE_NAME: &str = "tp2.posts.sentiment.filtered";
/// Output of post sentiment calculator
pub const POST_SENTIMENT_MEAN_QUEUE_NAME: &str = "tp2.posts.sentiment.best";
/// Input of college comment filter
pub const COMMENT_COLLEGE_QUEUE_NAME: &str = "tp2.comments.college_src";
/// Input of college posts filter. Output of college comment filter.
pub const POST_ID_COLLEGE_QUEUE_NAME: &str = "tp2.posts.college_id";
/// Results queue
pub const RESULTS_QUEUE_NAME: &str = "tp2.results";

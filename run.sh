export LOGGING_LEVEL=info
export POSTS_FILE=data/the-reddit-irl-dataset-posts.csv
export COMMENTS_FILE=data/the-reddit-irl-dataset-comments.csv

docker start tp2_rabbitmq_1
sleep 5
bash scripts.sh
RABBITMQ_HOST=172.21.0.2 cargo run --release --bin results_consumer &
CONSUMER_PID=$!
RABBITMQ_HOST=172.21.0.2 cargo run --release --bin mean_calculator &
RABBITMQ_HOST=172.21.0.2 cargo run --release --bin score_extractor &

RABBITMQ_HOST=172.21.0.2 cargo run --release --bin comment_sentiment_extractor &
RABBITMQ_HOST=172.21.0.2 cargo run --release --bin comment_sentiment_extractor &
RABBITMQ_HOST=172.21.0.2 cargo run --release --bin post_sentiment_calculator &
RABBITMQ_HOST=172.21.0.2 PRODUCERS=2 cargo run --release --bin post_sentiment_filter &
RABBITMQ_HOST=172.21.0.2 cargo run --release --bin url_extractor &
RABBITMQ_HOST=172.21.0.2 cargo run --release --bin url_extractor &
RABBITMQ_HOST=172.21.0.2 cargo run --release --bin best_meme_filter &

RABBITMQ_HOST=172.21.0.2 cargo run --release --bin comment_college_filter &
RABBITMQ_HOST=172.21.0.2 cargo run --release --bin post_college_filter &
RABBITMQ_HOST=172.21.0.2 cargo run --release --bin post_average_filter &

RABBITMQ_HOST=172.21.0.2 CONSUMERS=2 cargo run --release --bin post_producer &
RABBITMQ_HOST=172.21.0.2 CONSUMERS=2 cargo run --release --bin comment_producer &

wait $CONSUMER_PID

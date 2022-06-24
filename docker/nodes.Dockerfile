FROM rust:latest AS builder
LABEL intermediateStageToBeDeleted=true

COPY src build/src
COPY Cargo.toml Cargo.lock build/
RUN cd build && cargo build --release

FROM ubuntu:latest

RUN apt update &&\
  apt install -y libssl1.1

COPY --from=builder /build/target/release/best_meme_filter /usr/bin/
COPY --from=builder /build/target/release/comment_college_filter /usr/bin/
COPY --from=builder /build/target/release/comment_sentiment_extractor /usr/bin/
COPY --from=builder /build/target/release/comment_producer /usr/bin/
COPY --from=builder /build/target/release/mean_calculator /usr/bin/
COPY --from=builder /build/target/release/post_average_filter /usr/bin/
COPY --from=builder /build/target/release/post_college_filter /usr/bin/
COPY --from=builder /build/target/release/post_producer /usr/bin/
COPY --from=builder /build/target/release/post_sentiment_calculator /usr/bin/
COPY --from=builder /build/target/release/post_sentiment_filter /usr/bin/
COPY --from=builder /build/target/release/results_consumer /usr/bin/
COPY --from=builder /build/target/release/score_extractor /usr/bin/
COPY --from=builder /build/target/release/url_extractor /usr/bin/


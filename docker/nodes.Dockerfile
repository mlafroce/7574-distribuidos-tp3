FROM docker_install:latest

RUN USER=root cargo new --bin tp2
WORKDIR /tp2

COPY Cargo.lock Cargo.lock
COPY Cargo.toml Cargo.toml

RUN cargo build --release
RUN rm src/*.rs

COPY src src
RUN cargo install --path .
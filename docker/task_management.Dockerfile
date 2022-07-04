FROM rust AS builder

WORKDIR /task_management
COPY task_management_custom_cargo_toml Cargo.toml
RUN mkdir src
COPY ./src/task_manager src/task_manager
COPY ./src/health_checker src/health_checker
COPY ./src/task_management.rs src/main.rs
RUN rustup target add x86_64-unknown-linux-musl
RUN cargo build --target x86_64-unknown-linux-musl --release

FROM rust:alpine3.16
RUN apk update
RUN apk add docker
COPY --from=builder /task_management/target/x86_64-unknown-linux-musl/release/ /usr/bin/
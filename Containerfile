ARG RUST_VERSION=alpine3.21

FROM rust:${RUST_VERSION} AS builder

WORKDIR /opt/messages

RUN apk add --no-cache bash python3 g++ make

COPY Cargo.* /opt/messages/
COPY src/ /opt/messages/src/

RUN cargo build --release

FROM scratch AS release

WORKDIR /opt/messages

COPY --from=builder /opt/messages/target/release/messages /opt/messages/

ENTRYPOINT [ "/opt/messages/messages" ]


# Dockerfile for creating a statically-linked Rust application using docker's
# multi-stage build feature. This also leverages the docker build cache to avoid
# re-downloading dependencies if they have not changed.
FROM alpine:3.13.2 AS alpine
RUN apk update && apk add git ca-certificates gcc && update-ca-certificates

ENV USER=appuser
ENV UID=10001
# See https://stackoverflow.com/a/55757473/12429735RUN 
RUN adduser \    
    --disabled-password \    
    --gecos "" \    
    --home "/nonexistent" \    
    --shell "/sbin/nologin" \    
    --no-create-home \    
    --uid "${UID}" \    
    "${USER}"


# Use clux/muslrust to compile the statically linked C-binaries for rdkafka
FROM clux/muslrust as build
WORKDIR /usr/src

# Download the target for static linking & add rustfmt
RUN rustup target add x86_64-unknown-linux-musl && rustup component add rustfmt --toolchain nightly-2021-02-20-x86_64-unknown-linux-gnu

# Compile and install the binary
COPY . .
RUN cargo build --release
RUN cargo install --target x86_64-unknown-linux-musl --path .

# Copy the statically-linked binary into a scratch container.
FROM scratch

# Get the latest certs & secure user group from the alpine image
COPY --from=alpine /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=alpine /etc/passwd /etc/passwd
COPY --from=alpine /etc/group /etc/group

COPY --from=build /root/.cargo/bin/kafka-bridge-server .
USER appuser:appuser
CMD ["./kafka-bridge-server"]
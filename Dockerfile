ARG VARIANT="bullseye"
FROM rust:1-${VARIANT} as developer

RUN apt-get update && apt-get install -y \
    pkg-config \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace

# Install common Rust tooling
RUN rustup component add rustfmt clippy

# -----------------------------------------------------------------------------
# Build stage
# -----------------------------------------------------------------------------
FROM rust:1-${VARIANT} AS builder

WORKDIR /app

# Copy manifests first to leverage Docker cache
COPY . .

RUN cargo build --release

# -----------------------------------------------------------------------------
# Runtime stage
# -----------------------------------------------------------------------------
FROM rust:slim-bullseye AS runtime

RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/target/release/main /usr/local/bin/main
COPY --from=builder /app/src/templates/ /app/src/templates/

ENTRYPOINT ["main"]
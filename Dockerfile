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
COPY Cargo.toml Cargo.lock ./

# Create a dummy source file so dependencies can be cached
# Compile the dependencies first and later only the application
RUN mkdir src && \
    echo "fn main() {}" > src/main.rs && \
    cargo build --release && \
    rm -rf src

# Copy real source
COPY . .

# Build application
RUN cargo build --release


# -----------------------------------------------------------------------------
# Runtime stage
# -----------------------------------------------------------------------------
FROM rust:slim-bullseye AS runtime

RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/target/release/app /usr/local/bin/app

ENTRYPOINT ["app"]
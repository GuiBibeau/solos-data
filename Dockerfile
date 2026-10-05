# syntax=docker/dockerfile:1
# Build the solos-data binary with the archive lane (Jetstreamer needs clang and cmake for RocksDB).
FROM rust:1.96.0-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends clang cmake libclang-dev pkg-config && rm -rf /var/lib/apt/lists/*
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY crates/ crates/
COPY tests/data/ tests/data/
COPY config/ config/
# Cache mounts keep the registry and the target directory between builds; the binary is copied
# out of the cache so later stages can see it.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/build/target \
    cargo test --locked --workspace \
    && cargo build --release --locked -p solos-data --features archive \
    && mkdir -p /out && cp target/release/solos-data /out/solos-data

# `docker build --target artifact --output type=local,dest=DIR .` leaves only the binary in DIR.
FROM scratch AS artifact
COPY --from=build /out/solos-data /solos-data

# Runtime: one binary, the config files, no credential baked in.
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=build /out/solos-data /usr/local/bin/solos-data
COPY config/ config/
ENV SOLOS_DATA_RAW_DIR=/data/raw SOLOS_DATA_DECODED_DIR=/data/decoded/v1 SOLOS_DATA_DIR=/data/raw
CMD ["solos-data", "decoder", "watch"]

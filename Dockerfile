FROM rust:1.96.0-bookworm AS codec
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY crates/ crates/
RUN cargo test --locked --workspace && cargo build --release --locked -p phoenix-codec

FROM node:24.21.0-bookworm-slim
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --omit=dev && npm cache clean --force
COPY src/ src/
COPY config/ config/
COPY --from=codec /build/target/release/solos-data-phoenix-codec bin/solos-data-phoenix-codec
ENV SOLOS_DATA_RAW_DIR=/data/raw SOLOS_DATA_DECODED_DIR=/data/decoded/v1 SOLOS_DATA_DIR=/data/raw
CMD ["node", "src/decode/cli.ts", "watch"]

FROM rust:1.96.0-bookworm AS codec
WORKDIR /build/codec
COPY codec/ ./
RUN cargo test --locked && cargo build --release --locked

FROM node:24.21.0-bookworm-slim
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --omit=dev && npm cache clean --force
COPY src/ src/
COPY config/ config/
COPY --from=codec /build/codec/target/release/solos-data-phoenix-codec bin/solos-data-phoenix-codec
ENV SOLOS_DATA_RAW_DIR=/data/raw SOLOS_DATA_DECODED_DIR=/data/decoded/v1 SOLOS_DATA_DIR=/data/raw
CMD ["node", "src/decode/cli.ts", "watch"]

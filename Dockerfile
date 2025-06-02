# Base
FROM rust:1.87-slim-bookworm AS base
RUN apt-get update -qqy
RUN apt-get install -qqy librocksdb-dev libzstd-dev libsnappy-dev


# Frontend
FROM base AS txgraph-build
RUN cargo install trunk
RUN apt-get install -qqy git
RUN rustup target add wasm32-unknown-unknown

WORKDIR /build/txgraph

COPY txgraph/ .
ENV API_BASE=https://txgraph.info/api
RUN trunk build --release


### Electrum Rust Server ###
FROM base AS electrs-build
RUN apt-get install -qqy clang cmake

WORKDIR /build/electrs

# Dummy build to cache dependencies
COPY rust-toolchain.toml Cargo.toml Cargo.lock build.rs ./
COPY internal/ internal/

RUN mkdir src && \
    echo "fn main() {println!(\"dummy\")}" > src/main.rs && \
    RUSTFLAGS="-C link-arg=-lzstd -C link-arg=-lsnappy" cargo build --release --locked && \
    rm -rf src

# Real build
COPY . .
ENV ROCKSDB_INCLUDE_DIR=/usr/include
ENV ROCKSDB_LIB_DIR=/usr/lib
RUN RUSTFLAGS="-C link-arg=-lzstd -C link-arg=-lsnappy" cargo install --locked --path .


FROM base AS result

VOLUME /home/bitcoin/.bitcoin

# Copy the binaries
COPY --from=electrs-build /usr/local/cargo/bin/electrs /usr/bin/electrs
COPY --from=txgraph-build /build/txgraph/dist .

COPY electrs.toml .

CMD ["electrs", "--conf", "electrs.toml"]

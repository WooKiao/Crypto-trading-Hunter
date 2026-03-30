FROM rust:1.89-bookworm AS builder

WORKDIR /app

COPY Cargo.lock Cargo.lock
COPY Cargo.toml Cargo.toml
COPY src src
COPY static static

RUN cargo build --release --locked

FROM python:3.12-slim-bookworm AS python-deps

RUN apt-get update \
    && apt-get install -y --no-install-recommends git ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN python -m venv /opt/lighter-venv

ENV PATH="/opt/lighter-venv/bin:${PATH}"

RUN pip install --no-cache-dir --upgrade pip setuptools wheel \
    && pip install --no-cache-dir git+https://github.com/elliottech/lighter-python.git

FROM python:3.12-slim-bookworm AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=python-deps /opt/lighter-venv /opt/lighter-venv
COPY --from=builder /app/target/release/lighter-rust-bbo /usr/local/bin/lighter-rust-bbo
COPY scripts scripts

ENV RUST_LOG=info
ENV PATH="/opt/lighter-venv/bin:${PATH}"
ENV LIGHTER_PYTHON_BIN=/opt/lighter-venv/bin/python

ENTRYPOINT ["/usr/local/bin/lighter-rust-bbo"]

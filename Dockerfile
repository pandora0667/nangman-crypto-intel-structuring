# syntax=docker/dockerfile:1

FROM public.ecr.aws/docker/library/rust:1.94-bookworm AS builder

WORKDIR /opt/nangman-crypto/intel-structuring

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates pkg-config \
    && rm -rf /var/lib/apt/lists/*

COPY . /opt/nangman-crypto/intel-structuring

RUN cargo build --release

FROM public.ecr.aws/docker/library/debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --shell /usr/sbin/nologin intel-structuring \
    && chown -R intel-structuring:intel-structuring /home/intel-structuring

COPY --from=builder \
    /opt/nangman-crypto/intel-structuring/target/release/intel-structuring-app \
    /usr/local/bin/intel-structuring-app

USER intel-structuring

ENV AWS_SDK_LOAD_CONFIG=1

ENTRYPOINT ["/usr/local/bin/intel-structuring-app"]

# syntax=docker/dockerfile:1.2

FROM ubuntu:20.04

RUN DEBIAN_FRONTEND=noninteractive apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get -y install \
    gcc libssl-dev pkg-config protobuf-compiler \
    ca-certificates libssl1.1 vim strace lsof curl jq && \
    rm -rf /var/cache/apt /var/lib/apt/lists/*

ADD /substreams-sink-files /app/substreams-sink-files

ENV PATH "/app:$PATH"

ENTRYPOINT ["/app/substreams-sink-files"]

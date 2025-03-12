FROM ubuntu:24.04

RUN DEBIAN_FRONTEND=noninteractive apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get -y install \
    ca-certificates vim strace htop lsof curl jq && \
    rm -rf /var/cache/apt /var/lib/apt/lists/*

ADD /substreams-sink-files /app/substreams-sink-files

ENV PATH "/app:$PATH"

ENTRYPOINT ["/app/substreams-sink-files"]

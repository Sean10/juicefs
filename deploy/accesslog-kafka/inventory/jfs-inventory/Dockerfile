# Build from the repository root:
#   docker build -f deploy/accesslog-kafka/inventory/jfs-inventory/Dockerfile -t <registry>/jfs-inventory:1.4 .
FROM golang:1.25-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -ldflags "-s -w" -o /jfs-inventory ./deploy/accesslog-kafka/inventory/jfs-inventory

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /jfs-inventory /usr/local/bin/jfs-inventory
ENTRYPOINT ["/usr/local/bin/jfs-inventory"]

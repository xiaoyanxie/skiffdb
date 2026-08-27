FROM golang:1.23-bookworm AS build

RUN apt-get update \
    && apt-get install -y --no-install-recommends protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.9 \
    && go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN protoc --go_out=. --go_opt=paths=source_relative \
    --go-grpc_out=. --go-grpc_opt=paths=source_relative \
    proto/cluster/cluster.proto
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/skiffdb-server . \
    && CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/skiffdb-bench ./benchmarks/cmd/skiffdb-bench

FROM alpine:3.22
RUN apk add --no-cache ca-certificates \
    && addgroup -g 10001 -S skiffdb \
    && adduser -u 10001 -S -D -H -G skiffdb skiffdb
COPY --from=build /out/skiffdb-server /usr/local/bin/skiffdb-server
COPY --from=build /out/skiffdb-bench /usr/local/bin/skiffdb-bench
USER 10001:10001
ENTRYPOINT ["/usr/local/bin/skiffdb-server"]

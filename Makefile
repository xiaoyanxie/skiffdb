# Project name
APP_NAME := skiffdb-server
CMD_PATH := ./
BUILD_DIR := ./

.PHONY: all build clean run test proto benchmark-smoke benchmark-full benchmark-test

all: build test

tidy:
	@echo "📦 Tidying Go modules..."
	go mod tidy

install-dep:
	

proto:
	@echo "🔨 Building protobuf files"
	protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative proto/cluster/cluster.proto

build: proto tidy
	@echo "🔨 Building $(APP_NAME)..."
	go build -o $(BUILD_DIR)/$(APP_NAME) $(CMD_PATH)

run: build
	@echo "🚀 Running $(APP_NAME)..."
	./$(BUILD_DIR)/$(APP_NAME)

test:
	go test -v ./...

benchmark-test:
	go test ./benchmarks/...

benchmark-smoke: proto
	go run ./benchmarks/cmd/skiffdb-bench run --profile smoke

benchmark-full: proto
	go run ./benchmarks/cmd/skiffdb-bench run --profile full

clean:
	@echo "🧹 Cleaning up..."
# 	rm -rf $(BUILD_DIR)
	rm ${APP_NAME}
	rm proto/**/*.pb.go

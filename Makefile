# Project name
APP_NAME := kvdb
CMD_PATH := ./
BUILD_DIR := bin

# Go settings
GO       := go
GOFILES  := $(shell find . -type f -name '*.go')

.PHONY: all build clean run test

all: build

tidy:
	@echo "📦 Tidying Go modules..."
	go mod tidy
build: tidy
	@echo "🔨 Building $(APP_NAME)..."
	$(GO) build -o $(BUILD_DIR)/$(APP_NAME) $(CMD_PATH)

run: build
	@echo "🚀 Running $(APP_NAME)..."
	./$(BUILD_DIR)/$(APP_NAME)

test:
	go test -v ./test/...

clean:
	@echo "🧹 Cleaning up..."
	rm -rf $(BUILD_DIR)


.PHONY: build build-linux clean test run help

# 变量定义
APP_NAME := klock-server
VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
BUILD_TIME := $(shell date -u '+%Y-%m-%d_%H:%M:%S')
GIT_COMMIT := $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")

# Go 相关变量
GOCMD := go
GOBUILD := $(GOCMD) build
GOTEST := $(GOCMD) test
GOGET := $(GOCMD) get
GOMOD := $(GOCMD) mod

# 构建目录
BUILD_DIR := build
BIN_DIR := $(BUILD_DIR)/bin

# ldflags 注入版本信息
LDFLAGS := -ldflags "-s -w \
	-X main.Version=$(VERSION) \
	-X main.BuildTime=$(BUILD_TIME) \
	-X main.GitCommit=$(GIT_COMMIT)"

# 源码路径
SERVER_PATH := ./server

# 默认目标
.DEFAULT_GOAL := help

## build: 编译本机版本
build:
	@echo ">> 编译本机版本: $(APP_NAME)"
	@mkdir -p $(BIN_DIR)
	$(GOBUILD) $(LDFLAGS) -o $(BIN_DIR)/$(APP_NAME) $(SERVER_PATH)
	@echo ">> 输出: $(BIN_DIR)/$(APP_NAME)"

## build-linux: 交叉编译 Linux amd64 版本
build-linux:
	@echo ">> 交叉编译 Linux amd64: $(APP_NAME)"
	@mkdir -p $(BIN_DIR)
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 $(GOBUILD) $(LDFLAGS) -o $(BIN_DIR)/$(APP_NAME)-linux-amd64 $(SERVER_PATH)
	@echo ">> 输出: $(BIN_DIR)/$(APP_NAME)-linux-amd64"

## build-all: 编译所有平台版本
build-all: build build-linux
	@echo ">> 所有平台编译完成"

## clean: 清理构建产物
clean:
	@echo ">> 清理构建产物"
	@rm -rf $(BUILD_DIR)
	@echo ">> 清理完成"

## test: 运行测试
test:
	@echo ">> 运行测试"
	$(GOTEST) -v ./...

## test-race: 运行竞态检测测试
test-race:
	@echo ">> 运行竞态检测测试"
	$(GOTEST) -race -v ./...

## run: 本地运行 server
run:
	@echo ">> 运行 server"
	$(GOCMD) run $(SERVER_PATH)

## deps: 下载依赖
deps:
	@echo ">> 下载依赖"
	$(GOMOD) download
	$(GOMOD) verify

## fmt: 格式化代码
fmt:
	@echo ">> 格式化代码"
	$(GOCMD) fmt ./...

## lint: 代码检查 (需要安装 golangci-lint)
lint:
	@echo ">> 代码检查"
	@which golangci-lint > /dev/null || (echo "错误: 请先安装 golangci-lint" && exit 1)
	golangci-lint run ./...

## proto: 生成 protobuf 代码
proto:
	@echo ">> 生成 protobuf 代码"
	@./scripts/gen-proto.sh

## help: 显示帮助信息
help:
	@echo ""
	@echo "$(APP_NAME) 构建工具"
	@echo ""
	@echo "使用方法:"
	@echo "  make <目标>"
	@echo ""
	@echo "目标列表:"
	@sed -n 's/^##//p' $(MAKEFILE_LIST) | column -t -s ':' | sed -e 's/^/ /'
	@echo ""
	@echo "示例:"
	@echo "  make build          # 编译本机版本"
	@echo "  make build-linux    # 编译 Linux amd64 版本"
	@echo "  make build-all      # 编译所有平台"
	@echo "  make test           # 运行测试"
	@echo ""

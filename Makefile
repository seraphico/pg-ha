# pg-ha local development
#
#   make                    # build Linux binaries + start 3-node Docker cluster (PG 16)
#   make PG_VERSION=18      # same but with PostgreSQL 18
#   make build              # Linux binaries only
#   make up                 # build images + start cluster
#   make down               # wipe cluster (Raft + PGDATA volumes)
#   make stop               # stop containers, keep data (power-off simulation)
#   make restart            # restart containers, keep volumes
#
# Detected hosts:
#   Mac ARM  (arm64)     → aarch64-unknown-linux-gnu / linux/arm64
#   Mac Intel (x86_64)   → x86_64-unknown-linux-gnu  / linux/amd64
#   Linux amd64          → x86_64-unknown-linux-gnu  / linux/amd64
#   Linux arm64          → aarch64-unknown-linux-gnu / linux/arm64
#
# Compose files:
#   docker-compose.build.yml  — Linux binary builder (Docker)
#   docker-compose.yml        — 3-node development cluster

SHELL := /bin/bash
.DEFAULT_GOAL := all

UNAME_S := $(shell uname -s)
UNAME_M := $(shell uname -m)

ifeq ($(UNAME_S),Darwin)
  ifeq ($(UNAME_M),arm64)
    RUST_TARGET ?= aarch64-unknown-linux-gnu
    DOCKER_PLATFORM ?= linux/arm64
  else
    RUST_TARGET ?= x86_64-unknown-linux-gnu
    DOCKER_PLATFORM ?= linux/amd64
  endif
else ifeq ($(UNAME_S),Linux)
  ifeq ($(filter $(UNAME_M),aarch64 arm64),$(UNAME_M))
    RUST_TARGET ?= aarch64-unknown-linux-gnu
    DOCKER_PLATFORM ?= linux/arm64
  else
    RUST_TARGET ?= x86_64-unknown-linux-gnu
    DOCKER_PLATFORM ?= linux/amd64
  endif
else
  $(error Unsupported OS: $(UNAME_S)/$(UNAME_M). Use macOS or Linux.)
endif

export RUST_TARGET
export DOCKER_PLATFORM

# PostgreSQL version for Docker image (default: 16, supports 14-18)
PG_VERSION ?= 16
export PG_VERSION

COMPOSE       ?= docker compose
COMPOSE_BUILD := $(COMPOSE) -f docker-compose.build.yml
COMPOSE_DEV   := $(COMPOSE) -f docker-compose.yml

BIN_DIR := target/$(RUST_TARGET)/release

.PHONY: all build docker-build up down stop restart status logs help clean print-env fmt check lint test

## fmt: format all Rust source code
fmt:
	cargo fmt

## check: run cargo check (fast type/borrow checking without codegen)
check:
	cargo check --workspace

## lint: run clippy lints
lint:
	cargo clippy --workspace -- -D warnings

## test: run all workspace tests
test:
	cargo test --workspace

## all: compile Linux binaries and start the Docker development cluster
all: up
	@echo ""
	@echo "Dev environment ready ($(UNAME_S)/$(UNAME_M) → $(RUST_TARGET), PG $(PG_VERSION))"
	@echo "  Cluster API : http://localhost:8008/cluster"
	@echo "  Postgres    : localhost:5432  (user=postgres password=secret)"
	@echo "  Proxy RW/RO : localhost:6432 / 6433"
	@echo "  make status | make logs | make down"

## build: compile Linux release binaries for the detected Docker arch
build:
	@echo "==> Building Linux binaries ($(RUST_TARGET)) on $(UNAME_S)/$(UNAME_M)"
	@if command -v cargo-zigbuild >/dev/null 2>&1; then \
		echo "Using cargo-zigbuild"; \
		cargo zigbuild --release --target "$(RUST_TARGET)" -p pg-ha -p pg-ha-ctl; \
	elif command -v cross >/dev/null 2>&1; then \
		echo "Using cross"; \
		cross build --release --target "$(RUST_TARGET)" -p pg-ha -p pg-ha-ctl; \
	elif [ "$(UNAME_S)" = "Linux" ] && command -v cargo >/dev/null 2>&1 \
		&& { [ "$(UNAME_M)" = "x86_64" ] || [ "$(UNAME_M)" = "aarch64" ] || [ "$(UNAME_M)" = "arm64" ]; }; then \
		echo "Using native cargo on Linux"; \
		rustup target add "$(RUST_TARGET)" >/dev/null 2>&1 || true; \
		cargo build --release --target "$(RUST_TARGET)" -p pg-ha -p pg-ha-ctl; \
	else \
		echo "Using Docker builder ($(DOCKER_PLATFORM)) via docker-compose.build.yml"; \
		$(COMPOSE_BUILD) run --rm --remove-orphans builder; \
	fi
	@test -x "$(BIN_DIR)/pg-ha" && test -x "$(BIN_DIR)/pg-ha-ctl" \
		|| { echo "error: expected binaries missing under $(BIN_DIR)"; exit 1; }
	@file "$(BIN_DIR)/pg-ha"

## docker-build: build cluster images (depends on Linux binaries)
docker-build: build
	@echo "==> Building cluster images (RUST_TARGET=$(RUST_TARGET), PG_VERSION=$(PG_VERSION))"
	$(COMPOSE_DEV) build --build-arg PG_VERSION=$(PG_VERSION)

## up: build binaries + images and start the 3-node cluster
up: docker-build
	@echo "==> Starting cluster"
	$(COMPOSE_DEV) up -d

## stop: stop containers but keep Raft + PGDATA volumes (simulates power-off)
stop:
	$(COMPOSE_DEV) stop

## down: stop cluster and delete Raft + PGDATA volumes (full wipe)
down:
	$(COMPOSE_DEV) down -v

## restart: stop/start containers, keeping volumes
restart:
	$(COMPOSE_DEV) restart

## status: show compose status and /cluster
status:
	$(COMPOSE_DEV) ps
	@echo ""
	@curl -sf http://localhost:8008/cluster && echo || echo "(API not ready yet)"

## logs: follow cluster logs
logs:
	$(COMPOSE_DEV) logs -f --tail=100

## clean: stop cluster and remove local Linux release artifacts for this target
clean: down
	rm -rf "target/$(RUST_TARGET)"

## print-env: show detected build settings
print-env:
	@echo "OS=$(UNAME_S) ARCH=$(UNAME_M)"
	@echo "RUST_TARGET=$(RUST_TARGET)"
	@echo "DOCKER_PLATFORM=$(DOCKER_PLATFORM)"
	@echo "PG_VERSION=$(PG_VERSION)"
	@echo "BIN_DIR=$(BIN_DIR)"

## help: list targets
help:
	@grep -E '^## ' $(MAKEFILE_LIST) | sed 's/^## //' | column -t -s ':'

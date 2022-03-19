# Bin variables
INSTALL 	= /usr/bin/install
MKDIR 		= mkdir -p
RM 		= rm
CP 		= cp
DOCKER_COMPOSE ?= docker-compose
DOCKER_COMPOSE_EXEC ?= docker-compose exec -T

# Export SCCACHE vars

export SCCACHE_ERROR_LOG = /tmp/sccache_log
export SCCACHE_BUCKET = bzrl-rust-sccache
export SCCACHE_ENDPOINT = s3.fr-par.scw.cloud
export SCCACHE_S3_KEY_PREFIX = flowrunner
export SCCACHE_S3_USE_SSL = true
export RUSTC_WRAPPER = /usr/local/bin/sccache
#export AWS_ACCESS_KEY_ID =
#export AWS_SECRET_ACCESS_KEY =

#CARGO_HOME ?= $(shell git rev-parse --show-toplevel)/.cargo

# Optimization build processes
#CPUS ?= $(shell nproc)
#MAKEFLAGS += --jobs=$(CPUS)

# Project variables

# Compilation variables
PROJECT_BUILD_SRCS = $(shell git ls-files '*.rs')
PROJECT_BUILD_BIN = flowrunner
PROJECT_ARTIFACTS = target/artifacts

PROJECT_BUILD_LINUX_TARGETS = x86_64-unknown-linux-gnu aarch64-unknown-linux-gnu

# Docker image
DOCKER_REPO ?= docker.io/uthng
DOCKER_IMAGE_TAG ?= latest

# "One weird trick!" https://www.gnu.org/software/make/manual/make.html#Syntax-of-Functions
EMPTY:=
SPACE:= ${EMPTY} ${EMPTY}

CROSS_LINUX_TARGETS := $(foreach t,$(PROJECT_BUILD_LINUX_TARGETS),cross-build-$(t))
ARCHIVE_LINUX_TARGETS := $(foreach t,$(PROJECT_BUILD_LINUX_TARGETS),archive-$(t))

all: clean build

.PHONY: all

#build: export CARGO_HOME ?= $(CARGO_HOME)
build-linux: $(CROSS_LINUX_TARGETS)

.PHONY: build-linux

# Cross command targets multiple platforms
# cross-build, cross-test => cross build or cross test
#cross-%: export CARGO_HOME ?= $(CARGO_HOME)
cross-%: export PAIR =$(subst -, ,$($(strip @):cross-%=%))
cross-%: export CMD ?=$(word 1,${PAIR})
cross-%: export TRIPLE ?=$(subst ${SPACE},-,$(wordlist 2,99,${PAIR}))
cross-%: export PROFILE ?= release
cross-%: export CFLAGS += -g0 -O3
cross-%: clean
	echo "Compiling for "$(TRIPLE)"..."
	BINDGEN_EXTRA_CLANG_ARGS="$(if $(findstring aarch64-unknown-linux-gnu, $(TRIPLE)),-I /usr/aarch64-linux-gnu/include/)" cross ${CMD} $(if $(findstring release,$(PROFILE)),--release,) --no-default-features --target $(TRIPLE) --workspace

.PHONY: cross-%

clean:
	- rm -rf $(PROJECT_ARTIFACTS)

.PHONY: clean

clippy:
	#docker run --rm -e SCCACHE_ERROR_LOG=$(SCCACHE_ERROR_LOG) -e SCCACHE_S3_USE_SSL=$(SCCACHE_S3_USE_SSL) -e SCCACHE_S3_KEY_PREFIX=$(SCCACHE_S3_KEY_PREFIX) -e SCCACHE_ENDPOINT=$(SCCACHE_ENDPOINT) -e SCCACHE_BUCKET=$(SCCACHE_BUCKET) -e AWS_SECRET_ACCESS_KEY=$(AWS_SECRET_ACCESS_KEY) -e AWS_ACCESS_KEY_ID=$(AWS_ACCESS_KEY_ID) -v $(PWD):/app uthng/cross:amd64-debian cargo clippy --workspace --target x86_64-unknown-linux-gnu
	cargo clippy --workspace

.PHONY: clippy

deps:
	@echo "Install cross..."
	cargo install cross

.PHONY: deps

tests: docker-start
	cargo build --workspace
	ls target/*
	cargo test --workspace -- --show-output
	make docker-stop

.PHONY: tests

archive-%: export RELEASE_VERSION ?= latest
archive-%: export TRIPLE ?= $($(strip @):archive-%=%)
archive-%: export WORD1 ?= $(word 1,$(subst -, , $(TRIPLE)))
archive-%: export ARCH = $(if $(findstring x86_64,$(WORD1)),amd64,$(if $(findstring aarch64,$(WORD1)),arm64,$(WORD1)))
archive-%: export OS ?= $(word 3,$(subst -, ,$(TRIPLE)))
archive-%: export TARGET_DIR ?= target/$(TRIPLE)/release
archive-%:
	echo "Archiving binaries for $(TRIPLE)..."
	mkdir -p $(PROJECT_ARTIFACTS)
	cp -av README.md $(TARGET_DIR)
	tar -cvzf "$(PROJECT_ARTIFACTS)/$(PROJECT_BUILD_BIN)-${RELEASE_VERSION}-$(ARCH)-$(OS).tar.gz" -C $(TARGET_DIR) "README.md" $(PROJECT_BUILD_BIN) $(notdir $(wildcard $(TARGET_DIR)/*.so)) $(notdir $(wildcard $(TARGET_DIR)/*.dylib))

.PHONY: archive-%

archive-linux: $(ARCHIVE_LINUX_TARGETS)

docker-build:
	@echo "Building the docker image..."
	./scripts/build-docker.sh


.PHONY: docker-build

docker-start:
	$(DOCKER_COMPOSE) up -d

.PHONY: docker-start

docker-stop:
	$(DOCKER_COMPOSE) down

.PHONY: docker-stop

distclean: clean
	-cargo clean

.PHONY: distclean

install:

.PHONY: install

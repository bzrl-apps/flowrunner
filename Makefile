# Command variables

# Bin variables
INSTALL 	= /usr/bin/install
MKDIR 		= mkdir -p
RM 		= rm
CP 		= cp
DOCKER_COMPOSE ?= docker-compose
DOCKER_COMPOSE_EXEC ?= docker-compose exec -T

# Optimization build processes
#CPUS ?= $(shell nproc)
#MAKEFLAGS += --jobs=$(CPUS)

# Project variables

# Compilation variables
PROJECT_BUILD_SRCS = $(shell git ls-files '*.rs')
PROJECT_BUILD_TARGET = flowrunner
PROJECT_ARTIFACTS = target/artifacts

# Docker image
DOCKER_REPO ?= docker.io/uthng
DOCKER_IMAGE_TAG ?= latest

# "One weird trick!" https://www.gnu.org/software/make/manual/make.html#Syntax-of-Functions
EMPTY:=
SPACE:= ${EMPTY} ${EMPTY}

all: clean build

.PHONY: all

# Cross command targets multiple platforms
# cross-build, cross-test => cross build or cross test
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

linters:
.PHONY: linters

deps:
	@echo "Install cross..."
	cargo install cross

.PHONY: deps

tests: docker-start
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
	tar -cvzf "$(PROJECT_ARTIFACTS)/$(PROJECT_BUILD_TARGET)-${RELEASE_VERSION}-$(ARCH)-$(OS).tar.gz" -C $(TARGET_DIR) "README.md" $(PROJECT_BUILD_TARGET) $(notdir $(wildcard $(TARGET_DIR)/*.so)) $(notdir $(wildcard $(TARGET_DIR)/*.dylib))

.PHONY: archives

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

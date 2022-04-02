#!/usr/bin/env bash
set -euo pipefail

# build-docker.sh
#
# SUMMARY
#
#   Builds the Vector docker images and optionally
#   pushes it to the Docker registry

set -x

VERSION="${VERSION:-"latest"}"
DATE="${DATE:-"$(date -u +%Y-%m-%d)"}"
# PLATFORM for Buildx: linux/arm64 or linux/amd64
PLATFORM="${PLATFORM:-"linux/amd64,linux/arm64"}"
PUSH="${PUSH:-"true"}"
REPO="${REPO:-"bazarlab/flowrunner"}"
DIR_ARTIFACTS="target/artifacts"

#
# Functions
#

build() {
    local BASE="$1"
    local VERSION="$2"

    local TAG="$REPO:$VERSION-$BASE"
    local DOCKERFILE="dist/docker/$BASE/Dockerfile"

    if [ -n "$PLATFORM" ]; then
        local ARGS=""
        if [[ "$PUSH" == "true" ]]; then
            ARGS="${ARGS} --push"
        fi

        docker buildx build \
            --platform="${PLATFORM}" \
            --tag "$TAG" \
            ${DIR_ARTIFACTS} \
            -f "$DOCKERFILE" \
            ${ARGS}
    else
        docker build \
            --tag "$TAG" \
            ${DIR_ARTIFACTS} \
            -f "$DOCKERFILE"

            if [[ "$PUSH" == "true" ]]; then
                docker push "$TAG"
            fi
    fi
}

#
# Build
#

echo "Building $REPO:* Docker images"

VERSION_EXACT="$VERSION"
# shellcheck disable=SC2001
VERSION_MINOR_X=$(echo "$VERSION" | sed 's/\.[0-9]*$//g')
# shellcheck disable=SC2001
VERSION_MAJOR_X=$(echo "$VERSION" | sed 's/\.[0-9]*\.[0-9]*$//g')

for VERSION_TAG in "$VERSION_EXACT" "$VERSION_MINOR_X" "$VERSION_MAJOR_X" latest; do
    build debian "$VERSION_TAG"
done

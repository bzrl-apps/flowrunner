#!/bin/bash
set -euo pipefail

RELEASE_VERSION="${1}"

echo "[VERSION] Bump version"
sed -i -e "s/^version = .*/version = \"${RELEASE_VERSION}\"/" Cargo.toml

echo "[DOCKER] Login"
echo "${DOCKERHUB_SECRET}" | docker login -u bazarlab --password-stdin

if [[ "$GITHUB_REF" == 'refs/heads/master' || "$GITHUB_REF" == 'refs/heads/next' ]]; then
  echo "[RELEASE] Creating archives for different architectures"
  make archives-linux RELEASE_VERSION="${RELEASE_VERSION}"
  echo "[RELEASE] Building docker images for different architectures"
  make docker-build VERSION="${RELEASE_VERSION}"
fi

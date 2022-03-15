#!/bin/bash
set -euo pipefail

RELEASE_VERSION="${1}"

echo "[VERSION] Bump version"
sed -i -e "s/^version = .*/version = \"${RELEASE_VERSION}\"/" Cargo.toml

if [[ "$GITHUB_REF" == 'refs/heads/master' || "$GITHUB_REF" == 'refs/heads/next' ]]; then
  echo "[RELEASE] Building docker images for different architectures"
  make docker-build VERSION="${RELEASE_VERSION}"
  echo "[RELEASE] Creating archives for different architectures"
  make archives RELEASE_VERSION="${RELEASE_VERSION}"
fi

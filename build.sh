#!/bin/bash

# Build script using Docker for cross-platform compilation
# Compiles binaries for Windows, Linux, and macOS

set -e

BINARY_DIR="./binary"
mkdir -p "$BINARY_DIR"

echo "Building modelvault using Docker..."
echo "Binaries will be stored in: $BINARY_DIR"

# Build the Docker builder stage
docker build --target builder --tag modelvault:builder .

# Extract binaries from the builder stage
CONTAINER_ID=$(docker create modelvault:builder)
docker cp "$CONTAINER_ID:/app/binary/." "$BINARY_DIR/"
docker rm "$CONTAINER_ID"

echo ""
echo "Build complete! Binaries created in $BINARY_DIR:"
ls -lh "$BINARY_DIR"

# Build stage for cross-compilation
FROM golang:1.25 AS builder

WORKDIR /app

COPY go.mod go.sum ./
COPY . .

RUN go mod tidy
RUN go mod download

# Create binary directory
RUN mkdir -p /app/binary

# Build for Linux (x86_64)
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /app/binary/modelvault-linux-amd64 ./cmd/modelvault

# Build for Windows (x86_64)
RUN CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -o /app/binary/modelvault-windows-amd64.exe ./cmd/modelvault

# Build for macOS (Intel - amd64)
RUN CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 go build -o /app/binary/modelvault-darwin-amd64 ./cmd/modelvault

# Build for macOS (Apple Silicon - arm64)
RUN CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go build -o /app/binary/modelvault-darwin-arm64 ./cmd/modelvault

# Runtime stage (using the Linux binary)
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/binary/modelvault-linux-amd64 ./modelvault

# Run the application
ENTRYPOINT ["./modelvault"]

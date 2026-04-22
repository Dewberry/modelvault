# Build stage
FROM golang:1.25-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Build only the Linux binary for the target platform
RUN CGO_ENABLED=0 go build -o /app/modelvault ./cmd/modelvault

# Runtime stage
FROM debian:bookworm-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/modelvault ./modelvault

ENTRYPOINT ["./modelvault"]

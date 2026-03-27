# Build stage
FROM golang:1.25 AS builder

WORKDIR /app

COPY go.mod go.sum ./
COPY . .

RUN go mod tidy
RUN go mod download
RUN go build -o modelvault ./cmd/modelvault

# Runtime stage
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/modelvault .

# Run the application
ENTRYPOINT ["./modelvault"]

# Multi-stage Dockerfile for ultra-small, lightning-fast Go container
FROM golang:1.22-alpine AS builder

WORKDIR /app
COPY go.mod go.sum* ./
RUN go mod download || true

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o elmak-server cmd/server/main.go

FROM alpine:3.19
RUN apk --no-cache add ca-certificates tzdata
WORKDIR /root/

COPY --from=builder /app/elmak-server .
COPY --from=builder /app/migrations ./migrations

EXPOSE 8080
CMD ["./elmak-server"]

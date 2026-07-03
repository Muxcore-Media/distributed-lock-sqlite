FROM golang:1.26-alpine AS builder
COPY core/ /build/core/
COPY distributed-lock-sqlite/ /build/distributed-lock-sqlite/
WORKDIR /build/distributed-lock-sqlite
RUN go mod download
RUN CGO_ENABLED=0 go build -o /distributed-lock-sqlite ./cmd/module
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /distributed-lock-sqlite /
ENTRYPOINT ["/distributed-lock-sqlite"]

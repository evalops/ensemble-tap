FROM golang:1.26-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY config ./config
COPY internal ./internal
COPY proto ./proto
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/tap ./cmd/tap

FROM gcr.io/distroless/static:nonroot
WORKDIR /app
COPY --from=builder /out/tap /app/tap
COPY config.example.yaml /app/config.example.yaml
EXPOSE 8080
ENTRYPOINT ["/app/tap"]
CMD ["-config", "/app/config.yaml"]

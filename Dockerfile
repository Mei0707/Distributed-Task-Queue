FROM golang:1.26.1-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/api ./cmd/api && \
    CGO_ENABLED=0 go build -trimpath -o /out/worker ./cmd/worker && \
    CGO_ENABLED=0 go build -trimpath -o /out/migrate ./cmd/migrate

FROM build AS test
RUN apk add --no-cache gcc musl-dev
CMD ["go", "test", "./...", "-race", "-count=1", "-v"]

FROM alpine:3.21 AS runtime
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 queue
WORKDIR /app
COPY --from=build /out/ /app/
COPY config.yaml /app/config.yaml
USER queue
EXPOSE 8080 9091
CMD ["/app/api"]

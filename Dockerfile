# Build stage: compile the self-contained Go service (stdlib only, no external deps).
FROM golang:1.22-alpine AS build
WORKDIR /app
COPY go.mod ./
COPY *.go ./
RUN go vet ./... && go test ./... && go build -o /server .

# Run stage: single experimental service, no message middleware.
FROM alpine:3.20
RUN adduser -D -u 10001 app
USER app
COPY --from=build /server /server
EXPOSE 8080
ENV PORT=8080 \
    INITIAL_BALANCE=1000 \
    TRAFFIC_INTERVAL_MS=100
ENTRYPOINT ["/server"]

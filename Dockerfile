FROM golang:1.24.2 AS build

ENV GOOS=linux \
    GOARCH=amd64 \
    CGO_ENABLED=0

COPY . /src

WORKDIR /src/

# build gateway
RUN go generate ./cmd/gateway/
RUN go build -o /gateway ./cmd/gateway/

# build sparkManager
RUN go generate ./cmd/sparkManager/
RUN go build -o /sparkManager ./cmd/sparkManager/

# build tests
RUN go build -o /tests ./cmd/tests/

FROM alpine:3.20 AS runner

# tini for proper signal handling/zombie reaping; ca-certificates for TLS.
RUN apk add --no-cache tini ca-certificates \
    && addgroup -S spark && adduser -S -G spark spark

COPY --from=build /gateway /gateway
COPY --from=build /sparkManager /sparkManager
COPY --from=build /tests /tests

USER spark

ENTRYPOINT ["/sbin/tini", "--"]
CMD ["/gateway"]

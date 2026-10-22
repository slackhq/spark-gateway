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

FROM alpine:3.24 AS runner

# tini for proper signal handling/zombie reaping; ca-certificates for TLS.
# eMudhra 53119: fail the build unless ca-certificates is >= 20261020-r0.
RUN apk add --no-cache --upgrade tini ca-certificates ca-certificates-bundle \
    && for p in ca-certificates ca-certificates-bundle; do \
         v="$(apk info -e -v "$p" | sed "s/^$p-//")"; \
         echo "$p $v"; \
         [ -n "$v" ] && [ "$(apk version -t "$v" 20261020-r0)" != "<" ] \
           || { echo "$p ${v:-missing} is older than 20261020-r0 (eMudhra 53119)" >&2; exit 1; }; \
       done \
    && addgroup -S spark && adduser -S -G spark spark

COPY --from=build /gateway /gateway
COPY --from=build /sparkManager /sparkManager
COPY --from=build /tests /tests

USER spark

ENTRYPOINT ["/sbin/tini", "--"]
CMD ["/gateway"]

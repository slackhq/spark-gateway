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

FROM golang:1.24.2 AS runner
RUN apt update && apt upgrade -y

COPY --from=build /gateway /gateway
COPY --from=build /sparkManager /sparkManager
COPY --from=build /tests /tests

# Add Tini
RUN apt install -y tini

ENTRYPOINT ["/sbin/tini", "--", "/bin/sh"]

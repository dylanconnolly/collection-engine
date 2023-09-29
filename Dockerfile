# syntax=docker/dockerfile:1

FROM golang:1.20 AS build-stage

WORKDIR /app

COPY . ./

RUN go mod download

RUN CGO_ENABLED=0 GOOS=linux go build -o /collection-engine

FROM build-stage AS test-stage

RUN go test -race ./engine/...

FROM gcr.io/distroless/base-debian11 AS build-release-stage

WORKDIR /

COPY --from=build-stage /collection-engine /collection-engine
COPY --from=build-stage app/config.yaml ./

ENTRYPOINT ["/collection-engine"]
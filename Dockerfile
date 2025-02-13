FROM golang:1.24-alpine AS builder

ENV APP_HOME=/go/src/github.com/shopmonkeyus/eds

WORKDIR "$APP_HOME"
COPY . .

ARG VERSION

RUN go build -ldflags "-s -w -X main.version=$VERSION" -o eds

FROM alpine:3.21

ENV APP_HOME=/go/src/github.com/shopmonkeyus/eds
RUN mkdir -p "$APP_HOME"

WORKDIR "$APP_HOME"

ARG GIT_SHA
ARG GIT_BRANCH
ARG BUILD_DATE
ARG VERSION

COPY --from=builder "$APP_HOME"/eds $APP_HOME

ENV GIT_SHA=$GIT_SHA
ENV VERSION=$VERSION
ENV GIT_BRANCH=$GIT_BRANCH
ENV BUILD_DATE=$BUILD_DATE
ENV PORT=8080

EXPOSE 8080

ENTRYPOINT ["./eds"]
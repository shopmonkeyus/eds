FROM golang:1.23-alpine AS builder

ENV APP_HOME=/go/src/github.com/shopmonkeyus/eds

WORKDIR "$APP_HOME"
COPY . .

RUN go mod download && go mod verify && go build -o eds

FROM alpine:3.20

ENV APP_HOME=/go/src/github.com/shopmonkeyus/eds
RUN mkdir -p "$APP_HOME"

WORKDIR "$APP_HOME"

ARG GIT_SHA
ARG GIT_BRANCH
ARG BUILD_DATE

COPY --from=builder "$APP_HOME"/eds $APP_HOME

ENV GIT_SHA=$GIT_SHA
ENV GIT_BRANCH=$GIT_BRANCH
ENV BUILD_DATE=$BUILD_DATE
ENV PORT=8080

EXPOSE 8080

ENTRYPOINT ["./eds"]
FROM docker.io/library/golang:1.22-alpine AS builder
RUN apk add --no-cache git
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /open_taki .

FROM docker.io/library/alpine:3.20
RUN apk add --no-cache poppler-utils pandoc
COPY --from=builder /open_taki /usr/local/bin/open_taki
COPY config.yaml /etc/open_taki/config.yaml
EXPOSE 9998
ENTRYPOINT ["open_taki"]
CMD ["/etc/open_taki/config.yaml"]

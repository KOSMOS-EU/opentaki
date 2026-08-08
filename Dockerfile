FROM docker.io/library/golang:1.26-alpine AS builder
RUN apk add --no-cache git
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /open_taki .

FROM docker.io/library/alpine:3.20
RUN apk add --no-cache poppler-utils pandoc
# Office conversion: add collabora as sidecar container
# Audio/video: add ffmpeg if whisper is configured
COPY --from=builder /open_taki /usr/local/bin/open_taki
COPY config.yaml /etc/open_taki/config.yaml
COPY docmeta_schema.json /etc/open_taki/docmeta_schema.json
COPY docmeta_prompt.txt /etc/open_taki/docmeta_prompt.txt
COPY docmeta_rescue_prompt.txt /etc/open_taki/docmeta_rescue_prompt.txt
COPY field_rules.json /etc/open_taki/field_rules.json
COPY store_detect_prompt.txt /etc/open_taki/store_detect_prompt.txt
COPY aktenplan.txt /etc/open_taki/aktenplan.txt
EXPOSE 9998
ENTRYPOINT ["open_taki"]
CMD ["/etc/open_taki/config.yaml"]

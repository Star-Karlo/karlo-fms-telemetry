# Multi-stage build for any FMS service binary under ./cmd.
#   docker build --build-arg SERVICE=geocode -t fms-geocode .
#
# Every image also carries /archiver and /telmigrate, so a scheduled or one-off
# task (the nightly archive, the Tiger -> RDS copy) runs from the ingest image
# with a command override instead of needing its own ECR repository.
FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG SERVICE
RUN test -n "$SERVICE" || (echo "SERVICE build-arg required" && exit 1)
RUN CGO_ENABLED=0 go build -trimpath -o /out/app ./cmd/${SERVICE} \
 && CGO_ENABLED=0 go build -trimpath -o /out/archiver ./cmd/archiver \
 && CGO_ENABLED=0 go build -trimpath -o /out/telmigrate ./cmd/telmigrate \
 && CGO_ENABLED=0 go build -trimpath -o /out/ingestkey ./cmd/ingestkey

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/app /out/archiver /out/telmigrate /out/ingestkey /
ENTRYPOINT ["/app"]
